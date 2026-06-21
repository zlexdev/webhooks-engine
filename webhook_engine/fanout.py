"""Fanout handler — one ``EventHandler`` per discovered webhook event.

Each discovered event gets its own specialized subclass built at
install time via :meth:`WebhookFanoutHandler.specialize`. This uses
``types.new_class`` to trigger
:meth:`EventHandler.__init_subclass__`, so each subclass gets its own
``_installed_buses`` set (prevents cross-event registration leaks).

Signing is *not* done here — the dispatcher re-signs at send time so
timestamps stay inside ``security.signature_window_s``.
"""

from __future__ import annotations

import json
import types as pytypes
from collections.abc import Callable
from dataclasses import asdict
from datetime import datetime
from typing import TYPE_CHECKING, ClassVar
from uuid import uuid4

from asyncbus import BaseEvent
from asyncbus.handlers import EventHandler, HandlerContext
from asyncbus.serialization import _default as _event_default_serializer

from webhook_engine.enums import DeliveryStatus
from webhook_engine.envelope import redact_payload
from webhook_engine.meta import WebhookMeta
from webhook_engine.protocols import (
    SubscriptionReaderProtocol,
    WebhookDeliveryStoreProtocol,
)
from webhook_engine.types import DeliveryRecord, SubscriptionSnapshot

__all__ = ["WebhookFanoutHandler"]

if TYPE_CHECKING:
    from asyncbus import BaseEventBus


def _event_to_raw(event: BaseEvent) -> dict[str, object]:
    """Event → plain dict. Mirrors the outbox serializer so the same
    payload the bus would write ends up in the webhook body."""
    # asdict keeps `meta` nested; the redactor strips it.
    return asdict(event)


class WebhookFanoutHandler(EventHandler[BaseEvent]):
    """Generic handler subclass. The installer synthesizes one concrete
    subclass per catalog entry via :meth:`specialize`.

    Concrete subclasses declare:
    - ``event_cls`` — the event this handler fans out
    - ``plugin_id`` — ``"core.webhooks.<meta.name>"``
    - ``meta`` — the :class:`WebhookMeta` for this event
    """

    meta: ClassVar[WebhookMeta]
    plugin_id: ClassVar[str] = "core.webhooks._base"

    def __init__(
        self,
        bus: BaseEventBus,
        reader: SubscriptionReaderProtocol,
        store: WebhookDeliveryStoreProtocol,
        dispatch_enabled: Callable[[], bool],
        clock: Callable[[], datetime],
    ) -> None:
        super().__init__(bus)
        self._reader = reader
        self._store = store
        self._dispatch_enabled = dispatch_enabled
        self._clock = clock

    async def handle(self, event: BaseEvent, ctx: HandlerContext) -> None:
        if not self._dispatch_enabled():
            # Drain-first-flip-second: fanout skips enqueue when
            # dispatch is disabled so Redis doesn't fill.
            return
        meta = type(self).meta
        tenant_id: str | None = None
        if meta.tenant_field:
            raw_val = getattr(event, meta.tenant_field, None)
            tenant_id = str(raw_val) if raw_val is not None and raw_val != "" else None
        subs = await self._reader.for_event(meta.name, tenant_id)
        if not subs:
            return
        raw = _event_to_raw(event)
        redacted = redact_payload(raw, meta)
        data_json = json.dumps(
            redacted,
            separators=(",", ":"),
            sort_keys=True,
            default=_event_default_serializer,
        ).encode()
        now = self._clock()
        records = [self._build_record(event, sub, meta, tenant_id, data_json, now) for sub in subs]
        await self._store.enqueue_many(records)

    def _build_record(
        self,
        event: BaseEvent,
        sub: SubscriptionSnapshot,
        meta: WebhookMeta,
        tenant_id: str | None,
        data_json: bytes,
        now: datetime,
    ) -> DeliveryRecord:
        emeta = event.meta
        return DeliveryRecord(
            delivery_id=uuid4().hex,
            subscription_id=sub.id,
            owner_id=sub.owner_id,
            event_name=meta.name,
            event_id=emeta.event_id,
            schema_version=emeta.schema_version,
            tenant_id=tenant_id,
            data_json=data_json,
            emitted_at=emeta.emitted_at,
            target_url=sub.url,
            resolved_ip=sub.resolved_ip,
            redelivery_of=None,
            attempts=0,
            fire_at=now,
            created_at=now,
            status=DeliveryStatus.PENDING,
        )

    @classmethod
    def specialize(
        cls,
        *,
        event_cls: type[BaseEvent],
        meta: WebhookMeta,
    ) -> type[WebhookFanoutHandler]:
        """Return a dedicated subclass bound to one event.

        Uses ``types.new_class`` so ``EventHandler.__init_subclass__``
        fires and the subclass gets its own ``_installed_buses``.
        """
        plugin_id = f"core.webhooks.{meta.name}"
        qualname = f"WebhookFanoutHandler__{event_cls.__name__}"

        def _exec(ns: dict[str, object]) -> None:
            ns["event_cls"] = event_cls
            ns["plugin_id"] = plugin_id
            ns["meta"] = meta
            ns["__qualname__"] = qualname

        return pytypes.new_class(qualname, (cls,), exec_body=_exec)
