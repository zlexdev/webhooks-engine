"""``WebhookMeta`` marker + ``@webhook_event`` decorator.

Opt-in marker: an event class is eligible for fan-out only when a
class-level ``webhook_meta`` attribute is present. Opt-in by default
prevents accidental exposure of internal events.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from asyncbus import MutablePreEvent, PreEvent

from webhook_engine.enums import WebhookScope

__all__ = ["WebhookMeta", "webhook_event"]

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class WebhookMeta:
    """Attached to a ``BaseEvent`` subclass to expose it to subscribers.

    ``tenant_field`` is the attribute on the concrete event that holds
    the owning ``PanelUser.id``. ``None`` → event is admin-only
    (global scope).

    ``redacted_fields`` is explicit; ``allow_sensitive`` bypasses the
    automatic suffix-based redaction (``_enc`` / ``_token`` / ``password``
    / ``secret``). Leave ``allow_sensitive=False`` unless reviewed.
    """

    name: str
    description: str
    scopes: tuple[WebhookScope, ...] = (WebhookScope.TENANT,)
    tenant_field: str | None = "user_id"
    since: str = "1.0.0"
    sample_payload: dict[str, Any] | None = None
    redacted_fields: tuple[str, ...] = ()
    allow_sensitive: bool = False


def webhook_event(meta: WebhookMeta) -> Callable[[type[T]], type[T]]:
    """Attach ``webhook_meta`` to an event class. Opt-in marker.

    Rejects ``PreEvent`` / ``MutablePreEvent`` subclasses — pre-events
    use synchronous abort semantics that don't survive async external
    delivery, so subscribing to them would be a silent "never fires"
    trap.
    """

    def _apply(cls: type[T]) -> type[T]:
        if issubclass(cls, (PreEvent, MutablePreEvent)):
            raise ValueError(
                f"{cls.__qualname__}: PreEvent subclasses cannot carry webhook_meta "
                "(abort semantics don't survive external delivery)"
            )
        # Reject double-decoration but allow the class to *inherit*
        # webhook_meta from a parent (covered by registry walk).
        if "webhook_meta" in cls.__dict__:
            raise ValueError(f"{cls.__qualname__} already has webhook_meta")
        cls.webhook_meta = meta  # type: ignore[attr-defined]
        return cls

    return _apply
