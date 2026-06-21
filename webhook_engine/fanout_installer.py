"""Installs one :class:`WebhookFanoutHandler` per catalog entry.

Called from the app lifespan after the :class:`WebhookEventRegistry`
discovery pass (initially and again after
``plugin_registry.load_all()`` so plugin-declared events reach the
bus). Uses the handler's ``specialize`` to produce a dedicated
subclass per event — each subclass owns its own ``_installed_buses``
set, so repeated ``install_all()`` calls are idempotent per event.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING

from libs.shared.logging import get_logger
from webhook_engine.fanout import WebhookFanoutHandler
from webhook_engine.protocols import (
    SubscriptionReaderProtocol,
    WebhookDeliveryStoreProtocol,
)
from webhook_engine.registry import WebhookCatalog

__all__ = ["WebhookFanoutInstaller"]

if TYPE_CHECKING:
    from asyncbus import BaseEventBus
    from asyncbus.handlers import HandlerRegistry


class WebhookFanoutInstaller:
    def __init__(
        self,
        *,
        bus: BaseEventBus,
        handler_registry: HandlerRegistry,
        catalog: WebhookCatalog,
        reader: SubscriptionReaderProtocol,
        store: WebhookDeliveryStoreProtocol,
        dispatch_enabled: Callable[[], bool],
        clock: Callable[[], datetime],
    ) -> None:
        self._bus = bus
        self._handler_registry = handler_registry
        self._catalog = catalog
        self._reader = reader
        self._store = store
        self._dispatch_enabled = dispatch_enabled
        self._clock = clock
        self._installed: set[str] = set()
        self._log = get_logger("webhooks.fanout_installer")

    def install_all(self, catalog: WebhookCatalog | None = None) -> int:
        """Install handlers for every entry; return count of new installs.

        Safe to call repeatedly. Each invocation walks the active
        catalog, synthesizes a specialized :class:`WebhookFanoutHandler`
        subclass per entry, and installs it on the bus. Entries that
        were already installed on a previous call are skipped via the
        subclass-level ``_installed_buses`` guard on
        :class:`asyncbus.handlers.EventHandler`.
        """
        target = catalog or self._catalog
        count = 0
        for entry in target.entries:
            if entry.meta.name in self._installed:
                continue
            subclass = WebhookFanoutHandler.specialize(
                event_cls=entry.event_cls,
                meta=entry.meta,
            )
            handler = subclass(
                self._bus,
                self._reader,
                self._store,
                self._dispatch_enabled,
                self._clock,
            )
            self._handler_registry.install(handler)
            self._installed.add(entry.meta.name)
            count += 1
        if count:
            self._log.info(
                "fanout_installed",
                count=count,
                total=len(target.entries),
            )
        return count

    def is_installed(self, event_name: str) -> bool:
        return event_name in self._installed
