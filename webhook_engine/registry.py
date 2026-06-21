"""Event catalog discovery.

``WebhookEventRegistry.discover()`` walks ``BaseEvent`` and
``MutableBaseEvent`` subclass trees transitively, rejects pre-events
(abortable), and collates every class that carries a class-level
``webhook_meta``. Duplicate ``meta.name`` values raise
:class:`WebhookEventCollision` so the app fails to boot rather than
silently shadowing one event with another.

Catalog is immutable after ``discover()``; callers build a new one
after a second pass (e.g. after ``PluginRegistry.load_all()``) and
replace the reference atomically.
"""

from __future__ import annotations

from dataclasses import dataclass

from asyncbus import BaseEvent, MutableBaseEvent

from webhook_engine.enums import WebhookScope
from webhook_engine.exceptions import (
    WebhookEventCollision,
    WebhookTenantFieldMissing,
)
from webhook_engine.meta import WebhookMeta

__all__ = ["WebhookCatalog", "WebhookEventEntry", "WebhookEventRegistry"]


@dataclass(frozen=True, slots=True)
class WebhookEventEntry:
    event_cls: type
    meta: WebhookMeta


@dataclass(frozen=True, slots=True)
class WebhookCatalog:
    entries: tuple[WebhookEventEntry, ...]

    @property
    def by_name(self) -> dict[str, WebhookEventEntry]:
        return {e.meta.name: e for e in self.entries}

    def names(self) -> tuple[str, ...]:
        return tuple(e.meta.name for e in self.entries)

    def by_scope(self, scope: WebhookScope) -> tuple[WebhookEventEntry, ...]:
        return tuple(e for e in self.entries if scope in e.meta.scopes)

    def verify_tenant_fields(self) -> None:
        """Assert every entry with ``tenant_field`` has that attribute on the event class.

        Catches decorator typos (``tenant_field="userId"``) at startup
        instead of at first fanout. Walks dataclass ``__dataclass_fields__``
        first (covers frozen slots dataclasses) and falls back to
        ``hasattr`` for non-dataclass events.
        """
        for entry in self.entries:
            field_name = entry.meta.tenant_field
            if not field_name:
                continue
            cls = entry.event_cls
            fields = getattr(cls, "__dataclass_fields__", None)
            if fields is not None:
                if field_name in fields:
                    continue
            elif hasattr(cls, field_name):
                continue
            raise WebhookTenantFieldMissing(
                event_name=entry.meta.name,
                event_cls=cls.__qualname__,
                tenant_field=field_name,
            )


class WebhookEventRegistry:
    def discover(
        self,
        roots: tuple[type, ...] = (BaseEvent, MutableBaseEvent),
    ) -> WebhookCatalog:
        seen: set[type] = set()
        entries: list[WebhookEventEntry] = []
        by_name: dict[str, str] = {}

        def walk(cls: type) -> None:
            for sub in cls.__subclasses__():
                if sub in seen:
                    continue
                seen.add(sub)
                # Pre-events have abortable=True — skip them but keep
                # walking their descendants in case a post-event root
                # sits under them (defensive).
                if getattr(sub, "abortable", False):
                    walk(sub)
                    continue
                # Only pick up webhook_meta declared on *this* class,
                # not inherited. Inherited marker would double-register
                # the ancestor's event under a subclass' qualname.
                meta = sub.__dict__.get("webhook_meta")
                if isinstance(meta, WebhookMeta):
                    name = meta.name
                    if name in by_name:
                        raise WebhookEventCollision(
                            name,
                            by_name[name],
                            sub.__qualname__,
                        )
                    by_name[name] = sub.__qualname__
                    entries.append(WebhookEventEntry(event_cls=sub, meta=meta))
                walk(sub)

        for root in roots:
            walk(root)
        return WebhookCatalog(entries=tuple(entries))
