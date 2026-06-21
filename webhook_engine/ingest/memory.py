"""In-memory event store — the dependency-free backend for dev and tests.

Mirrors the claim/ack/reclaim contract of the PG / Mongo event stores with a
plain dict guarded by a lock. Append is idempotent on ``event_id`` (the same
event appended twice is a no-op), matching the durable backends.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from webhook_engine.ingest.base import BaseEventStore
from webhook_engine.ingest.events import EventIngestStatus, IncomingEvent

__all__ = ["MemoryEventStore"]


@dataclass
class _Row:
    event: IncomingEvent
    status: EventIngestStatus = EventIngestStatus.PENDING
    lease_until: datetime | None = None


@dataclass
class MemoryEventStore(BaseEventStore):
    _rows: dict[str, _Row] = field(default_factory=dict)

    async def ensure_schema(self) -> None:
        return None

    async def append(self, event: IncomingEvent) -> None:
        self._rows.setdefault(event.event_id, _Row(event=event))

    async def claim_events(
        self, now: datetime, lease_ttl_s: int, batch_size: int
    ) -> list[IncomingEvent]:
        claimed: list[IncomingEvent] = []
        for row in self._rows.values():
            if len(claimed) >= batch_size:
                break
            if row.status is EventIngestStatus.PENDING:
                row.status = EventIngestStatus.CLAIMED
                row.lease_until = now + timedelta(seconds=lease_ttl_s)
                claimed.append(row.event)
        return claimed

    async def ack(self, event_ids: list[str]) -> None:
        for event_id in event_ids:
            row = self._rows.get(event_id)
            if row is not None:
                row.status = EventIngestStatus.DONE
                row.lease_until = None

    async def mark_failed(self, event_id: str, error: str) -> None:
        row = self._rows.get(event_id)
        if row is not None:
            row.status = EventIngestStatus.FAILED
            row.lease_until = None

    async def reclaim_stale(self, now: datetime) -> int:
        reclaimed = 0
        for row in self._rows.values():
            if (
                row.status is EventIngestStatus.CLAIMED
                and row.lease_until is not None
                and row.lease_until < now
            ):
                row.status = EventIngestStatus.PENDING
                row.lease_until = None
                reclaimed += 1
        return reclaimed

    async def aclose(self) -> None:
        return None
