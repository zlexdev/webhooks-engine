"""Ingestion seam — the durable producer→consumer event queue.

A producing service ``append``\\s :class:`IncomingEvent`\\s (or writes the row
directly per the documented schema); the engine ``claim_events``, fans them out,
then ``ack``\\s. Atomicity of the claim is the backend's job (PG
``FOR UPDATE SKIP LOCKED`` / Mongo ``find_one_and_update``) so multiple engine
replicas can share one store without double-processing.

Ships two backends: :class:`PgEventStore`, :class:`MongoEventStore`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

from webhook_engine.ingest.events import IncomingEvent

__all__ = ["BaseEventStore"]


class BaseEventStore(ABC):
    @abstractmethod
    async def append(self, event: IncomingEvent) -> None:
        """Producer side — durably persist one event as PENDING.

        Idempotent on ``event.event_id``: appending a duplicate id is a no-op,
        so a producer that retries its write never enqueues twice.
        """

    async def append_many(self, events: list[IncomingEvent]) -> None:
        """Durably persist a batch of events as PENDING in one shot.

        Same per-event idempotency as :meth:`append` (duplicate ``event_id``\\s
        are skipped). The default loops :meth:`append`; durable backends override
        it with a single multi-row write. Empty input is a no-op.
        """
        for event in events:
            await self.append(event)

    @abstractmethod
    async def claim_events(
        self, now: datetime, lease_ttl_s: int, batch_size: int
    ) -> list[IncomingEvent]:
        """Atomically lease up to ``batch_size`` PENDING events.

        Claimed events move to CLAIMED with a lease expiring at
        ``now + lease_ttl_s``; a crash before ``ack`` lets ``reclaim_stale``
        return them to PENDING.
        """

    @abstractmethod
    async def ack(self, event_ids: list[str]) -> None:
        """Mark events DONE once their deliveries are enqueued."""

    @abstractmethod
    async def mark_failed(self, event_id: str, error: str) -> None:
        """Park a permanently un-ingestable event (e.g. malformed payload)."""

    @abstractmethod
    async def reclaim_stale(self, now: datetime) -> int:
        """Return CLAIMED events whose lease expired to PENDING. Returns count."""

    @abstractmethod
    async def aclose(self) -> None:
        """Release the backend connection. Idempotent."""
