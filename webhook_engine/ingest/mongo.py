"""MongoDB-backed ingestion event store.

Uses Motor (``AsyncIOMotorCollection``).  The collection is injected by the
caller; the Motor client lifecycle is owned externally, so ``aclose()`` is a
documented no-op — dispose the client at the application boundary instead.

Call ``ensure_schema()`` once at startup to create the compound index that
powers the PENDING-ordered scan.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from pymongo import ASCENDING, ReturnDocument
from pymongo.errors import BulkWriteError, DuplicateKeyError

from libs.shared.logging import get_logger
from webhook_engine.ingest.base import BaseEventStore
from webhook_engine.ingest.events import IncomingEvent

__all__ = ["MongoEventStore"]

if TYPE_CHECKING:
    from motor.motor_asyncio import AsyncIOMotorCollection as AsyncIOMotorCollection

logger = get_logger("webhooks.ingest.mongo")


class MongoEventStore(BaseEventStore):
    """MongoDB-backed durable queue for :class:`~webhook_engine.ingest.events.IncomingEvent`.

    Args:
        collection: An already-initialised Motor collection.  The collection
            name and database are chosen by the caller.
        lock_ttl_s: Seconds after which a CLAIMED lease is considered stale
            and eligible for reclamation. Defaults to 30.

    Note:
        ``aclose()`` is intentionally a no-op: the Motor client that owns the
        connection pool was created by the caller and must be closed there.
        This avoids double-dispose when multiple stores share one client.
    """

    def __init__(
        self,
        collection: AsyncIOMotorCollection[Any],
        lock_ttl_s: int = 30,
    ) -> None:
        self._col = collection
        self._lock_ttl_s = lock_ttl_s
        self._closed = False

    async def ensure_schema(self) -> None:
        """Create the compound index on ``(status, created_at)``.

        The ``_id`` unique index is implicit (MongoDB always creates it).
        Safe to call on every startup — ``create_index`` is idempotent.
        """
        await self._col.create_index(
            [("status", ASCENDING), ("created_at", ASCENDING)],
            name="status_created_at_idx",
            background=True,
        )
        logger.info(
            "mongo_event_store.schema_ensured",
            collection=self._col.name,
        )

    async def append(self, event: IncomingEvent) -> None:
        """Persist *event* as PENDING.  Duplicate ``event_id`` is a no-op."""
        created_at = event.created_at or datetime.now(UTC)
        doc: dict[str, Any] = {
            "_id": event.event_id,
            "event": event.event,
            "tenant_id": event.tenant_id,
            "schema_version": event.schema_version,
            "data": event.data,
            "status": "pending",
            "created_at": created_at,
        }
        try:
            await self._col.insert_one(doc)
        except DuplicateKeyError:
            # Idempotent — producer retry, not an error.
            pass
        else:
            logger.debug(
                "mongo_event_store.appended",
                event_id=event.event_id,
                event_name=event.event,
            )

    async def append_many(self, events: list[IncomingEvent]) -> None:
        """Persist a batch as PENDING in one round trip (``insert_many``).

        ``ordered=False`` so a duplicate ``_id`` (idempotent producer retry)
        skips that doc and the rest still insert; the resulting bulk
        ``DuplicateKeyError`` is swallowed. Empty input is a no-op.
        """
        if not events:
            return
        docs: list[dict[str, Any]] = [
            {
                "_id": e.event_id,
                "event": e.event,
                "tenant_id": e.tenant_id,
                "schema_version": e.schema_version,
                "data": e.data,
                "status": "pending",
                "created_at": e.created_at or datetime.now(UTC),
            }
            for e in events
        ]
        try:
            await self._col.insert_many(docs, ordered=False)
        except BulkWriteError as exc:
            non_dup = [e for e in exc.details.get("writeErrors", []) if e.get("code") != 11000]
            if non_dup:
                raise
        logger.debug(
            "mongo_event_store.appended_many", count=len(events), collection=self._col.name
        )

    async def claim_events(
        self, now: datetime, lease_ttl_s: int, batch_size: int
    ) -> list[IncomingEvent]:
        """Atomically lease up to *batch_size* PENDING events.

        Uses ``find_one_and_update`` with ``sort`` so each call is an atomic
        compare-and-swap; multiple engine replicas can share the collection
        without double-claiming.
        """
        _ = lease_ttl_s
        events: list[IncomingEvent] = []
        for _ in range(batch_size):
            doc: dict[str, Any] | None = await self._col.find_one_and_update(
                {"status": "pending"},
                {"$set": {"status": "claimed", "locked_at": now}},
                sort=[("created_at", ASCENDING)],
                return_document=ReturnDocument.AFTER,
            )
            if doc is None:
                break
            created_at: datetime = doc["created_at"]
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=UTC)
            events.append(
                IncomingEvent(
                    event_id=doc["_id"],
                    event=doc["event"],
                    tenant_id=doc.get("tenant_id"),
                    schema_version=doc.get("schema_version", "1.0"),
                    data=doc.get("data", {}),
                    created_at=created_at,
                )
            )
        if events:
            logger.debug(
                "mongo_event_store.claimed",
                count=len(events),
                collection=self._col.name,
            )
        return events

    async def ack(self, event_ids: list[str]) -> None:
        if not event_ids:
            return
        await self._col.update_many(
            {"_id": {"$in": event_ids}},
            {"$set": {"status": "done"}},
        )
        logger.debug(
            "mongo_event_store.acked",
            count=len(event_ids),
            collection=self._col.name,
        )

    async def mark_failed(self, event_id: str, error: str) -> None:
        """Park *event_id* as FAILED with *error* detail."""
        await self._col.update_one(
            {"_id": event_id},
            {"$set": {"status": "failed", "error": error}},
        )
        logger.warning(
            "mongo_event_store.marked_failed",
            event_id=event_id,
            error=error,
            collection=self._col.name,
        )

    async def reclaim_stale(self, now: datetime) -> int:
        """Return CLAIMED events whose lease expired back to PENDING.

        Returns the number of documents reclaimed.
        """
        cutoff = now - timedelta(seconds=self._lock_ttl_s)
        result = await self._col.update_many(
            {"status": "claimed", "locked_at": {"$lt": cutoff}},
            {"$set": {"status": "pending"}, "$unset": {"locked_at": ""}},
        )
        count: int = result.modified_count
        if count:
            logger.info(
                "mongo_event_store.reclaimed_stale",
                count=count,
                collection=self._col.name,
            )
        return count

    async def aclose(self) -> None:
        """No-op — the Motor client is owned by the caller.

        Set the internal flag so callers can safely call this multiple times
        without confusion; the client connection pool is unaffected.
        """
        self._closed = True
