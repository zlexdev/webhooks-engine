"""MongoDB-backed webhook delivery store.

Layout (single collection, one document per delivery):

    {
        "_id":             delivery_id (str),
        "subscription_id": str,
        "owner_id":        str,
        "event_name":      str,
        "event_id":        str,
        "schema_version":  str,
        "tenant_id":       str | None,
        "data_json":       bson.Binary   # raw bytes; decoded back to bytes on read
        "emitted_at":      datetime (UTC-aware),
        "target_url":      str,
        "resolved_ip":     str,
        "redelivery_of":   str | None,
        "attempts":        int,
        "fire_at":         datetime (UTC-aware),
        "created_at":      datetime (UTC-aware),
        "status":          str  (DeliveryStatus value),
        "lease_expiry":    datetime | None,
        "attempt_log":     [   # capped embedded array; newest first
            {
                "attempt":        int,
                "attempted_at":   datetime (UTC-aware),
                "http_code":      int | None,
                "duration_ms":    int,
                "error":          str | None,
                "response_snippet": str,
            },
            ...
        ],
    }

Claim leasing:
    ``find_one_and_update`` with a per-document ``$set`` of ``status=IN_FLIGHT``
    and ``lease_expiry=now+lease_ttl_s``.  The filter on ``fire_at + status``
    guarantees at-most-once delivery per claim cycle; stale leases are reset
    by :meth:`reclaim_stale`.

``data_json`` round-trip:
    Stored as ``bson.Binary(payload, subtype=0)`` so MongoDB keeps the exact
    byte sequence without charset coercion.  On read, ``bson.Binary`` is a
    ``bytes`` subclass, so the cast ``bytes(doc["data_json"])`` is always safe.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import bson
from pymongo import ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import BulkWriteError

from libs.shared.logging import get_logger
from webhook_engine.enums import DeliveryStatus
from webhook_engine.stores.base import BaseWebhookDeliveryStore
from webhook_engine.types import AttemptRecord, DeliveryId, DeliveryRecord

__all__ = ["MongoWebhookDeliveryStore"]

if TYPE_CHECKING:
    from motor.motor_asyncio import AsyncIOMotorCollection

_log = get_logger("webhooks.stores.mongo")

_CLAIMABLE = [DeliveryStatus.PENDING.value, DeliveryStatus.FAILED_RETRY.value]


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


def _record_to_doc(record: DeliveryRecord) -> dict[str, Any]:
    return {
        "_id": record.delivery_id,
        "subscription_id": record.subscription_id,
        "owner_id": record.owner_id,
        "event_name": record.event_name,
        "event_id": record.event_id,
        "schema_version": record.schema_version,
        "tenant_id": record.tenant_id,
        "data_json": bson.Binary(record.data_json, subtype=0),
        "emitted_at": _ensure_utc(record.emitted_at),
        "target_url": record.target_url,
        "resolved_ip": record.resolved_ip,
        "redelivery_of": record.redelivery_of,
        "attempts": record.attempts,
        "fire_at": _ensure_utc(record.fire_at),
        "created_at": _ensure_utc(record.created_at),
        "status": record.status.value,
        "lease_expiry": None,
        "attempt_log": [],
    }


def _doc_to_record(doc: dict[str, Any]) -> DeliveryRecord:
    raw_data = doc["data_json"]
    # bson.Binary is a bytes subclass; plain bytes also accepted gracefully.
    data_bytes: bytes = bytes(raw_data)

    def _dt(key: str) -> datetime:
        val: datetime = doc[key]
        return _ensure_utc(val)

    return DeliveryRecord(
        delivery_id=doc["_id"],
        subscription_id=doc["subscription_id"],
        owner_id=doc["owner_id"],
        event_name=doc["event_name"],
        event_id=doc["event_id"],
        schema_version=doc["schema_version"],
        tenant_id=doc.get("tenant_id"),
        data_json=data_bytes,
        emitted_at=_dt("emitted_at"),
        target_url=doc["target_url"],
        resolved_ip=doc["resolved_ip"],
        redelivery_of=doc.get("redelivery_of"),
        attempts=doc.get("attempts", 0),
        fire_at=_dt("fire_at"),
        created_at=_dt("created_at"),
        status=DeliveryStatus(doc.get("status", DeliveryStatus.PENDING.value)),
    )


def _attempt_to_subdoc(attempt: AttemptRecord) -> dict[str, Any]:
    return {
        "attempt": attempt.attempt,
        "attempted_at": _ensure_utc(attempt.attempted_at),
        "http_code": attempt.http_code,
        "duration_ms": attempt.duration_ms,
        "error": attempt.error,
        "response_snippet": attempt.response_snippet,
    }


class MongoWebhookDeliveryStore(BaseWebhookDeliveryStore):
    """Motor-based delivery store — one MongoDB collection, embedded attempt log.

    Args:
        collection:    An already-initialised ``AsyncIOMotorCollection``.
                       Database and collection name are chosen by the caller.
        attempts_limit: Maximum number of attempt sub-documents retained per
                        delivery.  Older entries are dropped via ``$slice``.
                        Defaults to 20.
    """

    def __init__(
        self,
        collection: AsyncIOMotorCollection[Any],
        attempts_limit: int = 20,
    ) -> None:
        self._col = collection
        self._attempts_limit = attempts_limit

    async def ensure_schema(self) -> None:
        """Create indexes required for polling, leasing, and subscription queries.

        Safe to call on every startup — ``create_index`` is idempotent.
        """
        # Claim / reclaim scan: claimable statuses ordered by fire_at
        await self._col.create_index(
            [("status", ASCENDING), ("fire_at", ASCENDING)],
            name="status_fire_at_idx",
            background=True,
        )
        # Stale-lease reclaim: IN_FLIGHT rows filtered by lease_expiry
        await self._col.create_index(
            [("status", ASCENDING), ("lease_expiry", ASCENDING)],
            name="status_lease_expiry_idx",
            background=True,
        )
        # recent_for_subscription query
        await self._col.create_index(
            [("subscription_id", ASCENDING), ("created_at", DESCENDING)],
            name="subscription_created_at_idx",
            background=True,
        )
        _log.info(
            "mongo_delivery_store.schema_ensured",
            collection=self._col.name,
        )

    async def enqueue_many(self, records: list[DeliveryRecord]) -> None:
        """Insert delivery records, ignoring duplicates (idempotent by _id)."""
        if not records:
            return
        docs = [_record_to_doc(r) for r in records]
        try:
            await self._col.insert_many(docs, ordered=False)
        except BulkWriteError as exc:
            # Filter out duplicate-key errors (code 11000); re-raise anything else.
            non_dup = [e for e in exc.details.get("writeErrors", []) if e.get("code") != 11000]
            if non_dup:
                _log.error(
                    "mongo_delivery_store.enqueue_many_error",
                    non_duplicate_errors=non_dup,
                )
                raise
            _log.debug(
                "mongo_delivery_store.enqueue_many_duplicates_skipped",
                count=len(exc.details.get("writeErrors", [])),
            )

    async def claim_batch(
        self,
        now: datetime,
        lease_ttl_s: int,
        batch_size: int,
    ) -> list[DeliveryRecord]:
        """Atomically lease up to *batch_size* ready deliveries.

        Uses ``find_one_and_update`` in a loop (one round-trip per claim) so
        concurrent dispatcher workers on separate processes cannot double-claim
        the same row.
        """
        now = _ensure_utc(now)
        lease_expiry = now + timedelta(seconds=lease_ttl_s)
        claimed: list[DeliveryRecord] = []

        for _ in range(batch_size):
            doc: dict[str, Any] | None = await self._col.find_one_and_update(
                {
                    "status": {"$in": _CLAIMABLE},
                    "fire_at": {"$lte": now},
                },
                {
                    "$set": {
                        "status": DeliveryStatus.IN_FLIGHT.value,
                        "lease_expiry": lease_expiry,
                    }
                },
                sort=[("fire_at", ASCENDING)],
                return_document=ReturnDocument.AFTER,
            )
            if doc is None:
                break
            # Reflect the just-written IN_FLIGHT status in the returned record.
            record = _doc_to_record(doc)
            claimed.append(record)

        if claimed:
            _log.debug(
                "mongo_delivery_store.claim_batch",
                count=len(claimed),
            )
        return claimed

    async def reclaim_stale(self, now: datetime) -> int:
        """Reset IN_FLIGHT deliveries whose lease has expired back to PENDING.

        Returns the number of documents reclaimed.
        """
        now = _ensure_utc(now)
        result = await self._col.update_many(
            {
                "status": DeliveryStatus.IN_FLIGHT.value,
                "lease_expiry": {"$lt": now},
            },
            {
                "$set": {
                    "status": DeliveryStatus.PENDING.value,
                    "lease_expiry": None,
                }
            },
        )
        count: int = result.modified_count
        if count:
            _log.info(
                "mongo_delivery_store.reclaim_stale",
                count=count,
            )
        return count

    async def mark_sent(
        self,
        delivery_id: DeliveryId,
        attempt: AttemptRecord,
    ) -> None:
        """Transition delivery to SENT and append the final attempt record."""
        await self._col.update_one(
            {"_id": delivery_id},
            {
                "$set": {
                    "status": DeliveryStatus.SENT.value,
                    "attempts": attempt.attempt,
                    "lease_expiry": None,
                },
                "$push": {
                    "attempt_log": {
                        "$each": [_attempt_to_subdoc(attempt)],
                        "$position": 0,
                        "$slice": self._attempts_limit,
                    }
                },
            },
        )
        _log.debug(
            "mongo_delivery_store.mark_sent",
            delivery_id=delivery_id,
            attempt=attempt.attempt,
        )

    async def schedule_retry(
        self,
        delivery_id: DeliveryId,
        fire_at: datetime,
        attempt: AttemptRecord,
    ) -> None:
        """Move delivery to FAILED_RETRY with an updated fire_at and attempt count."""
        fire_at = _ensure_utc(fire_at)
        await self._col.update_one(
            {"_id": delivery_id},
            {
                "$set": {
                    "status": DeliveryStatus.FAILED_RETRY.value,
                    "fire_at": fire_at,
                    "attempts": attempt.attempt,
                    "lease_expiry": None,
                },
                "$push": {
                    "attempt_log": {
                        "$each": [_attempt_to_subdoc(attempt)],
                        "$position": 0,
                        "$slice": self._attempts_limit,
                    }
                },
            },
        )
        _log.debug(
            "mongo_delivery_store.schedule_retry",
            delivery_id=delivery_id,
            fire_at=fire_at.isoformat(),
            attempt=attempt.attempt,
        )

    async def mark_dead(
        self,
        delivery_id: DeliveryId,
        attempt: AttemptRecord,
    ) -> None:
        """Transition delivery to DEAD_LETTERED and record the final attempt."""
        await self._col.update_one(
            {"_id": delivery_id},
            {
                "$set": {
                    "status": DeliveryStatus.DEAD_LETTERED.value,
                    "attempts": attempt.attempt,
                    "lease_expiry": None,
                },
                "$push": {
                    "attempt_log": {
                        "$each": [_attempt_to_subdoc(attempt)],
                        "$position": 0,
                        "$slice": self._attempts_limit,
                    }
                },
            },
        )
        _log.debug(
            "mongo_delivery_store.mark_dead",
            delivery_id=delivery_id,
            attempt=attempt.attempt,
        )

    async def append_attempt(
        self,
        delivery_id: DeliveryId,
        attempt: AttemptRecord,
    ) -> None:
        """Append an attempt record without changing delivery status.

        The embedded ``attempt_log`` array is kept to at most
        ``attempts_limit`` entries; the newest entry sits at index 0.
        """
        await self._col.update_one(
            {"_id": delivery_id},
            {
                "$push": {
                    "attempt_log": {
                        "$each": [_attempt_to_subdoc(attempt)],
                        "$position": 0,
                        "$slice": self._attempts_limit,
                    }
                }
            },
        )

    async def get(self, delivery_id: DeliveryId) -> DeliveryRecord | None:
        """Fetch a single delivery record by id, or ``None`` if not found."""
        doc: dict[str, Any] | None = await self._col.find_one({"_id": delivery_id})
        if doc is None:
            return None
        return _doc_to_record(doc)

    async def recent_for_subscription(
        self,
        sub_id: str,
        limit: int,
    ) -> list[DeliveryRecord]:
        """Return the most recent *limit* deliveries for a subscription, newest first."""
        cursor = (
            self._col.find({"subscription_id": sub_id}).sort("created_at", DESCENDING).limit(limit)
        )
        docs: list[dict[str, Any]] = await cursor.to_list(length=limit)
        return [_doc_to_record(doc) for doc in docs]
