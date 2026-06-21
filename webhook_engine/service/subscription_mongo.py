"""MongoDB-backed subscription store (Motor async driver).

Collection document shape
--------------------------
{
    "_id":         "<sub_id>",        # str — same as Subscription.id
    "owner_id":    "<owner_id>",
    "url":         "<url>",
    "secret":      "<secret>",
    "events":      ["event.name", ...],   # array — supports multikey index
    "status":      "active" | "paused" | "deleted",
    "created_at":  <datetime UTC>,
    "resolved_ip": "<ip>"
}

Deviations from the Redis store
---------------------------------
- ``pause``/``resume`` return ``False`` when no document was matched
  (``matched_count == 0``).  The Redis ``_set_status`` bug (always True)
  is NOT replicated.
- ``invalidate_secret`` is a documented no-op.
- ``get_secret`` raises ``KeyError`` on a missing sub, matching Redis behaviour.
"""

from __future__ import annotations

import secrets
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import structlog

from webhook_engine.enums import SubscriptionStatus
from webhook_engine.protocols import (
    SecretMaterial,
    SecretReaderProtocol,
    SubscriptionReaderProtocol,
)
from webhook_engine.service.subscription_store import Subscription, _resolve_ip  # noqa: PLC2701
from webhook_engine.types import SubscriptionSnapshot

__all__ = [
    "MongoSubscriptionStore",
    "make_mongo_readers",
]

if TYPE_CHECKING:
    from motor.motor_asyncio import AsyncIOMotorCollection

log: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


class MongoSubscriptionStore:
    """CRUD for subscriptions backed by a MongoDB collection (Motor)."""

    def __init__(self, collection: AsyncIOMotorCollection) -> None:
        self._col = collection

    async def ensure_schema(self) -> None:
        """Create indexes on the collection (idempotent)."""
        await self._col.create_index("owner_id", background=True)
        # Multikey index — one entry per element in the events array.
        await self._col.create_index("events", background=True)
        await self._col.create_index("status", background=True)
        log.info("mongo_subscription_store.indexes_ensured", collection=self._col.name)

    async def for_event(
        self,
        event_name: str,
        _tenant_id: str | None,  # noqa: ARG002 — reserved for future multi-tenant filtering
    ) -> list[SubscriptionSnapshot]:
        """Return active subscriptions interested in *event_name*.

        MongoDB matches *event_name* against the ``events`` array via the
        multikey index (equality match on an array field).
        """
        cursor = self._col.find(
            {"events": event_name, "status": SubscriptionStatus.ACTIVE.value},
            {"_id": 1, "owner_id": 1, "url": 1, "resolved_ip": 1},
        )
        results: list[SubscriptionSnapshot] = []
        async for doc in cursor:
            results.append(
                SubscriptionSnapshot(
                    id=doc["_id"],
                    owner_id=doc["owner_id"],
                    url=doc["url"],
                    resolved_ip=doc["resolved_ip"],
                    retry_overrides=None,
                )
            )
        return results

    async def get_secret(self, subscription_id: str) -> SecretMaterial:
        doc = await self._col.find_one({"_id": subscription_id}, {"secret": 1})
        if doc is None:
            raise KeyError(f"subscription not found: {subscription_id!r}")
        return SecretMaterial(
            current=doc["secret"],
            previous=None,
            previous_expires_at=None,
        )

    def invalidate_secret(self, _subscription_id: str) -> None:  # noqa: ARG002
        """No-op — Mongo store has no in-process secret cache."""

    async def create(
        self,
        owner_id: str,
        url: str,
        events: list[str],
        secret: str | None = None,
    ) -> Subscription:
        resolved_ip = await _resolve_ip(url)
        sub_id = uuid4().hex
        sub_secret = secret or secrets.token_hex(32)
        now = datetime.now(UTC)

        doc: dict[str, Any] = {
            "_id": sub_id,
            "owner_id": owner_id,
            "url": url,
            "secret": sub_secret,
            "events": events,
            "status": SubscriptionStatus.ACTIVE.value,
            "created_at": now,
            "resolved_ip": resolved_ip,
        }
        await self._col.insert_one(doc)
        log.info("mongo_subscription_store.created", sub_id=sub_id, owner_id=owner_id)
        return Subscription(
            id=sub_id,
            owner_id=owner_id,
            url=url,
            secret=sub_secret,
            events=tuple(events),
            status=SubscriptionStatus.ACTIVE,
            created_at=now,
            resolved_ip=resolved_ip,
        )

    async def get(self, sub_id: str) -> Subscription | None:
        doc = await self._col.find_one({"_id": sub_id})
        if doc is None:
            return None
        return _doc_to_sub(doc)

    async def list_for_owner(self, owner_id: str) -> list[Subscription]:
        cursor = self._col.find({"owner_id": owner_id})
        return [_doc_to_sub(doc) async for doc in cursor]

    async def delete(self, sub_id: str) -> bool:
        result = await self._col.delete_one({"_id": sub_id})
        deleted = bool(result.deleted_count > 0)
        if deleted:
            log.info("mongo_subscription_store.deleted", sub_id=sub_id)
        return deleted

    async def pause(self, sub_id: str) -> bool:
        return await self._set_status(sub_id, SubscriptionStatus.PAUSED)

    async def resume(self, sub_id: str) -> bool:
        return await self._set_status(sub_id, SubscriptionStatus.ACTIVE)

    async def _set_status(self, sub_id: str, status: SubscriptionStatus) -> bool:
        result = await self._col.update_one(
            {"_id": sub_id},
            {"$set": {"status": status.value}},
        )
        # matched_count is 0 when no document has that _id — correct False return.
        existed = bool(result.matched_count > 0)
        if existed:
            log.info(
                "mongo_subscription_store.status_changed",
                sub_id=sub_id,
                status=status.value,
            )
        return existed


class _MongoSubscriptionReader(SubscriptionReaderProtocol):
    def __init__(self, store: MongoSubscriptionStore) -> None:
        self._store = store

    async def for_event(self, event_name: str, tenant_id: str | None) -> list[SubscriptionSnapshot]:
        return await self._store.for_event(event_name, tenant_id)


class _MongoSecretReader(SecretReaderProtocol):
    def __init__(self, store: MongoSubscriptionStore) -> None:
        self._store = store

    async def get(self, subscription_id: str) -> SecretMaterial:
        return await self._store.get_secret(subscription_id)

    def invalidate(self, subscription_id: str) -> None:
        self._store.invalidate_secret(subscription_id)


def make_mongo_readers(
    store: MongoSubscriptionStore,
) -> tuple[SubscriptionReaderProtocol, SecretReaderProtocol]:
    return _MongoSubscriptionReader(store), _MongoSecretReader(store)


def _doc_to_sub(doc: dict[str, Any]) -> Subscription:
    """Convert a Motor document to a ``Subscription`` dataclass."""
    created_at: datetime = doc["created_at"]
    if isinstance(created_at, str):
        created_at = datetime.fromisoformat(created_at)

    return Subscription(
        id=doc["_id"],
        owner_id=doc["owner_id"],
        url=doc["url"],
        secret=doc["secret"],
        events=tuple(doc.get("events", [])),
        status=SubscriptionStatus(doc["status"]),
        created_at=created_at,
        resolved_ip=doc["resolved_ip"],
    )
