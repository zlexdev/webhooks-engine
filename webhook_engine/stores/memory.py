"""In-memory delivery store — default for tests and local dev.

Zero-dependency test double for the storage contract. Single-process only:
lease atomicity is held by an in-process lock, not a shared backend.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta

from webhook_engine.enums import DeliveryStatus
from webhook_engine.stores.base import BaseWebhookDeliveryStore
from webhook_engine.types import AttemptRecord, DeliveryId, DeliveryRecord

__all__ = ["MemoryWebhookDeliveryStore"]


class MemoryWebhookDeliveryStore(BaseWebhookDeliveryStore):
    def __init__(self) -> None:
        self._records: dict[DeliveryId, DeliveryRecord] = {}
        self._attempts: dict[DeliveryId, list[AttemptRecord]] = {}
        self._lease_expiry: dict[DeliveryId, datetime] = {}
        self._lock = asyncio.Lock()

    async def enqueue_many(self, records: list[DeliveryRecord]) -> None:
        async with self._lock:
            for r in records:
                if r.delivery_id not in self._records:
                    self._records[r.delivery_id] = r
                    self._attempts[r.delivery_id] = []

    async def mark_sent(self, delivery_id: DeliveryId, attempt: AttemptRecord) -> None:
        async with self._lock:
            self._set_status(delivery_id, DeliveryStatus.SENT)
            self._attempts[delivery_id].append(attempt)
            self._lease_expiry.pop(delivery_id, None)

    async def schedule_retry(
        self,
        delivery_id: DeliveryId,
        fire_at: datetime,
        attempt: AttemptRecord,
    ) -> None:
        async with self._lock:
            rec = self._records[delivery_id]
            self._records[delivery_id] = replace(
                rec,
                status=DeliveryStatus.FAILED_RETRY,
                fire_at=fire_at,
                attempts=rec.attempts + 1,
            )
            self._attempts[delivery_id].append(attempt)
            self._lease_expiry.pop(delivery_id, None)

    async def mark_dead(self, delivery_id: DeliveryId, attempt: AttemptRecord) -> None:
        async with self._lock:
            self._set_status(delivery_id, DeliveryStatus.DEAD_LETTERED)
            self._attempts[delivery_id].append(attempt)
            self._lease_expiry.pop(delivery_id, None)

    async def append_attempt(self, delivery_id: DeliveryId, attempt: AttemptRecord) -> None:
        async with self._lock:
            self._attempts.setdefault(delivery_id, []).append(attempt)

    async def get(self, delivery_id: DeliveryId) -> DeliveryRecord | None:
        return self._records.get(delivery_id)

    async def recent_for_subscription(self, sub_id: str, limit: int) -> list[DeliveryRecord]:
        return [r for r in self._records.values() if r.subscription_id == sub_id][-limit:]

    async def claim_batch(
        self, now: datetime, lease_ttl_s: int, batch_size: int
    ) -> list[DeliveryRecord]:
        async with self._lock:
            ready = sorted(
                (
                    r
                    for r in self._records.values()
                    if r.status in (DeliveryStatus.PENDING, DeliveryStatus.FAILED_RETRY)
                    and r.fire_at <= now
                ),
                key=lambda r: r.fire_at,
            )
            batch = ready[:batch_size]
            expiry = now + timedelta(seconds=lease_ttl_s)
            for r in batch:
                self._records[r.delivery_id] = replace(r, status=DeliveryStatus.IN_FLIGHT)
                self._lease_expiry[r.delivery_id] = expiry
            return batch

    async def reclaim_stale(self, now: datetime) -> int:
        async with self._lock:
            stale = [
                did
                for did, expiry in self._lease_expiry.items()
                if expiry < now and self._records[did].status == DeliveryStatus.IN_FLIGHT
            ]
            for did in stale:
                self._records[did] = replace(self._records[did], status=DeliveryStatus.PENDING)
                self._lease_expiry.pop(did, None)
            return len(stale)

    def _set_status(self, delivery_id: DeliveryId, status: DeliveryStatus) -> None:
        self._records[delivery_id] = replace(self._records[delivery_id], status=status)
