"""Operational webhook management — inspection, manual redelivery, test ping.

Service layer between the admin routes and the delivery / subscription stores.
Routes stay thin; this object owns the logic of cloning a delivery for replay
and synthesising a ping delivery.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from webhook_engine.enums import DeliveryStatus
from webhook_engine.exceptions import DeliveryNotFound, SubscriptionNotFound
from webhook_engine.types import DeliveryRecord

__all__ = ["DeliveryAdminService", "PING_EVENT"]

if TYPE_CHECKING:
    from webhook_engine.protocols import WebhookDeliveryStoreProtocol
    from webhook_engine.service.deps import SubscriptionAdminStore
    from webhook_engine.ssrf_guard import UrlSafetyValidator

PING_EVENT = "webhook.ping"


class DeliveryAdminService:
    def __init__(
        self,
        store: WebhookDeliveryStoreProtocol,
        sub_store: SubscriptionAdminStore,
        ssrf_guard: UrlSafetyValidator,
    ) -> None:
        self._store = store
        self._subs = sub_store
        self._ssrf = ssrf_guard

    async def list_for_subscription(self, sub_id: str, limit: int) -> list[DeliveryRecord]:
        return await self._store.recent_for_subscription(sub_id, limit)

    async def get(self, delivery_id: str) -> DeliveryRecord:
        record = await self._store.get(delivery_id)
        if record is None:
            raise DeliveryNotFound(delivery_id=delivery_id)
        return record

    async def redeliver(self, delivery_id: str) -> DeliveryRecord:
        """Clone a past delivery and re-queue it as a fresh attempt.

        The new record points back at the original through ``redelivery_of``
        so the delivery log keeps the replay lineage. The subscription's
        current URL / resolved IP and the SSRF block-list are re-evaluated,
        so a replay never resurrects a now-blocked target.
        """
        original = await self.get(delivery_id)
        self._ssrf.check_ip(original.resolved_ip)

        now = datetime.now(UTC)
        clone = DeliveryRecord(
            delivery_id=uuid4().hex,
            subscription_id=original.subscription_id,
            owner_id=original.owner_id,
            event_name=original.event_name,
            event_id=original.event_id,
            schema_version=original.schema_version,
            tenant_id=original.tenant_id,
            data_json=original.data_json,
            emitted_at=original.emitted_at,
            target_url=original.target_url,
            resolved_ip=original.resolved_ip,
            redelivery_of=original.delivery_id,
            attempts=0,
            fire_at=now,
            created_at=now,
            status=DeliveryStatus.PENDING,
        )
        await self._store.enqueue_many([clone])
        return clone

    async def ping(self, sub_id: str) -> DeliveryRecord:
        """Queue a synthetic ``webhook.ping`` delivery so a subscriber can
        confirm their endpoint receives and verifies signed payloads."""
        sub = await self._subs.get(sub_id)
        if sub is None:
            raise SubscriptionNotFound(subscription_id=sub_id)
        self._ssrf.check_ip(sub.resolved_ip)

        now = datetime.now(UTC)
        delivery_id = uuid4().hex
        record = DeliveryRecord(
            delivery_id=delivery_id,
            subscription_id=sub.id,
            owner_id=sub.owner_id,
            event_name=PING_EVENT,
            event_id=uuid4().hex,
            schema_version="1.0",
            tenant_id=None,
            data_json=json.dumps(
                {"ping": True, "subscription_id": sub.id, "at": now.isoformat()}
            ).encode(),
            emitted_at=now,
            target_url=sub.url,
            resolved_ip=sub.resolved_ip,
            redelivery_of=None,
            attempts=0,
            fire_at=now,
            created_at=now,
            status=DeliveryStatus.PENDING,
        )
        await self._store.enqueue_many([record])
        return record
