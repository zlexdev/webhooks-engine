"""Claimed-event → queued-delivery fan-out.

The read side of ingestion: pull a batch of events the producer wrote, resolve
every active subscription for each event, build one :class:`DeliveryRecord` per
(subscription, event), enqueue them, and ack. Pure orchestration — all I/O is
behind the store / reader protocols, so it unit-tests with fakes.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from libs.shared.logging import get_logger
from webhook_engine.enums import DeliveryStatus
from webhook_engine.exceptions import InvalidWebhookTarget
from webhook_engine.types import DeliveryRecord

__all__ = ["EventIngestor", "IngestTickStats"]

if TYPE_CHECKING:
    from webhook_engine.ingest.base import BaseEventStore
    from webhook_engine.ingest.events import IncomingEvent
    from webhook_engine.protocols import (
        Clock,
        SubscriptionReaderProtocol,
        WebhookDeliveryStoreProtocol,
    )
    from webhook_engine.ssrf_guard import UrlSafetyValidator


@dataclass(frozen=True, slots=True)
class IngestTickStats:
    claimed: int
    fanned_out: int
    failed: int


class EventIngestor:
    def __init__(
        self,
        *,
        event_store: BaseEventStore,
        subscription_reader: SubscriptionReaderProtocol,
        delivery_store: WebhookDeliveryStoreProtocol,
        ssrf_guard: UrlSafetyValidator,
        clock: Clock,
        lease_ttl_s: int = 30,
        batch_size: int = 100,
    ) -> None:
        self._events = event_store
        self._subs = subscription_reader
        self._deliveries = delivery_store
        self._ssrf = ssrf_guard
        self._clock = clock
        self._lease_ttl_s = lease_ttl_s
        self._batch_size = batch_size
        self._log = get_logger("webhooks.ingestor")

    async def tick(self) -> IngestTickStats:
        now = self._clock()
        try:
            await self._events.reclaim_stale(now)
        except Exception as exc:  # noqa: BLE001 — reclaim failure mustn't kill the loop
            self._log.warn("ingest_reclaim_failed", error=str(exc), exc_info=True)

        events = await self._events.claim_events(now, self._lease_ttl_s, self._batch_size)
        if not events:
            return IngestTickStats(claimed=0, fanned_out=0, failed=0)

        acked: list[str] = []
        fanned_out = 0
        failed = 0
        for event in events:
            try:
                fanned_out += await self._fan_out(event, now)
                acked.append(event.event_id)
            except Exception as exc:  # noqa: BLE001 — one bad event must not block the batch
                failed += 1
                self._log.error(
                    "ingest_event_failed",
                    event_id=event.event_id,
                    event_name=event.event,
                    error=str(exc),
                    exc_info=True,
                )
                await self._events.mark_failed(event.event_id, str(exc))

        if acked:
            await self._events.ack(acked)
        return IngestTickStats(claimed=len(events), fanned_out=fanned_out, failed=failed)

    async def _fan_out(self, event: IncomingEvent, now: datetime) -> int:
        subscriptions = await self._subs.for_event(event.event, event.tenant_id)
        if not subscriptions:
            return 0

        data_json = json.dumps(event.data).encode()
        emitted_at = event.created_at or now
        records: list[DeliveryRecord] = []
        for sub in subscriptions:
            try:
                self._ssrf.check_ip(sub.resolved_ip)
            except InvalidWebhookTarget:
                self._log.warn(
                    "ingest_target_blocked",
                    subscription_id=sub.id,
                    event_name=event.event,
                )
                continue
            records.append(
                DeliveryRecord(
                    delivery_id=uuid4().hex,
                    subscription_id=sub.id,
                    owner_id=sub.owner_id,
                    event_name=event.event,
                    event_id=event.event_id,
                    schema_version=event.schema_version,
                    tenant_id=event.tenant_id,
                    data_json=data_json,
                    emitted_at=emitted_at,
                    target_url=sub.url,
                    resolved_ip=sub.resolved_ip,
                    redelivery_of=None,
                    attempts=0,
                    fire_at=now,
                    created_at=now,
                    status=DeliveryStatus.PENDING,
                )
            )

        if records:
            await self._deliveries.enqueue_many(records)
        return len(records)
