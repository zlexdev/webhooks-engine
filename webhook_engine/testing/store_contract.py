"""`StoreContract` — one behaviour suite for every `WebhookDeliveryStoreProtocol`.

A consumer subclasses it in its own tests and implements `make_store()`; pytest collects
every `test_*` against that store:

    class TestMyStore(StoreContract):
        async def make_store(self) -> WebhookDeliveryStoreProtocol:
            return MyStore(...)

Each case is also a plain coroutine (`case_*`) so a test can run one against a
deliberately broken store and expect it to fail.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from webhook_engine.enums import DeliveryStatus
from webhook_engine.protocols import WebhookDeliveryStoreProtocol
from webhook_engine.types import DEFAULT_PROFILE, AttemptRecord, DeliveryRecord

__all__ = ["StoreContract", "record"]

T0 = datetime(2026, 10, 2, 12, tzinfo=UTC)
LEASE_S = 30
BATCH = 10
OTHER_PROFILE = "plugin_push"
SECRET_VERSION = "v7"


def record(
    *, profile: str = DEFAULT_PROFILE, secret_version: str | None = None, fire_at: datetime = T0
) -> DeliveryRecord:
    return DeliveryRecord(
        delivery_id=uuid4().hex,
        subscription_id="sub-1",
        owner_id="owner-1",
        event_name="order.paid",
        event_id=uuid4().hex,
        schema_version="1",
        tenant_id="tenant-1",
        data_json=b'{"ok": true}',
        emitted_at=T0,
        target_url="https://receiver.example/hook",
        resolved_ip="1.1.1.1",
        redelivery_of=None,
        attempts=0,
        fire_at=fire_at,
        created_at=T0,
        status=DeliveryStatus.PENDING,
        profile=profile,
        secret_version=secret_version,
    )


def attempt(n: int = 1, *, body: bytes = b"") -> AttemptRecord:
    return AttemptRecord(
        attempt=n,
        attempted_at=T0,
        http_code=200,
        duration_ms=5,
        error=None,
        response_snippet=body.decode(),
        response_body=body,
    )


async def case_two_claimers_never_share_a_row(store: WebhookDeliveryStoreProtocol) -> None:
    rows = [record() for _ in range(BATCH)]
    await store.enqueue_many(rows)
    first, second = await asyncio.gather(
        store.claim_batch(T0, LEASE_S, BATCH), store.claim_batch(T0, LEASE_S, BATCH)
    )
    taken = [r.delivery_id for r in first] + [r.delivery_id for r in second]
    assert len(taken) == len(set(taken)), "a row was leased twice"
    assert sorted(taken) == sorted(r.delivery_id for r in rows)


async def case_reclaim_returns_an_expired_lease(store: WebhookDeliveryStoreProtocol) -> None:
    row = record()
    await store.enqueue_many([row])
    assert [r.delivery_id for r in await store.claim_batch(T0, LEASE_S, BATCH)] == [row.delivery_id]
    assert await store.reclaim_stale(T0 + timedelta(seconds=LEASE_S + 1)) == 1
    assert [r.delivery_id for r in await store.claim_batch(T0, LEASE_S, BATCH)] == [row.delivery_id]


async def case_retry_keeps_the_id_and_waits(store: WebhookDeliveryStoreProtocol) -> None:
    row = record()
    await store.enqueue_many([row])
    await store.claim_batch(T0, LEASE_S, BATCH)
    later = T0 + timedelta(minutes=5)
    await store.schedule_retry(row.delivery_id, later, attempt(1))
    assert await store.claim_batch(T0, LEASE_S, BATCH) == []
    again = await store.claim_batch(later, LEASE_S, BATCH)
    assert [r.delivery_id for r in again] == [row.delivery_id]


async def case_dead_is_terminal(store: WebhookDeliveryStoreProtocol) -> None:
    row = record()
    await store.enqueue_many([row])
    await store.claim_batch(T0, LEASE_S, BATCH)
    await store.mark_dead(row.delivery_id, attempt(1))
    assert await store.claim_batch(T0 + timedelta(days=1), LEASE_S, BATCH) == []
    found = await store.get(row.delivery_id)
    assert found is not None and found.status is DeliveryStatus.DEAD_LETTERED


async def case_profile_and_secret_version_round_trip(store: WebhookDeliveryStoreProtocol) -> None:
    row = record(profile=OTHER_PROFILE, secret_version=SECRET_VERSION)
    await store.enqueue_many([row])
    (claimed,) = await store.claim_batch(T0, LEASE_S, BATCH)
    assert (claimed.profile, claimed.secret_version) == (OTHER_PROFILE, SECRET_VERSION)


async def case_mark_sent_changes_exactly_one_row(store: WebhookDeliveryStoreProtocol) -> None:
    sent, other = record(), record()
    await store.enqueue_many([sent, other])
    await store.claim_batch(T0, LEASE_S, BATCH)
    await store.mark_sent(sent.delivery_id, attempt(1, body=b'{"type": "complete"}'))
    after_sent, after_other = await store.get(sent.delivery_id), await store.get(other.delivery_id)
    assert after_sent is not None and after_sent.status is DeliveryStatus.SENT
    assert after_other is not None and after_other.status is DeliveryStatus.IN_FLIGHT


async def case_enqueue_is_idempotent(store: WebhookDeliveryStoreProtocol) -> None:
    row = record()
    await store.enqueue_many([row])
    await store.enqueue_many([row])
    assert len(await store.claim_batch(T0, LEASE_S, BATCH)) == 1


class StoreContract(ABC):
    @abstractmethod
    async def make_store(self) -> WebhookDeliveryStoreProtocol: ...

    async def test_two_claimers_never_share_a_row(self) -> None:
        await case_two_claimers_never_share_a_row(await self.make_store())

    async def test_reclaim_returns_an_expired_lease(self) -> None:
        await case_reclaim_returns_an_expired_lease(await self.make_store())

    async def test_retry_keeps_the_id_and_waits(self) -> None:
        await case_retry_keeps_the_id_and_waits(await self.make_store())

    async def test_dead_is_terminal(self) -> None:
        await case_dead_is_terminal(await self.make_store())

    async def test_profile_and_secret_version_round_trip(self) -> None:
        await case_profile_and_secret_version_round_trip(await self.make_store())

    async def test_mark_sent_changes_exactly_one_row(self) -> None:
        await case_mark_sent_changes_exactly_one_row(await self.make_store())

    async def test_enqueue_is_idempotent(self) -> None:
        await case_enqueue_is_idempotent(await self.make_store())
