"""WebhookEngine(on_outcome=...): the host sees the receiver's answer."""

from __future__ import annotations

import asyncio
from dataclasses import replace

from _fakes import RecordingSender, make_dispatcher, make_record, now_utc

from webhook_engine import (
    DeliveryRecord,
    DeliveryStatus,
    DispatchOutcome,
    WebhookEngine,
    WebhookPolicyConfig,
)
from webhook_engine.stores.memory import MemoryWebhookDeliveryStore


def _engine(
    store: MemoryWebhookDeliveryStore,
    sender: RecordingSender,
    on_outcome: object | None = None,
    *,
    timeout_s: float = 5.0,
) -> WebhookEngine:
    cfg = WebhookPolicyConfig()
    dispatch_cfg = replace(cfg.dispatch, on_outcome_timeout_s=timeout_s)
    return WebhookEngine(
        store=store,
        dispatcher=make_dispatcher(store=store, sender=sender, config=cfg),
        config=dispatch_cfg,
        clock=now_utc,
        on_outcome=on_outcome,  # type: ignore[arg-type]
    )


async def _tick_one(engine: WebhookEngine, store: MemoryWebhookDeliveryStore) -> None:
    await store.enqueue_many([make_record()])
    await engine.tick()


async def test_callback_receives_the_receiver_response_body(
    store: MemoryWebhookDeliveryStore, sender: RecordingSender
) -> None:
    seen: list[tuple[DeliveryRecord, DispatchOutcome]] = []

    async def on_outcome(record: DeliveryRecord, outcome: DispatchOutcome) -> None:
        seen.append((record, outcome))

    await _tick_one(_engine(store, sender, on_outcome), store)

    assert len(seen) == 1
    record, outcome = seen[0]
    assert record.delivery_id == "d1"
    assert outcome.status is DeliveryStatus.SENT
    # The whole point of the seam: the receiver's synchronous answer.
    assert outcome.attempt.response_snippet == '{"verdict":"spam"}'


async def test_no_callback_is_the_default_and_changes_nothing(
    store: MemoryWebhookDeliveryStore, sender: RecordingSender
) -> None:
    await store.enqueue_many([make_record()])
    stats = await _engine(store, sender).tick()
    assert (stats.claimed, stats.sent, stats.errors) == (1, 1, 0)


async def test_a_hanging_callback_is_cut_off_and_the_delivery_still_counts(
    store: MemoryWebhookDeliveryStore, sender: RecordingSender
) -> None:
    async def hangs(record: DeliveryRecord, outcome: DispatchOutcome) -> None:
        await asyncio.sleep(30)

    await store.enqueue_many([make_record()])
    stats = await asyncio.wait_for(_engine(store, sender, hangs, timeout_s=0.05).tick(), timeout=5)
    assert (stats.claimed, stats.sent, stats.errors) == (1, 1, 0)


async def test_a_throwing_callback_does_not_affect_the_delivery(
    store: MemoryWebhookDeliveryStore, sender: RecordingSender
) -> None:
    async def explodes(record: DeliveryRecord, outcome: DispatchOutcome) -> None:
        raise RuntimeError("host bug")

    await store.enqueue_many([make_record()])
    stats = await _engine(store, sender, explodes).tick()

    assert (stats.claimed, stats.sent, stats.errors) == (1, 1, 0)
    stored = await store.get("d1")
    assert stored is not None
    assert stored.status is DeliveryStatus.SENT


async def test_callback_runs_outside_the_concurrency_gate(
    store: MemoryWebhookDeliveryStore, sender: RecordingSender
) -> None:
    """A slow callback must not hold a worker slot: with worker_concurrency=1
    and two records, the second delivery starts before the first callback ends."""
    order: list[str] = []
    release = asyncio.Event()

    async def slow(record: DeliveryRecord, outcome: DispatchOutcome) -> None:
        order.append(f"cb-start:{record.delivery_id}")
        await release.wait()
        order.append(f"cb-end:{record.delivery_id}")

    cfg = WebhookPolicyConfig()
    dispatch_cfg = replace(cfg.dispatch, worker_concurrency=1, on_outcome_timeout_s=5.0)
    engine = WebhookEngine(
        store=store,
        dispatcher=make_dispatcher(store=store, sender=sender, config=cfg),
        config=dispatch_cfg,
        clock=now_utc,
        on_outcome=slow,
    )

    await store.enqueue_many([make_record(delivery_id="d1"), make_record(delivery_id="d2")])
    task = asyncio.create_task(engine.tick())
    await asyncio.sleep(0.05)

    # Both callbacks are in flight while the single delivery slot is free.
    assert sorted(order) == ["cb-start:d1", "cb-start:d2"]
    assert len(sender.calls) == 2
    release.set()
    stats = await task
    assert stats.sent == 2
