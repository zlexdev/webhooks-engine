"""TargetResolverProtocol: re-resolve between attempts, without trusting the result."""

from __future__ import annotations

from _fakes import PUBLIC_IP, RecordingSender, make_dispatcher, make_record

from webhook_engine import DeliveryRecord, DeliveryStatus, TargetResolverProtocol
from webhook_engine.stores.memory import MemoryWebhookDeliveryStore

MOVED_IP = "93.184.216.99"
PRIVATE_IP = "10.1.2.3"


class FixedResolver(TargetResolverProtocol):
    def __init__(self, answer: str | None) -> None:
        self.answer = answer
        self.hosts: list[str] = []

    async def resolve(self, host: str) -> str | None:
        self.hosts.append(host)
        return self.answer


class BrokenResolver(TargetResolverProtocol):
    async def resolve(self, host: str) -> str | None:
        raise OSError("DNS is down")


async def _enqueued(store: MemoryWebhookDeliveryStore) -> DeliveryRecord:
    record = make_record()
    await store.enqueue_many([record])
    return record


async def test_no_resolver_keeps_the_pinned_ip(
    store: MemoryWebhookDeliveryStore, sender: RecordingSender
) -> None:
    record = await _enqueued(store)
    await make_dispatcher(store=store, sender=sender).send(record)
    assert sender.calls[0]["resolved_ip"] == PUBLIC_IP


async def test_fresh_address_is_the_one_actually_posted_to(
    store: MemoryWebhookDeliveryStore, sender: RecordingSender
) -> None:
    record = await _enqueued(store)
    resolver = FixedResolver(MOVED_IP)
    outcome = await make_dispatcher(store=store, sender=sender, resolver=resolver).send(record)

    assert resolver.hosts == ["example.com"]
    assert sender.calls[0]["resolved_ip"] == MOVED_IP
    assert outcome.status is DeliveryStatus.SENT


async def test_resolver_cannot_talk_the_dispatcher_into_a_blocked_address(
    store: MemoryWebhookDeliveryStore, sender: RecordingSender
) -> None:
    """The returned IP goes through the same check_ip — no network call happens."""
    record = await _enqueued(store)
    outcome = await make_dispatcher(
        store=store, sender=sender, resolver=FixedResolver(PRIVATE_IP)
    ).send(record)

    assert sender.calls == []
    assert outcome.status is DeliveryStatus.DEAD_LETTERED
    assert outcome.attempt.error is not None
    assert "blocked_ip_at_send" in outcome.attempt.error


async def test_unknown_answer_falls_back_to_the_pinned_ip(
    store: MemoryWebhookDeliveryStore, sender: RecordingSender
) -> None:
    record = await _enqueued(store)
    await make_dispatcher(store=store, sender=sender, resolver=FixedResolver(None)).send(record)
    assert sender.calls[0]["resolved_ip"] == PUBLIC_IP


async def test_a_dead_resolver_does_not_kill_the_attempt(
    store: MemoryWebhookDeliveryStore, sender: RecordingSender
) -> None:
    record = await _enqueued(store)
    outcome = await make_dispatcher(store=store, sender=sender, resolver=BrokenResolver()).send(
        record
    )

    assert sender.calls[0]["resolved_ip"] == PUBLIC_IP
    assert outcome.status is DeliveryStatus.SENT
