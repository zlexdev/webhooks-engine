"""0.3 dispatcher seams: profiles, body encoders, Retry-After, the 2xx body, secret version."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from enum import StrEnum

import pytest
from _fakes import FakeSecretReader, RecordingSender, make_dispatcher, make_record

from webhook_engine import (
    DeliveryProfile,
    DeliveryStatus,
    IncompleteProfiles,
    ProfileRegistry,
    RawEncoder,
    SecretMaterial,
    UnknownProfile,
    WebhookRetryConfig,
    parse_retry_after,
)
from webhook_engine.stores.memory import MemoryWebhookDeliveryStore

NOW = datetime(2026, 10, 2, 12, tzinfo=UTC)


class Kind(StrEnum):
    POOL = "pool"
    PUSH = "push"


def test_a_kind_without_a_profile_fails_at_construction() -> None:
    with pytest.raises(IncompleteProfiles) as missing:
        ProfileRegistry.complete(Kind, {Kind.POOL: DeliveryProfile()})
    assert missing.value.missing == ("push",)  # type: ignore[attr-defined]


async def test_an_unknown_profile_on_a_row_is_refused(
    store: MemoryWebhookDeliveryStore, sender: RecordingSender
) -> None:
    with pytest.raises(UnknownProfile):
        await make_dispatcher(store=store, sender=sender).send(make_record(profile="nope"))


async def test_raw_profile_sends_the_stored_bytes(
    store: MemoryWebhookDeliveryStore, sender: RecordingSender
) -> None:
    profiles = ProfileRegistry.complete(
        Kind, {Kind.POOL: DeliveryProfile(), Kind.PUSH: DeliveryProfile(body=RawEncoder())}
    )
    record = make_record(profile=Kind.PUSH.value)
    await store.enqueue_many([record])
    await make_dispatcher(store=store, sender=sender, profiles=profiles).send(record)
    assert sender.calls[0]["body"] == record.data_json


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        pytest.param("7", 7.0, id="seconds"),
        pytest.param(
            format_datetime(NOW + timedelta(seconds=30), usegmt=True), 30.0, id="http-date"
        ),
        pytest.param("soon", None, id="garbage"),
    ],
)
def test_retry_after_forms(raw: str, expected: float | None) -> None:
    assert parse_retry_after(raw, now=NOW) == expected


async def test_429_with_retry_after_waits_exactly_that_long(
    store: MemoryWebhookDeliveryStore,
) -> None:
    sender = RecordingSender(429, headers={"Retry-After": "7"})
    record = make_record()
    await store.enqueue_many([record])
    outcome = await make_dispatcher(store=store, sender=sender, clock=lambda: NOW).send(record)
    assert (outcome.status, outcome.next_fire_at) == (
        DeliveryStatus.FAILED_RETRY,
        NOW + timedelta(seconds=7),
    )


async def test_a_2xx_body_reaches_mark_sent(store: MemoryWebhookDeliveryStore) -> None:
    answer = b'{"type": "complete"}'
    sender = RecordingSender(200, body=answer, truncated=True)
    record = make_record()
    await store.enqueue_many([record])
    outcome = await make_dispatcher(store=store, sender=sender).send(record)
    assert (outcome.attempt.response_body, outcome.attempt.response_truncated) == (answer, True)


class CountingReader(FakeSecretReader):
    def __init__(self) -> None:
        self.dropped: list[str] = []

    async def get(self, subscription_id: str) -> SecretMaterial:
        return SecretMaterial(current="s", previous=None, previous_expires_at=None)

    def invalidate(self, subscription_id: str) -> None:
        self.dropped.append(subscription_id)


async def test_a_new_secret_version_drops_the_cached_secret_once(
    store: MemoryWebhookDeliveryStore, sender: RecordingSender
) -> None:
    reader = CountingReader()
    dispatcher = make_dispatcher(store=store, sender=sender, secret_reader=reader)
    for delivery_id, version in (("a", "v1"), ("b", "v1"), ("c", "v2")):
        record = make_record(delivery_id=delivery_id, secret_version=version)
        await store.enqueue_many([record])
        await dispatcher.send(record)
    assert reader.dropped == ["sub1", "sub1"]


async def test_single_registry_keeps_the_config_retry(store: MemoryWebhookDeliveryStore) -> None:
    """`ProfileRegistry.single()` is 0.2: the dispatcher's own retry config applies."""
    sender = RecordingSender(500)
    record = make_record(attempts=WebhookRetryConfig().max_attempts - 1)
    await store.enqueue_many([record])
    outcome = await make_dispatcher(store=store, sender=sender).send(record)
    assert outcome.status is DeliveryStatus.DEAD_LETTERED
