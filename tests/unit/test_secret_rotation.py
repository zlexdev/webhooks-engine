"""CR-D: rotate a subscription secret with a grace window; both secrets sign while it is open.

Redis runs on fakeredis; Postgres is skipped without `WE_TEST_PG_URL` (`postgresql+asyncpg://...`).
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
from fakeredis import aioredis
from sqlalchemy.ext.asyncio import create_async_engine

from webhook_engine.exceptions import SignatureVerificationError
from webhook_engine.service import subscription_pg, subscription_store
from webhook_engine.service.deps import SubscriptionAdminStore
from webhook_engine.service.subscription_pg import PgSubscriptionStore
from webhook_engine.service.subscription_store import RedisSubscriptionStore
from webhook_engine.signer import HmacSigner
from webhook_engine.verify import WebhookVerificationError, verify_webhook

_PG_URL = os.environ.get("WE_TEST_PG_URL")


async def _no_dns(url: str) -> str:
    return "93.184.216.34"


@pytest.fixture(params=["redis", "pg"])
async def store(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[Any]:
    monkeypatch.setattr(subscription_store, "_resolve_ip", _no_dns)
    monkeypatch.setattr(subscription_pg, "_resolve_ip", _no_dns)
    if request.param == "redis":
        yield RedisSubscriptionStore(aioredis.FakeRedis())
        return
    if not _PG_URL:
        pytest.skip("WE_TEST_PG_URL not set")
    engine = create_async_engine(_PG_URL)
    pg = PgSubscriptionStore(engine, table=f"we_rot_{uuid4().hex[:8]}")
    await pg.ensure_schema()
    yield pg
    await engine.dispose()


async def test_the_old_secret_signs_only_inside_the_grace_window(
    store: SubscriptionAdminStore,
) -> None:
    sub = await store.create("t1", "https://hooks.example.com/x", ["order.paid"], secret="old")
    rotated = await store.rotate_secret(sub.id, grace_s=3600, secret="new")
    now = datetime.now(UTC)
    assert (rotated.current, rotated.previous_at(now)) == ("new", "old")
    read = await store.get_secret(sub.id)
    assert (read.current, read.previous_at(now)) == ("new", "old")

    closed = await store.rotate_secret(sub.id, grace_s=0, secret="newer")
    assert closed.current == "newer"
    assert (await store.get_secret(sub.id)).previous_at(datetime.now(UTC)) is None


async def test_rotating_an_unknown_subscription_is_a_key_error(
    store: SubscriptionAdminStore,
) -> None:
    with pytest.raises(KeyError):
        await store.rotate_secret("missing", grace_s=60)


def test_a_consumer_verifies_with_either_secret_of_a_dual_signature() -> None:
    signer, now = HmacSigner(), datetime.now(UTC)

    def headers(secret: str) -> dict[str, str]:
        return signer.sign(
            delivery_id="d1", event_name="e", event_id="ev1", body=b"{}", secret=secret, now=now
        ).as_dict()

    both = headers("new")
    both["Webhook-Signature"] += " " + headers("old")["Webhook-Signature"]
    for secret in ("new", "old"):
        signer.verify(body=b"{}", headers=both, secret=secret, now=now, window_s=300)
    with pytest.raises(SignatureVerificationError):
        signer.verify(body=b"{}", headers=both, secret="stranger", now=now, window_s=300)
    for secret in ("new", "old"):
        verify_webhook(body=b"{}", headers=both, secret=secret, now_ts=int(now.timestamp()))
    with pytest.raises(WebhookVerificationError):
        verify_webhook(body=b"{}", headers=both, secret="stranger", now_ts=int(now.timestamp()))
