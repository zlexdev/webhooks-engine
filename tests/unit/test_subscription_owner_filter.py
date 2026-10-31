"""W3: an event of owner A reaches only A's subscriptions; tenant_id=None keeps every owner.

Redis runs on fakeredis; Postgres is skipped without `WE_TEST_PG_URL`.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

import pytest
from fakeredis import aioredis
from sqlalchemy.ext.asyncio import create_async_engine

from webhook_engine.service import subscription_pg, subscription_store
from webhook_engine.service.subscription_pg import PgSubscriptionStore
from webhook_engine.service.subscription_store import RedisSubscriptionStore

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
    pg = PgSubscriptionStore(engine, table=f"we_subs_{uuid4().hex[:8]}")
    await pg.ensure_schema()
    yield pg
    await engine.dispose()


async def test_an_owner_event_reaches_only_that_owner(
    store: RedisSubscriptionStore | PgSubscriptionStore,
) -> None:
    a = await store.create("owner-a", "https://a.example/hook", ["order.paid"])
    b = await store.create("owner-b", "https://b.example/hook", ["order.paid"])

    only_a = await store.for_event("order.paid", "owner-a")
    every = await store.for_event("order.paid", None)

    assert [s.id for s in only_a] == [a.id]
    assert {s.id for s in every} == {a.id, b.id}
    assert await store.for_event("order.paid", "owner-c") == []
