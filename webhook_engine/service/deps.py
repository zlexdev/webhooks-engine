"""Dependency graph wiring for the FastAPI service.

Builds one backend profile (PG or Mongo) per process: the event store the
producer writes to, the subscription store, and the delivery queue all live in
the same database. Heavy objects are created once at startup and held on
``app.state``; :func:`get_deps` is the FastAPI dependency routes resolve.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

import httpx
from fastapi import Request

from libs.shared.time import now_utc
from webhook_engine.config import (
    WebhookDispatchConfig,
    WebhookPolicyConfig,
)
from webhook_engine.dispatcher import WebhookDispatcher
from webhook_engine.engine import WebhookEngine
from webhook_engine.host_semaphores import HostSemaphorePool
from webhook_engine.http_sender import HttpxSender
from webhook_engine.ingest.ingestor import EventIngestor
from webhook_engine.policy import DeliveryPolicy
from webhook_engine.service.settings import EventBackend, Settings
from webhook_engine.signer import HmacSigner
from webhook_engine.ssrf_guard import UrlSafetyValidator

__all__ = ["ServiceDeps", "SubscriptionAdminStore", "build_deps", "get_deps"]

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from webhook_engine.ingest.base import BaseEventStore
    from webhook_engine.protocols import (
        SecretMaterial,
        SecretReaderProtocol,
        SubscriptionReaderProtocol,
        WebhookDeliveryStoreProtocol,
    )
    from webhook_engine.service.subscription_store import Subscription
    from webhook_engine.types import SubscriptionSnapshot


class SubscriptionAdminStore(Protocol):
    """Structural type for the admin surface shared by the PG / Mongo stores."""

    async def for_event(
        self, event_name: str, tenant_id: str | None, /
    ) -> list[SubscriptionSnapshot]: ...
    async def get_secret(self, subscription_id: str) -> SecretMaterial: ...
    async def create(
        self, owner_id: str, url: str, events: list[str], secret: str | None = None
    ) -> Subscription: ...
    async def get(self, sub_id: str) -> Subscription | None: ...
    async def list_for_owner(self, owner_id: str) -> list[Subscription]: ...
    async def delete(self, sub_id: str) -> bool: ...
    async def pause(self, sub_id: str) -> bool: ...
    async def resume(self, sub_id: str) -> bool: ...


@dataclass
class _Backends:
    event_store: BaseEventStore
    sub_store: SubscriptionAdminStore
    sub_reader: SubscriptionReaderProtocol
    secret_reader: SecretReaderProtocol
    delivery_store: WebhookDeliveryStoreProtocol
    aclose: Callable[[], Awaitable[None]]


@dataclass
class ServiceDeps:
    event_store: BaseEventStore
    delivery_store: WebhookDeliveryStoreProtocol
    sub_store: SubscriptionAdminStore
    dispatch_engine: WebhookEngine
    ingestor: EventIngestor
    ssrf_guard: UrlSafetyValidator
    settings: Settings
    _aclose: Callable[[], Awaitable[None]]

    async def aclose(self) -> None:
        await self._aclose()


async def build_deps(settings: Settings) -> ServiceDeps:
    policy_cfg = WebhookPolicyConfig(
        dispatch=WebhookDispatchConfig(
            batch_size=settings.batch_size,
            worker_concurrency=settings.worker_concurrency,
            lease_ttl_s=settings.lease_ttl_s,
            drain_timeout_s=settings.drain_timeout_s,
        ),
    )
    ssrf_guard = UrlSafetyValidator(policy_cfg.security)

    if settings.backend is EventBackend.PG:
        backends = await _build_pg(settings)
    elif settings.backend is EventBackend.MONGO:
        backends = await _build_mongo(settings)
    else:
        backends = await _build_memory(settings)

    dispatcher = WebhookDispatcher(
        signer=HmacSigner(),
        http_sender=HttpxSender(httpx.AsyncClient()),
        policy=DeliveryPolicy(),
        config=policy_cfg,
        store=backends.delivery_store,
        secret_reader=backends.secret_reader,
        ssrf_guard=ssrf_guard,
        host_sem=HostSemaphorePool(policy_cfg.limits),
        clock=now_utc,
        bus=None,
    )
    dispatch_engine = WebhookEngine(
        store=backends.delivery_store,
        dispatcher=dispatcher,
        config=policy_cfg.dispatch,
        clock=now_utc,
    )
    ingestor = EventIngestor(
        event_store=backends.event_store,
        subscription_reader=backends.sub_reader,
        delivery_store=backends.delivery_store,
        ssrf_guard=ssrf_guard,
        clock=now_utc,
        lease_ttl_s=settings.lease_ttl_s,
        batch_size=settings.ingest_batch_size,
    )

    return ServiceDeps(
        event_store=backends.event_store,
        delivery_store=backends.delivery_store,
        sub_store=backends.sub_store,
        dispatch_engine=dispatch_engine,
        ingestor=ingestor,
        ssrf_guard=ssrf_guard,
        settings=settings,
        _aclose=backends.aclose,
    )


async def _build_pg(settings: Settings) -> _Backends:
    from sqlalchemy.ext.asyncio import create_async_engine

    from webhook_engine.ingest.pg import PgEventStore
    from webhook_engine.service.pg_repos import ensure_delivery_schema, make_pg_delivery_store
    from webhook_engine.service.subscription_pg import PgSubscriptionStore, make_pg_readers

    engine = create_async_engine(settings.pg_dsn)

    event_store = PgEventStore(engine, settings.pg_events_table, lock_ttl_s=settings.lease_ttl_s)
    await event_store.ensure_schema()

    sub_store = PgSubscriptionStore(engine, settings.pg_subscriptions_table)
    await sub_store.ensure_schema()
    sub_reader, secret_reader = make_pg_readers(sub_store)

    delivery_store = make_pg_delivery_store(
        engine, settings.pg_deliveries_table, settings.pg_attempts_table
    )
    await ensure_delivery_schema(engine, settings.pg_deliveries_table, settings.pg_attempts_table)

    async def _aclose() -> None:
        await engine.dispose()

    return _Backends(
        event_store=event_store,
        sub_store=sub_store,
        sub_reader=sub_reader,
        secret_reader=secret_reader,
        delivery_store=delivery_store,
        aclose=_aclose,
    )


async def _build_mongo(settings: Settings) -> _Backends:
    from motor.motor_asyncio import AsyncIOMotorClient

    from webhook_engine.ingest.mongo import MongoEventStore
    from webhook_engine.service.subscription_mongo import (
        MongoSubscriptionStore,
        make_mongo_readers,
    )
    from webhook_engine.stores.mongo import MongoWebhookDeliveryStore

    client: AsyncIOMotorClient[Any] = AsyncIOMotorClient(settings.mongo_dsn)
    db = client[settings.mongo_db]

    event_store = MongoEventStore(
        db[settings.mongo_events_collection], lock_ttl_s=settings.lease_ttl_s
    )
    await event_store.ensure_schema()

    sub_store = MongoSubscriptionStore(db[settings.mongo_subscriptions_collection])
    await sub_store.ensure_schema()
    sub_reader, secret_reader = make_mongo_readers(sub_store)

    delivery_store = MongoWebhookDeliveryStore(db[settings.mongo_deliveries_collection])
    await delivery_store.ensure_schema()

    async def _aclose() -> None:
        client.close()

    return _Backends(
        event_store=event_store,
        sub_store=sub_store,
        sub_reader=sub_reader,
        secret_reader=secret_reader,
        delivery_store=delivery_store,
        aclose=_aclose,
    )


async def _build_memory(settings: Settings) -> _Backends:
    """Dependency-free backend for dev and tests — no Postgres/Mongo/Redis needed.

    Subscriptions reuse the Redis store over an in-process fakeredis (no Lua, so
    the fake is exact); events and the delivery queue use the pure-Python memory
    stores. State is per-process and lost on restart — never for production.
    """
    import fakeredis.aioredis

    from webhook_engine.ingest.memory import MemoryEventStore
    from webhook_engine.service.subscription_store import RedisSubscriptionStore, make_readers
    from webhook_engine.stores.memory import MemoryWebhookDeliveryStore

    fake: Any = fakeredis.aioredis.FakeRedis()
    event_store = MemoryEventStore()
    sub_store = RedisSubscriptionStore(fake)
    sub_reader, secret_reader = make_readers(sub_store)
    delivery_store = MemoryWebhookDeliveryStore()

    async def _aclose() -> None:
        await fake.aclose()

    return _Backends(
        event_store=event_store,
        sub_store=sub_store,
        sub_reader=sub_reader,
        secret_reader=secret_reader,
        delivery_store=delivery_store,
        aclose=_aclose,
    )


def get_deps(request: Request) -> ServiceDeps:
    return request.app.state.deps  # type: ignore[no-any-return]
