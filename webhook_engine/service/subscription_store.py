"""Redis-backed subscription management + protocol implementations.

Key schema (prefix ``whe:``):

- ``whe:sub:{id}``              HASH  — full subscription row
- ``whe:subs:owner:{owner_id}`` SET   — sub IDs per owner
- ``whe:event:{name}:subs``     SET   — sub IDs subscribed to an event

Implements both :class:`SubscriptionReaderProtocol` and
:class:`SecretReaderProtocol` so they share the same Redis connection.
"""

from __future__ import annotations

import asyncio
import json
import secrets
import socket
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from urllib.parse import urlsplit
from uuid import uuid4

from webhook_engine.enums import SubscriptionStatus
from webhook_engine.protocols import (
    SecretMaterial,
    SecretReaderProtocol,
    SubscriptionReaderProtocol,
)
from webhook_engine.types import SubscriptionSnapshot

__all__ = [
    "RedisSubscriptionStore",
    "Subscription",
    "make_readers",
]

if TYPE_CHECKING:
    from redis.asyncio import Redis


_PREFIX = "whe"


@dataclass(frozen=True, slots=True)
class Subscription:
    id: str
    owner_id: str
    url: str
    secret: str
    events: tuple[str, ...]
    status: SubscriptionStatus
    created_at: datetime
    resolved_ip: str


class RedisSubscriptionStore:
    """CRUD for subscriptions; doubles as the reader and secret resolver."""

    def __init__(self, redis: Redis[bytes]) -> None:  # type: ignore[type-arg]
        self._r = redis

    async def for_event(
        self,
        event_name: str,
        _tenant_id: str | None,  # noqa: ARG002
    ) -> list[SubscriptionSnapshot]:
        key = f"{_PREFIX}:event:{event_name}:subs"
        sub_ids: set[bytes] = await self._r.smembers(key)  # type: ignore[assignment]  # redis-py stubs: returns Awaitable[T]|T
        if not sub_ids:
            return []

        pipe = self._r.pipeline()
        for sid in sub_ids:
            pipe.hgetall(f"{_PREFIX}:sub:{sid.decode()}")
        rows: list[dict[bytes, bytes]] = await pipe.execute()

        result: list[SubscriptionSnapshot] = []
        for row in rows:
            if not row:
                continue
            status_raw = row.get(b"status", b"").decode()
            if status_raw != SubscriptionStatus.ACTIVE.value:
                continue
            result.append(
                SubscriptionSnapshot(
                    id=row[b"id"].decode(),
                    owner_id=row[b"owner_id"].decode(),
                    url=row[b"url"].decode(),
                    resolved_ip=row[b"resolved_ip"].decode(),
                    retry_overrides=None,
                )
            )
        return result

    async def get_secret(self, subscription_id: str) -> SecretMaterial:
        secret = await self._r.hget(f"{_PREFIX}:sub:{subscription_id}", "secret")
        if not secret:
            raise KeyError(f"subscription not found: {subscription_id!r}")
        return SecretMaterial(
            current=secret.decode() if isinstance(secret, bytes) else secret,
            previous=None,
            previous_expires_at=None,
        )

    def invalidate_secret(self, _subscription_id: str) -> None:  # noqa: ARG002
        pass  # no cache to invalidate

    async def create(
        self,
        owner_id: str,
        url: str,
        events: list[str],
        secret: str | None = None,
    ) -> Subscription:
        resolved_ip = await _resolve_ip(url)
        sub_id = uuid4().hex
        sub_secret = secret or secrets.token_hex(32)
        now = datetime.now(UTC)

        row = {
            "id": sub_id,
            "owner_id": owner_id,
            "url": url,
            "secret": sub_secret,
            "events": json.dumps(events),
            "status": SubscriptionStatus.ACTIVE.value,
            "created_at": now.isoformat(),
            "resolved_ip": resolved_ip,
        }

        pipe = self._r.pipeline()
        pipe.hset(f"{_PREFIX}:sub:{sub_id}", mapping=row)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]
        pipe.sadd(f"{_PREFIX}:subs:owner:{owner_id}", sub_id)
        for event_name in events:
            pipe.sadd(f"{_PREFIX}:event:{event_name}:subs", sub_id)
        await pipe.execute()

        return Subscription(
            id=sub_id,
            owner_id=owner_id,
            url=url,
            secret=sub_secret,
            events=tuple(events),
            status=SubscriptionStatus.ACTIVE,
            created_at=now,
            resolved_ip=resolved_ip,
        )

    async def get(self, sub_id: str) -> Subscription | None:
        row: dict[bytes, bytes] = await self._r.hgetall(f"{_PREFIX}:sub:{sub_id}")  # type: ignore[assignment]  # redis-py stubs: returns Awaitable[T]|T
        if not row:
            return None
        return _row_to_sub(row)

    async def list_for_owner(self, owner_id: str) -> list[Subscription]:
        sub_ids: set[bytes] = await self._r.smembers(f"{_PREFIX}:subs:owner:{owner_id}")  # type: ignore[assignment]  # redis-py stubs: returns Awaitable[T]|T
        if not sub_ids:
            return []
        pipe = self._r.pipeline()
        for sid in sub_ids:
            pipe.hgetall(f"{_PREFIX}:sub:{sid.decode()}")
        rows: list[dict[bytes, bytes]] = await pipe.execute()
        return [_row_to_sub(r) for r in rows if r]

    async def delete(self, sub_id: str) -> bool:
        row: dict[bytes, bytes] = await self._r.hgetall(f"{_PREFIX}:sub:{sub_id}")  # type: ignore[assignment]  # redis-py stubs: returns Awaitable[T]|T
        if not row:
            return False
        owner_id = row[b"owner_id"].decode()
        events: list[str] = json.loads(row[b"events"].decode())

        pipe = self._r.pipeline()
        pipe.delete(f"{_PREFIX}:sub:{sub_id}")
        pipe.srem(f"{_PREFIX}:subs:owner:{owner_id}", sub_id)
        for event_name in events:
            pipe.srem(f"{_PREFIX}:event:{event_name}:subs", sub_id)
        await pipe.execute()
        return True

    async def pause(self, sub_id: str) -> bool:
        return await self._set_status(sub_id, SubscriptionStatus.PAUSED)

    async def resume(self, sub_id: str) -> bool:
        return await self._set_status(sub_id, SubscriptionStatus.ACTIVE)

    async def _set_status(self, sub_id: str, status: SubscriptionStatus) -> bool:
        key = f"{_PREFIX}:sub:{sub_id}"
        if not await self._r.exists(key):
            return False
        await self._r.hset(key, "status", status.value)  # redis-py stubs: returns Awaitable[T]|T
        return True


class _SubscriptionReader(SubscriptionReaderProtocol):
    def __init__(self, store: RedisSubscriptionStore) -> None:
        self._store = store

    async def for_event(self, event_name: str, tenant_id: str | None) -> list[SubscriptionSnapshot]:
        return await self._store.for_event(event_name, tenant_id)


class _SecretReader(SecretReaderProtocol):
    def __init__(self, store: RedisSubscriptionStore) -> None:
        self._store = store

    async def get(self, subscription_id: str) -> SecretMaterial:
        return await self._store.get_secret(subscription_id)

    def invalidate(self, subscription_id: str) -> None:
        self._store.invalidate_secret(subscription_id)


def make_readers(
    store: RedisSubscriptionStore,
) -> tuple[SubscriptionReaderProtocol, SecretReaderProtocol]:
    return _SubscriptionReader(store), _SecretReader(store)


async def _resolve_ip(url: str) -> str:
    host = urlsplit(url).hostname or ""
    if not host:
        raise ValueError(f"no hostname in URL: {url!r}")
    loop = asyncio.get_running_loop()
    infos = await loop.run_in_executor(
        None,
        lambda: socket.getaddrinfo(host, None, socket.AF_UNSPEC, socket.SOCK_STREAM),
    )
    if not infos:
        raise OSError(f"DNS lookup returned no results for {host!r}")
    return str(infos[0][4][0])


def _row_to_sub(row: dict[bytes, bytes]) -> Subscription:
    def _s(k: str) -> str:
        v = row.get(k.encode(), b"")
        return v.decode() if isinstance(v, bytes) else str(v)

    return Subscription(
        id=_s("id"),
        owner_id=_s("owner_id"),
        url=_s("url"),
        secret=_s("secret"),
        events=tuple(json.loads(_s("events") or "[]")),
        status=SubscriptionStatus(_s("status")),
        created_at=datetime.fromisoformat(_s("created_at")),
        resolved_ip=_s("resolved_ip"),
    )
