"""Redis-backed delivery store.

Layout (prefix from :class:`WebhookStorageConfig.key_prefix`, default ``wh``):

- ``wh:ready``               ZSET score=fire_at_ms, member=delivery_id
- ``wh:inflight``            ZSET score=lease_expires_ms, member=delivery_id
- ``wh:d:<id>``              HASH full record + latest attempt summary
- ``wh:d:<id>:atts``         LIST capped to ``attempts_history_limit``
- ``wh:sub:<sid>:recent``    STREAM capped to ``recent_stream_maxlen``

Atomicity for the claim / reclaim transitions is done via Lua so the
``ZREM`` (ready) and ``ZADD`` (inflight) land in the same redis
command. Enqueue uses a single ``pipeline()``; TTLs on terminal rows
are set inside :meth:`mark_sent` / :meth:`mark_dead`.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from libs.shared.logging import get_logger
from webhook_engine.config import WebhookStorageConfig
from webhook_engine.enums import DeliveryStatus
from webhook_engine.stores.base import BaseWebhookDeliveryStore
from webhook_engine.types import AttemptRecord, DeliveryRecord

__all__ = ["RedisWebhookDeliveryStore"]

if TYPE_CHECKING:
    from redis.asyncio import Redis


CLAIM_BATCH_LUA = """
-- KEYS[1]=ready KEYS[2]=inflight
-- ARGV[1]=now_ms ARGV[2]=lease_expires_ms ARGV[3]=batch_size
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'LIMIT', 0, tonumber(ARGV[3]))
for _, id in ipairs(ids) do
  redis.call('ZREM', KEYS[1], id)
  redis.call('ZADD', KEYS[2], ARGV[2], id)
end
return ids
"""

RECLAIM_LUA = """
-- KEYS[1]=ready KEYS[2]=inflight
-- ARGV[1]=now_ms
local stale = redis.call('ZRANGEBYSCORE', KEYS[2], 0, ARGV[1])
for _, id in ipairs(stale) do
  redis.call('ZREM', KEYS[2], id)
  redis.call('ZADD', KEYS[1], ARGV[1], id)
end
return #stale
"""


def _to_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * 1000)


def _from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


def _record_to_hash(record: DeliveryRecord) -> dict[str, str]:
    return {
        "delivery_id": record.delivery_id,
        "subscription_id": record.subscription_id,
        "owner_id": record.owner_id,
        "event_name": record.event_name,
        "event_id": record.event_id,
        "schema_version": record.schema_version,
        "tenant_id": record.tenant_id or "",
        "data_json": record.data_json.decode("utf-8"),
        "emitted_at_ms": str(_to_ms(record.emitted_at)),
        "target_url": record.target_url,
        "resolved_ip": record.resolved_ip,
        "redelivery_of": record.redelivery_of or "",
        "attempts": str(record.attempts),
        "fire_at_ms": str(_to_ms(record.fire_at)),
        "created_at_ms": str(_to_ms(record.created_at)),
        "status": record.status.value,
    }


def _hash_to_record(h: dict[bytes | str, bytes | str]) -> DeliveryRecord:
    def _s(k: str) -> str:
        v = h.get(k) or h.get(k.encode())
        if isinstance(v, bytes):
            return v.decode("utf-8")
        return v or ""

    return DeliveryRecord(
        delivery_id=_s("delivery_id"),
        subscription_id=_s("subscription_id"),
        owner_id=_s("owner_id"),
        event_name=_s("event_name"),
        event_id=_s("event_id"),
        schema_version=_s("schema_version"),
        tenant_id=_s("tenant_id") or None,
        data_json=_s("data_json").encode("utf-8"),
        emitted_at=_from_ms(int(_s("emitted_at_ms") or "0")),
        target_url=_s("target_url"),
        resolved_ip=_s("resolved_ip"),
        redelivery_of=_s("redelivery_of") or None,
        attempts=int(_s("attempts") or "0"),
        fire_at=_from_ms(int(_s("fire_at_ms") or "0")),
        created_at=_from_ms(int(_s("created_at_ms") or "0")),
        status=DeliveryStatus(_s("status") or DeliveryStatus.PENDING.value),
    )


def _attempt_to_json(a: AttemptRecord) -> str:
    return json.dumps(
        {
            "attempt": a.attempt,
            "attempted_at_ms": _to_ms(a.attempted_at),
            "http_code": a.http_code,
            "duration_ms": a.duration_ms,
            "error": a.error,
            "response_snippet": a.response_snippet,
        },
        separators=(",", ":"),
    )


class RedisWebhookDeliveryStore(BaseWebhookDeliveryStore):
    def __init__(self, redis: Redis, config: WebhookStorageConfig) -> None:
        self._r = redis
        self._cfg = config
        self._claim_sha: str | None = None
        self._reclaim_sha: str | None = None
        self._log = get_logger("webhooks.stores.redis")

    def _k(self, *parts: str) -> str:
        return f"{self._cfg.key_prefix}:{':'.join(parts)}"

    def _ready_key(self) -> str:
        return self._k("ready")

    def _inflight_key(self) -> str:
        return self._k("inflight")

    def _delivery_key(self, did: str) -> str:
        return self._k("d", did)

    def _attempts_key(self, did: str) -> str:
        return self._k("d", did, "atts")

    def _recent_key(self, sid: str) -> str:
        return self._k("sub", sid, "recent")

    def _idem_key(self, event_id: str, sub_id: str) -> str:
        return self._k("idem", event_id, sub_id)

    async def _ensure_scripts(self) -> None:
        if self._claim_sha is None:
            self._claim_sha = await self._r.script_load(CLAIM_BATCH_LUA)
        if self._reclaim_sha is None:
            self._reclaim_sha = await self._r.script_load(RECLAIM_LUA)

    async def enqueue_many(self, records: list[DeliveryRecord]) -> None:
        if not records:
            return
        pipe = self._r.pipeline(transaction=False)
        idem_ttl = self._cfg.idempotency_ttl_s
        for record in records:
            idem_key = self._idem_key(record.event_id, record.subscription_id)
            pipe.set(idem_key, record.delivery_id, nx=True, ex=idem_ttl)
            pipe.hset(self._delivery_key(record.delivery_id), mapping=_record_to_hash(record))  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]
            pipe.zadd(self._ready_key(), {record.delivery_id: _to_ms(record.fire_at)})
        await pipe.execute()

    async def claim_batch(
        self,
        now: datetime,
        lease_ttl_s: int,
        batch_size: int,
    ) -> list[DeliveryRecord]:
        await self._ensure_scripts()
        now_ms = _to_ms(now)
        lease_ms = now_ms + lease_ttl_s * 1000
        assert self._claim_sha is not None  # set by _ensure_scripts()
        raw: list[bytes | str] = await cast(
            "Awaitable[list[bytes | str]]",
            self._r.evalsha(
                self._claim_sha,
                2,
                self._ready_key(),
                self._inflight_key(),
                str(now_ms),
                str(lease_ms),
                str(batch_size),
            ),
        )
        ids = [r.decode("utf-8") if isinstance(r, bytes) else r for r in raw]
        if not ids:
            return []
        pipe = self._r.pipeline(transaction=False)
        for did in ids:
            pipe.hgetall(self._delivery_key(did))
        rows: list[dict[Any, Any]] = await pipe.execute()
        records: list[DeliveryRecord] = []
        for did, row in zip(ids, rows, strict=True):
            if not row:
                self._log.warn("claim_orphan_row", delivery_id=did)
                continue
            record = _hash_to_record(row)
            records.append(
                DeliveryRecord(
                    delivery_id=record.delivery_id,
                    subscription_id=record.subscription_id,
                    owner_id=record.owner_id,
                    event_name=record.event_name,
                    event_id=record.event_id,
                    schema_version=record.schema_version,
                    tenant_id=record.tenant_id,
                    data_json=record.data_json,
                    emitted_at=record.emitted_at,
                    target_url=record.target_url,
                    resolved_ip=record.resolved_ip,
                    redelivery_of=record.redelivery_of,
                    attempts=record.attempts,
                    fire_at=record.fire_at,
                    created_at=record.created_at,
                    status=DeliveryStatus.IN_FLIGHT,
                )
            )
        if records:
            pipe = self._r.pipeline(transaction=False)
            for r in records:
                pipe.hset(
                    self._delivery_key(r.delivery_id), "status", DeliveryStatus.IN_FLIGHT.value
                )
            await pipe.execute()
        return records

    async def reclaim_stale(self, now: datetime) -> int:
        await self._ensure_scripts()
        assert self._reclaim_sha is not None  # set by _ensure_scripts()
        count: int = await cast(
            "Awaitable[int]",
            self._r.evalsha(
                self._reclaim_sha,
                2,
                self._ready_key(),
                self._inflight_key(),
                str(_to_ms(now)),
            ),
        )
        return int(count)

    async def mark_sent(self, delivery_id: str, attempt: AttemptRecord) -> None:
        await self.append_attempt(delivery_id, attempt)
        pipe = self._r.pipeline(transaction=False)
        pipe.zrem(self._inflight_key(), delivery_id)
        pipe.zrem(self._ready_key(), delivery_id)
        pipe.hset(
            self._delivery_key(delivery_id),
            mapping={
                "status": DeliveryStatus.SENT.value,
                "attempts": str(attempt.attempt),
            },
        )
        pipe.expire(self._delivery_key(delivery_id), self._cfg.retention_sent_s)
        pipe.expire(self._attempts_key(delivery_id), self._cfg.retention_sent_s)
        await pipe.execute()

    async def schedule_retry(
        self,
        delivery_id: str,
        fire_at: datetime,
        attempt: AttemptRecord,
    ) -> None:
        await self.append_attempt(delivery_id, attempt)
        pipe = self._r.pipeline(transaction=False)
        pipe.zrem(self._inflight_key(), delivery_id)
        pipe.zadd(self._ready_key(), {delivery_id: _to_ms(fire_at)})
        pipe.hset(
            self._delivery_key(delivery_id),
            mapping={
                "status": DeliveryStatus.FAILED_RETRY.value,
                "attempts": str(attempt.attempt),
                "fire_at_ms": str(_to_ms(fire_at)),
            },
        )
        await pipe.execute()

    async def mark_dead(self, delivery_id: str, attempt: AttemptRecord) -> None:
        await self.append_attempt(delivery_id, attempt)
        pipe = self._r.pipeline(transaction=False)
        pipe.zrem(self._inflight_key(), delivery_id)
        pipe.zrem(self._ready_key(), delivery_id)
        pipe.hset(
            self._delivery_key(delivery_id),
            mapping={
                "status": DeliveryStatus.DEAD_LETTERED.value,
                "attempts": str(attempt.attempt),
            },
        )
        pipe.expire(self._delivery_key(delivery_id), self._cfg.retention_dead_s)
        pipe.expire(self._attempts_key(delivery_id), self._cfg.retention_dead_s)
        await pipe.execute()

    async def append_attempt(self, delivery_id: str, attempt: AttemptRecord) -> None:
        key = self._attempts_key(delivery_id)
        payload = _attempt_to_json(attempt)
        pipe = self._r.pipeline(transaction=False)
        pipe.lpush(key, payload)
        pipe.ltrim(key, 0, self._cfg.attempts_history_limit - 1)
        await pipe.execute()
        row: Any = await self._r.hget(self._delivery_key(delivery_id), "subscription_id")
        sid = row.decode("utf-8") if isinstance(row, bytes) else row
        if sid:
            summary = json.dumps(
                {
                    "delivery_id": delivery_id,
                    "attempt": attempt.attempt,
                    "http_code": attempt.http_code,
                    "duration_ms": attempt.duration_ms,
                    "error": attempt.error or "",
                    "attempted_at_ms": _to_ms(attempt.attempted_at),
                },
                separators=(",", ":"),
            )
            await self._r.xadd(
                self._recent_key(sid),
                {"summary": summary},
                maxlen=self._cfg.recent_stream_maxlen,
                approximate=True,
            )

    async def get(self, delivery_id: str) -> DeliveryRecord | None:
        row: Any = await self._r.hgetall(self._delivery_key(delivery_id))
        if not row:
            return None
        return _hash_to_record(row)

    async def recent_for_subscription(
        self,
        sub_id: str,
        limit: int,
    ) -> list[DeliveryRecord]:
        entries = await self._r.xrevrange(self._recent_key(sub_id), count=limit)
        if not entries:
            return []
        ids: list[str] = []
        for _stream_id, fields in entries:
            if fields is None:
                continue
            raw = fields.get(b"summary") or fields.get("summary")
            if raw is None:
                continue
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue
            did = payload.get("delivery_id")
            if did:
                ids.append(did)
        if not ids:
            return []
        pipe = self._r.pipeline(transaction=False)
        for did in ids:
            pipe.hgetall(self._delivery_key(did))
        rows = await pipe.execute()
        records: list[DeliveryRecord] = []
        for row in rows:
            if not row:
                continue
            records.append(_hash_to_record(row))
        return records
