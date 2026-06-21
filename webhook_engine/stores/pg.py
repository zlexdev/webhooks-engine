"""Postgres-backed webhook stores — concrete side of the webhook persistence protocols.

Libs code stays app-agnostic: sessions and repos are duck-typed via
``Any`` + repo-ctor callables. Each store method runs in its **own**
short transaction; the store owns the session, callers never share one.
The repo protocols below are the contract the app layer must satisfy
(see ``app/db/repos/webhook.py``).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from contextlib import AbstractAsyncContextManager
from datetime import datetime
from typing import Any

from webhook_engine.protocols import (
    SubscriptionReaderProtocol,
    WebhookDeliveryStoreProtocol,
)
from webhook_engine.types import (
    AttemptRecord,
    DeliveryId,
    DeliveryRecord,
    SubscriptionSnapshot,
)

__all__ = [
    "PgSubscriptionReader",
    "PgWebhookDeliveryStore",
    "SessionFactory",
    "WebhookAttemptRepoProtocol",
    "WebhookDeliveryRepoProtocol",
    "WebhookSubscriptionRepoProtocol",
]

SessionFactory = Callable[[], AbstractAsyncContextManager[Any]]


class WebhookDeliveryRepoProtocol(ABC):
    @abstractmethod
    async def enqueue_many(self, records: Sequence[DeliveryRecord]) -> int: ...

    @abstractmethod
    async def claim_batch(
        self,
        *,
        now: datetime,
        lease_ttl_s: int,
        batch_size: int,
    ) -> Sequence[DeliveryRecord]: ...

    @abstractmethod
    async def reclaim_stale(self, *, now: datetime) -> int: ...

    @abstractmethod
    async def mark_sent(self, delivery_id: str, attempt: AttemptRecord) -> None: ...

    @abstractmethod
    async def schedule_retry(
        self,
        delivery_id: str,
        fire_at: datetime,
        attempt: AttemptRecord,
    ) -> None: ...

    @abstractmethod
    async def mark_dead(self, delivery_id: str, attempt: AttemptRecord) -> None: ...

    @abstractmethod
    async def bump_attempt(self, delivery_id: str, attempt: AttemptRecord) -> None: ...

    @abstractmethod
    async def get_record(self, delivery_id: str) -> DeliveryRecord | None: ...

    @abstractmethod
    async def recent_for_subscription(
        self,
        sub_id: str,
        limit: int,
    ) -> Sequence[DeliveryRecord]: ...


class WebhookAttemptRepoProtocol(ABC):
    @abstractmethod
    async def append(self, delivery_id: str, attempt: AttemptRecord) -> None: ...


class WebhookSubscriptionRepoProtocol(ABC):
    @abstractmethod
    async def for_event(
        self,
        event_name: str,
        tenant_id: str | None,
    ) -> Sequence[SubscriptionSnapshot]: ...


class PgWebhookDeliveryStore(WebhookDeliveryStoreProtocol):
    def __init__(
        self,
        *,
        session_factory: SessionFactory,
        delivery_repo_ctor: Callable[[Any], WebhookDeliveryRepoProtocol],
        attempt_repo_ctor: Callable[[Any], WebhookAttemptRepoProtocol],
    ) -> None:
        self._session_factory = session_factory
        self._delivery_repo_ctor = delivery_repo_ctor
        self._attempt_repo_ctor = attempt_repo_ctor

    async def enqueue_many(self, records: list[DeliveryRecord]) -> None:
        if not records:
            return
        async with self._session_factory() as s:
            repo = self._delivery_repo_ctor(s)
            await repo.enqueue_many(records)
            await s.commit()

    async def claim_batch(
        self,
        now: datetime,
        lease_ttl_s: int,
        batch_size: int,
    ) -> list[DeliveryRecord]:
        async with self._session_factory() as s:
            repo = self._delivery_repo_ctor(s)
            rows = await repo.claim_batch(
                now=now,
                lease_ttl_s=lease_ttl_s,
                batch_size=batch_size,
            )
            await s.commit()
            return list(rows)

    async def reclaim_stale(self, now: datetime) -> int:
        async with self._session_factory() as s:
            repo = self._delivery_repo_ctor(s)
            count = await repo.reclaim_stale(now=now)
            await s.commit()
            return count

    async def mark_sent(
        self,
        delivery_id: DeliveryId,
        attempt: AttemptRecord,
    ) -> None:
        async with self._session_factory() as s:
            delivery_repo = self._delivery_repo_ctor(s)
            attempt_repo = self._attempt_repo_ctor(s)
            await attempt_repo.append(delivery_id, attempt)
            await delivery_repo.mark_sent(delivery_id, attempt)
            await s.commit()

    async def schedule_retry(
        self,
        delivery_id: DeliveryId,
        fire_at: datetime,
        attempt: AttemptRecord,
    ) -> None:
        async with self._session_factory() as s:
            delivery_repo = self._delivery_repo_ctor(s)
            attempt_repo = self._attempt_repo_ctor(s)
            await attempt_repo.append(delivery_id, attempt)
            await delivery_repo.schedule_retry(delivery_id, fire_at, attempt)
            await s.commit()

    async def mark_dead(
        self,
        delivery_id: DeliveryId,
        attempt: AttemptRecord,
    ) -> None:
        async with self._session_factory() as s:
            delivery_repo = self._delivery_repo_ctor(s)
            attempt_repo = self._attempt_repo_ctor(s)
            await attempt_repo.append(delivery_id, attempt)
            await delivery_repo.mark_dead(delivery_id, attempt)
            await s.commit()

    async def append_attempt(
        self,
        delivery_id: DeliveryId,
        attempt: AttemptRecord,
    ) -> None:
        async with self._session_factory() as s:
            delivery_repo = self._delivery_repo_ctor(s)
            attempt_repo = self._attempt_repo_ctor(s)
            await attempt_repo.append(delivery_id, attempt)
            await delivery_repo.bump_attempt(delivery_id, attempt)
            await s.commit()

    async def get(self, delivery_id: DeliveryId) -> DeliveryRecord | None:
        async with self._session_factory() as s:
            repo = self._delivery_repo_ctor(s)
            return await repo.get_record(delivery_id)

    async def recent_for_subscription(
        self,
        sub_id: str,
        limit: int,
    ) -> list[DeliveryRecord]:
        async with self._session_factory() as s:
            repo = self._delivery_repo_ctor(s)
            rows = await repo.recent_for_subscription(sub_id, limit)
            return list(rows)


class PgSubscriptionReader(SubscriptionReaderProtocol):
    def __init__(
        self,
        *,
        session_factory: SessionFactory,
        sub_repo_ctor: Callable[[Any], WebhookSubscriptionRepoProtocol],
    ) -> None:
        self._session_factory = session_factory
        self._sub_repo_ctor = sub_repo_ctor

    async def for_event(
        self,
        event_name: str,
        tenant_id: str | None,
    ) -> list[SubscriptionSnapshot]:
        async with self._session_factory() as s:
            repo = self._sub_repo_ctor(s)
            rows = await repo.for_event(event_name, tenant_id)
            return list(rows)
