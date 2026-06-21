"""Protocol contracts — services don't couple to redis / httpx.

Concrete implementations live elsewhere:
- :class:`WebhookDeliveryStoreProtocol` → ``libs/webhooks/stores/redis.py``
- :class:`HttpSenderProtocol` → ``api/services/webhooks/http_sender.py``
- :class:`SubscriptionReaderProtocol` → ``api/services/webhooks/subscription_reader.py``
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from webhook_engine.types import (
    AttemptRecord,
    DeliveryId,
    DeliveryRecord,
    HttpSendResult,
    SubscriptionSnapshot,
)

__all__ = [
    "Clock",
    "HttpSenderProtocol",
    "SecretMaterial",
    "SecretReaderProtocol",
    "SubscriptionReaderProtocol",
    "WebhookDeliveryStoreProtocol",
]

Clock = Callable[[], datetime]


@dataclass(frozen=True, slots=True)
class SecretMaterial:
    """Plaintext secret material resolved at dispatch time.

    ``previous`` + ``previous_expires_at`` support graceful rotation:
    consumers that are still verifying with the old secret keep
    working until the grace window elapses, at which point rotation
    completes.
    """

    current: str
    previous: str | None
    previous_expires_at: datetime | None


class WebhookDeliveryStoreProtocol(ABC):
    @abstractmethod
    async def enqueue_many(self, records: list[DeliveryRecord]) -> None: ...

    @abstractmethod
    async def claim_batch(
        self, now: datetime, lease_ttl_s: int, batch_size: int
    ) -> list[DeliveryRecord]: ...

    @abstractmethod
    async def reclaim_stale(self, now: datetime) -> int: ...

    @abstractmethod
    async def mark_sent(self, delivery_id: DeliveryId, attempt: AttemptRecord) -> None: ...

    @abstractmethod
    async def schedule_retry(
        self,
        delivery_id: DeliveryId,
        fire_at: datetime,
        attempt: AttemptRecord,
    ) -> None: ...

    @abstractmethod
    async def mark_dead(self, delivery_id: DeliveryId, attempt: AttemptRecord) -> None: ...

    @abstractmethod
    async def append_attempt(self, delivery_id: DeliveryId, attempt: AttemptRecord) -> None: ...

    @abstractmethod
    async def get(self, delivery_id: DeliveryId) -> DeliveryRecord | None: ...

    @abstractmethod
    async def recent_for_subscription(self, sub_id: str, limit: int) -> list[DeliveryRecord]: ...


class HttpSenderProtocol(Protocol):
    async def post(
        self,
        *,
        url: str,
        body: bytes,
        headers: dict[str, str],
        resolved_ip: str | None,
        connect_timeout_s: float,
        read_timeout_s: float,
        total_timeout_s: float,
        max_response_bytes: int,
    ) -> HttpSendResult: ...


class SubscriptionReaderProtocol(ABC):
    @abstractmethod
    async def for_event(
        self,
        event_name: str,
        tenant_id: str | None,
    ) -> list[SubscriptionSnapshot]: ...


class SecretReaderProtocol(ABC):
    @abstractmethod
    async def get(self, subscription_id: str) -> SecretMaterial: ...

    @abstractmethod
    def invalidate(self, subscription_id: str) -> None: ...
