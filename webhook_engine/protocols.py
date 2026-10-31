"""Protocol contracts — services don't couple to redis / httpx.

Concrete implementations live elsewhere:
- :class:`WebhookDeliveryStoreProtocol` → ``webhook_engine/stores/``
- :class:`HttpSenderProtocol` → ``webhook_engine/http_sender.py``
- :class:`SubscriptionReaderProtocol` → ``webhook_engine/service/subscription_store.py``
- :class:`SignerProtocol` → ``webhook_engine/signer.py`` (the shipped default)
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
    SignedHeaders,
    SubscriptionSnapshot,
)

__all__ = [
    "Clock",
    "HttpSenderProtocol",
    "SecretMaterial",
    "SecretReaderProtocol",
    "SignerProtocol",
    "SubscriptionReaderProtocol",
    "TargetResolverProtocol",
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


class SecretCipher(Protocol):
    """How a subscription secret is kept at rest: ``seal`` before a write, ``open`` after a read.

    The default :class:`IdentityCipher` stores it as given (today's rows open unchanged). A real
    cipher (``corekit.seal.Sealer`` fits as is: str in, str out, rotation by a key list) needs
    every existing row resealed before it is switched on.
    """

    def seal(self, raw: str) -> str: ...
    def open(self, sealed: str) -> str: ...


class IdentityCipher:
    """The default :class:`SecretCipher`: the secret is stored as given."""

    def seal(self, raw: str) -> str:
        return raw

    def open(self, sealed: str) -> str:
        return sealed


class SignerProtocol(ABC):
    """How a delivery is signed — and how a receiver checks that signature.

    The shipped :class:`~webhook_engine.signer.HmacSigner` produces
    ``v1=<hmac>`` over ``"<ts>.<event_id>.<sha256(body)>"`` under ``Webhook-*``
    headers. That is a *default*, not the contract: a host migrating off its
    own delivery code substitutes its existing scheme here — the GitHub /
    Stripe style ``sha256=<hex>`` over the raw body, a legacy format, a future
    ``v2`` — without touching the dispatcher.

    Both directions live on one protocol on purpose. A verifier that does not
    match its signer is the failure this seam exists to prevent, and two
    separate protocols would let them drift apart silently.
    """

    @abstractmethod
    def sign(
        self,
        *,
        delivery_id: str,
        event_name: str,
        event_id: str,
        body: bytes,
        secret: str,
        now: datetime,
    ) -> SignedHeaders: ...

    @abstractmethod
    def verify(
        self,
        *,
        body: bytes,
        headers: dict[str, str],
        secret: str,
        now: datetime,
        window_s: int,
    ) -> None:
        """Raise :class:`~webhook_engine.exceptions.SignatureVerificationError`
        on any mismatch; return ``None`` on success.

        ``headers`` arrive as the receiving framework produced them, so match
        header names case-insensitively — most of them lower-case everything.
        """


class TargetResolverProtocol(ABC):
    """Optional per-attempt DNS re-resolution.

    Without one, ``DeliveryRecord.resolved_ip`` is pinned at enqueue and used
    for every attempt — the strongest defence against DNS rebinding, and the
    default. The cost shows up on long retry ladders: with ``max_attempts=8``
    and ``max_delay_s=3600`` the last attempt fires hours later, so a receiver
    that moved (failover, new balancer, DNS change) is unreachable and the
    journal records ``network_error``, indistinguishable from a dead server.

    Supplying a resolver trades some of that rebinding protection for
    reachability. The address it returns is *not* trusted: it goes through the
    same ``check_ip`` as the pinned one before anything is sent.
    """

    @abstractmethod
    async def resolve(self, host: str) -> str | None:
        """Return the current address for ``host``, or ``None`` for "unknown".

        ``None`` means the dispatcher keeps the pinned IP. Returning a fallback
        here instead would hide "I could not resolve" behind a real answer.
        """


class SecretReaderProtocol(ABC):
    @abstractmethod
    async def get(self, subscription_id: str) -> SecretMaterial: ...

    @abstractmethod
    def invalidate(self, subscription_id: str) -> None: ...

    async def get_versioned(self, subscription_id: str, version: str | None) -> SecretMaterial:
        """`get` that drops the cached entry when the row names a newer secret version.

        Default for every reader: rotation is seen on the next delivery, not after the TTL.
        """
        seen: dict[str, str] = self.__dict__.setdefault("_secret_versions", {})
        if version is not None and seen.get(subscription_id) != version:
            self.invalidate(subscription_id)
            seen[subscription_id] = version
        return await self.get(subscription_id)
