"""Type aliases + small public dataclasses for webhook_engine.

Aliases keep service signatures readable without repeating ``str``.
Dataclasses here are shared across the fanout handler, the Redis store
and the dispatcher — no I/O, no framework deps.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

__all__ = [
    "AttemptRecord",
    "DEFAULT_PROFILE",
    "CidrString",
    "DeliveryId",
    "DeliveryRecord",
    "DispatchOutcome",
    "EnvelopeBody",
    "EventName",
    "HostName",
    "HttpSendResult",
    "OwnerId",
    "ResolvedTarget",
    "SecretToken",
    "SignatureHeaders",
    "SignedHeaders",
    "SubscriptionId",
    "SubscriptionSnapshot",
]

if TYPE_CHECKING:
    from webhook_engine.config import WebhookRetryConfig
    from webhook_engine.enums import DeliveryStatus


DEFAULT_PROFILE = "default"
"""Profile name of a row written without one — `ProfileRegistry.single()` serves it."""

DeliveryId = str
SubscriptionId = str
OwnerId = str
HostName = str
EventName = str
SecretToken = str
CidrString = str
EnvelopeBody = dict[str, Any]


@dataclass(frozen=True, slots=True)
class AttemptRecord:
    attempt: int
    attempted_at: datetime
    http_code: int | None
    duration_ms: int
    error: str | None
    response_snippet: str
    response_body: bytes = b""
    """The 2xx answer up to the profile's `max_response_bytes`: a host applies an answer
    carried in the body (a push the receiver completes in place)."""
    response_truncated: bool = False


@dataclass(frozen=True, slots=True)
class DeliveryRecord:
    """Persisted queue row — everything needed to sign + POST one delivery.

    Secret is NOT snapshotted here. The dispatcher reads the current
    secret through :class:`SecretCache` so rotation propagates within
    ``security.secret_cache_ttl_s``.
    """

    delivery_id: DeliveryId
    subscription_id: SubscriptionId
    owner_id: OwnerId
    event_name: EventName
    event_id: str
    schema_version: str
    tenant_id: str | None
    data_json: bytes
    emitted_at: datetime
    target_url: str
    resolved_ip: str
    redelivery_of: str | None
    attempts: int
    fire_at: datetime
    created_at: datetime
    status: DeliveryStatus
    profile: str = DEFAULT_PROFILE
    """Name of the `DeliveryProfile` this row is sent with (`ProfileRegistry.of`)."""
    secret_version: str | None = None
    """Changes when the owner's secret rotates: a new value bypasses the reader's cache
    at once. `None` — the reader's TTL alone (0.2 behaviour)."""


@dataclass(frozen=True, slots=True)
class HttpSendResult:
    status: int
    headers: dict[str, str]
    body_snippet: str
    duration_ms: int
    body: bytes = b""
    body_truncated: bool = False


@dataclass(frozen=True, slots=True)
class DispatchOutcome:
    status: DeliveryStatus
    next_fire_at: datetime | None
    attempt: AttemptRecord


@runtime_checkable
class SignedHeaders(Protocol):
    """What a signer hands back: the outbound headers for one delivery.

    Structural on purpose — ``as_dict`` is the only thing the dispatcher ever
    calls, so a custom :class:`~webhook_engine.protocols.SignerProtocol`
    returns its *own* type carrying its own header names
    (``X-Hub-Signature-256``, ``Stripe-Signature``, …) and inherits nothing
    from this package.
    """

    def as_dict(self) -> dict[str, str]: ...


@dataclass(frozen=True, slots=True)
class SignatureHeaders:
    """The shipped ``Webhook-*`` header set — what :class:`HmacSigner` returns."""

    webhook_id: str
    webhook_event: str
    webhook_event_id: str
    webhook_timestamp: str
    webhook_signature: str

    def as_dict(self) -> dict[str, str]:
        return {
            "Webhook-Id": self.webhook_id,
            "Webhook-Event": self.webhook_event,
            # event_id is part of the signed message — the receiver needs it
            # to reconstruct the MAC, so it must travel in a header.
            "Webhook-Event-Id": self.webhook_event_id,
            "Webhook-Timestamp": self.webhook_timestamp,
            "Webhook-Signature": self.webhook_signature,
            "Content-Type": "application/json",
        }


@dataclass(frozen=True, slots=True)
class ResolvedTarget:
    host: HostName
    port: int
    scheme: str
    ips: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SubscriptionSnapshot:
    """Tenant-scoped subscription view used by the fanout handler.

    Secret is resolved at send-time by the dispatcher (not snapshotted
    here) so rotations propagate without a second enqueue pass.
    """

    id: SubscriptionId
    owner_id: OwnerId
    url: str
    resolved_ip: str
    retry_overrides: WebhookRetryConfig | None
