"""Type aliases + small public dataclasses for libs/webhooks.

Aliases keep service signatures readable without repeating ``str``.
Dataclasses here are shared across the fanout handler, the Redis store
and the dispatcher — no I/O, no framework deps.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

__all__ = [
    "AttemptRecord",
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
    "SubscriptionId",
    "SubscriptionSnapshot",
]

if TYPE_CHECKING:
    from webhook_engine.config import WebhookRetryConfig
    from webhook_engine.enums import DeliveryStatus


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


@dataclass(frozen=True, slots=True)
class HttpSendResult:
    status: int
    headers: dict[str, str]
    body_snippet: str
    duration_ms: int


@dataclass(frozen=True, slots=True)
class DispatchOutcome:
    status: DeliveryStatus
    next_fire_at: datetime | None
    attempt: AttemptRecord


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
