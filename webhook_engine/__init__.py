"""Durable, signed, per-tenant webhook delivery.

Public API: opt-in marker + registry + fan-out + helpers. Concrete
I/O (Redis queue, HTTP sender, Mongo reader) lives under
``libs/webhooks/stores/`` and ``api/services/webhooks/``.
"""

from webhook_engine.config import (
    DEFAULT_WEBHOOK_POLICY,
    WebhookDispatchConfig,
    WebhookLimitsConfig,
    WebhookPolicyConfig,
    WebhookRetryConfig,
    WebhookSecurityConfig,
    WebhookStorageConfig,
)
from webhook_engine.dispatcher import WebhookDispatcher
from webhook_engine.engine import EngineTickStats, WebhookEngine
from webhook_engine.enums import (
    DeliveryStatus,
    InvalidTargetReason,
    RedactionReason,
    RetryReason,
    SignatureVersion,
    SubscriptionStatus,
    WebhookScope,
)
from webhook_engine.envelope import (
    REDACTED_PLACEHOLDER,
    WebhookEnvelope,
    redact_payload,
    sha256_hex,
    should_auto_redact,
)
from webhook_engine.exceptions import (
    DeliveryInFlight,
    DeliveryNotFound,
    EventLimitExceeded,
    InvalidWebhookTarget,
    SignatureVerificationError,
    SubscriptionLimitExceeded,
    SubscriptionNotFound,
    UnknownEventName,
    WebhookEventCollision,
    WebhookMetaMissing,
    WebhookPermissionDenied,
    WebhookStartupError,
)
from webhook_engine.host_semaphores import HostSemaphorePool
from webhook_engine.http_sender import HttpxSender
from webhook_engine.ingest import (
    BaseEventStore,
    EventIngestor,
    EventIngestStatus,
    IncomingEvent,
    IngestTickStats,
)
from webhook_engine.policy import DeliveryPolicy
from webhook_engine.protocols import (
    Clock,
    HttpSenderProtocol,
    SecretMaterial,
    SecretReaderProtocol,
    SubscriptionReaderProtocol,
    WebhookDeliveryStoreProtocol,
)
from webhook_engine.signer import HmacSigner, SignatureHeaders
from webhook_engine.ssrf_guard import BlockedCidrSet, UrlSafetyValidator
from webhook_engine.stores.base import BaseWebhookDeliveryStore
from webhook_engine.stores.redis import RedisWebhookDeliveryStore
from webhook_engine.types import (
    AttemptRecord,
    DeliveryRecord,
    DispatchOutcome,
    HttpSendResult,
    ResolvedTarget,
    SubscriptionSnapshot,
)
from webhook_engine.verify import WebhookVerificationError, verify_webhook

__all__ = [
    "AttemptRecord",
    "BaseEventStore",
    "BaseWebhookDeliveryStore",
    "BlockedCidrSet",
    "Clock",
    "EventIngestStatus",
    "EventIngestor",
    "IncomingEvent",
    "IngestTickStats",
    "WebhookVerificationError",
    "verify_webhook",
    "DEFAULT_WEBHOOK_POLICY",
    "DeliveryInFlight",
    "DeliveryNotFound",
    "DeliveryPolicy",
    "DeliveryRecord",
    "DeliveryStatus",
    "DispatchOutcome",
    "EngineTickStats",
    "EventLimitExceeded",
    "HmacSigner",
    "HostSemaphorePool",
    "HttpSendResult",
    "HttpSenderProtocol",
    "HttpxSender",
    "InvalidTargetReason",
    "InvalidWebhookTarget",
    "REDACTED_PLACEHOLDER",
    "RedactionReason",
    "RedisWebhookDeliveryStore",
    "ResolvedTarget",
    "RetryReason",
    "SecretMaterial",
    "SecretReaderProtocol",
    "SignatureHeaders",
    "SignatureVerificationError",
    "SignatureVersion",
    "SubscriptionLimitExceeded",
    "SubscriptionNotFound",
    "SubscriptionReaderProtocol",
    "SubscriptionSnapshot",
    "SubscriptionStatus",
    "UnknownEventName",
    "UrlSafetyValidator",
    "WebhookDeliveryStoreProtocol",
    "WebhookDispatchConfig",
    "WebhookDispatcher",
    "WebhookEngine",
    "WebhookEnvelope",
    "WebhookEventCollision",
    "WebhookLimitsConfig",
    "WebhookMetaMissing",
    "WebhookPermissionDenied",
    "WebhookPolicyConfig",
    "WebhookRetryConfig",
    "WebhookScope",
    "WebhookSecurityConfig",
    "WebhookStartupError",
    "WebhookStorageConfig",
    "redact_payload",
    "sha256_hex",
    "should_auto_redact",
]

# Optional event-bus fan-out surface — requires the `bus` extra (asyncbus). The
# core delivery service does not import it, so the package loads fine without
# asyncbus and simply omits these names.
try:
    from webhook_engine.fanout import WebhookFanoutHandler as WebhookFanoutHandler
    from webhook_engine.fanout_installer import (
        WebhookFanoutInstaller as WebhookFanoutInstaller,
    )
    from webhook_engine.meta import WebhookMeta as WebhookMeta
    from webhook_engine.meta import webhook_event as webhook_event
    from webhook_engine.registry import WebhookCatalog as WebhookCatalog
    from webhook_engine.registry import WebhookEventEntry as WebhookEventEntry
    from webhook_engine.registry import WebhookEventRegistry as WebhookEventRegistry
except ImportError:
    pass
else:
    __all__ += [
        "WebhookCatalog",
        "WebhookEventEntry",
        "WebhookEventRegistry",
        "WebhookFanoutHandler",
        "WebhookFanoutInstaller",
        "WebhookMeta",
        "webhook_event",
    ]
