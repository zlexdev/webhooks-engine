"""Domain enums for webhook delivery — all ``StrEnum`` to match house style."""

from __future__ import annotations

from enum import StrEnum

__all__ = [
    "DeliveryStatus",
    "InvalidTargetReason",
    "RedactionReason",
    "RetryReason",
    "SignatureVersion",
    "SubscriptionStatus",
    "WebhookScope",
]


class WebhookScope(StrEnum):
    TENANT = "tenant"
    GLOBAL = "global"


class DeliveryStatus(StrEnum):
    PENDING = "pending"
    IN_FLIGHT = "in_flight"
    SENT = "sent"
    FAILED_RETRY = "failed_retry"
    DEAD_LETTERED = "dead_lettered"
    CANCELLED = "cancelled"


class SubscriptionStatus(StrEnum):
    ACTIVE = "active"
    PAUSED = "paused"
    DELETED = "deleted"


class RetryReason(StrEnum):
    HTTP_5XX = "http_5xx"
    HTTP_408 = "http_408"
    HTTP_429 = "http_429"
    NETWORK_ERROR = "network_error"
    TIMEOUT = "timeout"
    NON_RETRYABLE_4XX = "non_retryable_4xx"
    PAYLOAD_TOO_LARGE = "payload_too_large"
    BLOCKED_IP_AT_SEND = "blocked_ip_at_send"


class RedactionReason(StrEnum):
    EXPLICIT_META = "explicit_meta"
    AUTO_SUFFIX = "auto_suffix"


class InvalidTargetReason(StrEnum):
    BAD_SCHEME = "bad_scheme"
    NOT_HTTPS = "not_https"
    UNRESOLVABLE = "unresolvable"
    PRIVATE_IP = "private_ip"
    LOOPBACK = "loopback"
    LINK_LOCAL = "link_local"
    CGNAT = "cgnat"
    ULA = "ula"
    BLOCKED_HOST = "blocked_host"
    HAS_CREDENTIALS = "has_credentials"
    PREFLIGHT_FAILED = "preflight_failed"


class SignatureVersion(StrEnum):
    V1 = "v1"
