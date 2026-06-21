"""Immutable policy configs for webhook delivery.

Every knob lives here so the rest of the package has zero hardcoded
constants. Configs are grouped by concern (retry / security / limits /
dispatch / storage) and aggregated under :class:`WebhookPolicyConfig`.
"""

from __future__ import annotations

from dataclasses import dataclass, field

__all__ = [
    "DEFAULT_WEBHOOK_POLICY",
    "WebhookDispatchConfig",
    "WebhookLimitsConfig",
    "WebhookPolicyConfig",
    "WebhookRetryConfig",
    "WebhookSecurityConfig",
    "WebhookStorageConfig",
]


@dataclass(frozen=True, slots=True)
class WebhookRetryConfig:
    max_attempts: int = 8
    base_delay_s: float = 5.0
    backoff_factor: float = 2.0
    max_delay_s: float = 3600.0
    jitter_pct: float = 0.2


@dataclass(frozen=True, slots=True)
class WebhookSecurityConfig:
    require_https_in_prod: bool = True
    allow_http_in_dev: bool = True
    blocked_cidrs: tuple[str, ...] = (
        "127.0.0.0/8",
        "10.0.0.0/8",
        "172.16.0.0/12",
        "192.168.0.0/16",
        "169.254.0.0/16",
        "100.64.0.0/10",
        "::1/128",
        "fc00::/7",
        "fe80::/10",
        "::ffff:0:0/96",
    )
    # Opt-in host allow-list. None = only deny-list applies.
    allowed_hosts: tuple[str, ...] | None = None
    preflight_enabled: bool = True
    preflight_timeout_s: float = 3.0
    signature_window_s: int = 300
    max_response_bytes: int = 8 * 1024
    # Dispatcher reads current secret through short-TTL cache.
    secret_cache_ttl_s: float = 5.0
    revalidate_every_s: int = 3600
    revalidate_batch_size: int = 100
    max_resolve_failures: int = 3


@dataclass(frozen=True, slots=True)
class WebhookLimitsConfig:
    max_subscriptions_per_user: int = 10
    max_events_per_subscription: int = 50
    per_host_concurrency: int = 3
    http_connect_timeout_s: float = 5.0
    http_read_timeout_s: float = 10.0
    http_total_timeout_s: float = 15.0
    # Outgoing POST body cap — oversized envelope is dead-lettered
    # before hitting the network.
    max_body_bytes: int = 64 * 1024


@dataclass(frozen=True, slots=True)
class WebhookDispatchConfig:
    enabled: bool = True
    poll_interval_s: float = 1.0
    batch_size: int = 50
    worker_concurrency: int = 20
    lease_ttl_s: int = 30
    reclaim_after_s: int = 60
    # Safety net for unhandled dispatcher exceptions: after this many
    # attempts the engine force-dead-letters the record to stop the
    # reclaim loop. Normal retry/dead-letter transitions are owned by
    # the dispatcher and bounded by WebhookRetryConfig.max_attempts.
    max_unhandled_attempts: int = 5
    # Graceful-shutdown deadline — engine.drain() awaits in-flight
    # deliveries up to this long before cancelling.
    drain_timeout_s: float = 10.0


@dataclass(frozen=True, slots=True)
class WebhookStorageConfig:
    key_prefix: str = "wh"
    retention_sent_s: int = 7 * 86400
    retention_dead_s: int = 30 * 86400
    attempts_history_limit: int = 20
    recent_stream_maxlen: int = 50
    idempotency_ttl_s: int = 24 * 3600


@dataclass(frozen=True, slots=True)
class WebhookPolicyConfig:
    retry: WebhookRetryConfig = field(default_factory=WebhookRetryConfig)
    security: WebhookSecurityConfig = field(default_factory=WebhookSecurityConfig)
    limits: WebhookLimitsConfig = field(default_factory=WebhookLimitsConfig)
    dispatch: WebhookDispatchConfig = field(default_factory=WebhookDispatchConfig)
    storage: WebhookStorageConfig = field(default_factory=WebhookStorageConfig)


DEFAULT_WEBHOOK_POLICY: WebhookPolicyConfig = WebhookPolicyConfig()
