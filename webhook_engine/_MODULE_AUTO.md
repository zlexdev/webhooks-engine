# webhook_engine/
<!-- AUTO-GENERATED. Do not edit. Run gen_module_auto.py to update. -->

> Durable, signed, per-tenant webhook delivery.

## Submodules

- [`_shared/`](_shared/_MODULE_AUTO.md) (2 py, 2 fn)
- [`ingest/`](ingest/_MODULE_AUTO.md) — Ingestion layer — producers write events to a shared DB; the engine reads. (6 py, 9 cls)
- [`service/`](service/_MODULE_AUTO.md) — Standalone HTTP microservice layer — optional, behind the ``service`` extra. (15 py, 29 cls, 44 fn)
- [`stores/`](stores/_MODULE_AUTO.md) — Persistence stores for webhook delivery — pick a backend at bootstrap. (5 py, 8 cls, 9 fn)

## __init__.py
```
# Durable, signed, per-tenant webhook delivery.


```

## config.py
```
# Immutable policy configs for webhook delivery.


cls WebhookRetryConfig: max_attempts: int, base_delay_s: float, backoff_factor: float, max_delay_s: float, jitter_pct: float

cls WebhookSecurityConfig: require_https_in_prod: bool, allow_http_in_dev: bool, blocked_cidrs: tuple[str, ...], allowed_hosts: tuple[str, ...] | None, trusted_private_hosts: tuple[str, ...], preflight_enabled: bool, preflight_timeout_s: float, signature_window_s: int, max_response_bytes: int, secret_cache_ttl_s: float, … (13)

cls WebhookLimitsConfig: max_subscriptions_per_user: int, max_events_per_subscription: int, per_host_concurrency: int, http_connect_timeout_s: float, http_read_timeout_s: float, http_total_timeout_s: float, max_body_bytes: int

cls WebhookDispatchConfig: enabled: bool, poll_interval_s: float, batch_size: int, worker_concurrency: int, lease_ttl_s: int, reclaim_after_s: int, max_unhandled_attempts: int, drain_timeout_s: float, on_outcome_timeout_s: float

cls WebhookStorageConfig: key_prefix: str, retention_sent_s: int, retention_dead_s: int, attempts_history_limit: int, recent_stream_maxlen: int, idempotency_ttl_s: int

cls WebhookPolicyConfig: retry: WebhookRetryConfig, security: WebhookSecurityConfig, limits: WebhookLimitsConfig, dispatch: WebhookDispatchConfig, storage: WebhookStorageConfig

```

## dispatcher.py
```
# Single-delivery orchestration.


cls _SignedDelivery: body: bytes, headers: dict[str, str], envelope: WebhookEnvelope

cls WebhookDispatcher
  __init__() -> None
  async send(record: DeliveryRecord) -> DispatchOutcome

```

## engine.py
```
# Framework-agnostic tick loop.


cls EngineTickStats: claimed: int, sent: int, retried: int, dead: int, errors: int

cls WebhookEngine
  __init__(store: WebhookDeliveryStoreProtocol, dispatcher: WebhookDispatcher, config: WebhookDispatchConfig, clock: Clock, on_outcome: OutcomeCallback? = None) -> None
  async tick() -> EngineTickStats
  async drain(timeout_s: float) -> None

```

## enums.py
```
# Domain enums for webhook delivery — all ``StrEnum`` to match house style.


cls WebhookScope(StrEnum): TENANT, GLOBAL

cls DeliveryStatus(StrEnum): PENDING, IN_FLIGHT, SENT, FAILED_RETRY, DEAD_LETTERED, CANCELLED

cls SubscriptionStatus(StrEnum): ACTIVE, PAUSED, DELETED

cls RetryReason(StrEnum): HTTP_5XX, HTTP_408, HTTP_429, NETWORK_ERROR, TIMEOUT, NON_RETRYABLE_4XX, PAYLOAD_TOO_LARGE, BLOCKED_IP_AT_SEND

cls RedactionReason(StrEnum): EXPLICIT_META, AUTO_SUFFIX

cls InvalidTargetReason(StrEnum): BAD_SCHEME, NOT_HTTPS, UNRESOLVABLE, PRIVATE_IP, LOOPBACK, LINK_LOCAL, CGNAT, ULA, BLOCKED_HOST, HAS_CREDENTIALS, PREFLIGHT_FAILED

cls SignatureVersion(StrEnum): V1

```

## envelope.py
```
# Outgoing envelope + payload redaction helpers.


cls WebhookEnvelope: API_VERSION: ClassVar[str], id: str, event: str, event_id: str, schema_version: str, emitted_at: datetime, tenant_id: str | None, attempt: int, redelivery_of: str | None, data: dict[str, Any]
  # Outer JSON shape every target receives.

should_auto_redact(field_name: str) -> bool
  # Whether a field name matches one of the auto-redact patterns.

redact_payload(data: dict[str, Any, meta: WebhookMeta) -> dict[str, Any]
  # Drop sensitive fields from the outgoing payload.

sha256_hex(body: bytes) -> str

```

## events.py
```
# Webhook lifecycle + dead-letter meta-events.


cls WebhookDeliveryExhausted(PostEvent): delivery_id: str, subscription_id: str, owner_id: str, source_event: str, attempts: int, last_error: str, use_outbox: ClassVar[bool]

cls WebhookSubscriptionCreated(PostEvent): subscription_id: str, owner_id: str, url_host: str, event_names: tuple[str, ...], scope: str, use_outbox: ClassVar[bool]

cls WebhookSubscriptionUpdated(PostEvent): subscription_id: str, owner_id: str, changed_fields: tuple[str, ...], use_outbox: ClassVar[bool]

cls WebhookSubscriptionPaused(PostEvent): subscription_id: str, owner_id: str, reason: str, use_outbox: ClassVar[bool]

cls WebhookSubscriptionResumed(PostEvent): subscription_id: str, owner_id: str, use_outbox: ClassVar[bool]

cls WebhookSubscriptionDeleted(PostEvent): subscription_id: str, owner_id: str, use_outbox: ClassVar[bool]

cls WebhookSubscriptionSecretRotated(PostEvent): subscription_id: str, owner_id: str, use_outbox: ClassVar[bool]

cls WebhookSubscriptionAutoSuspended(PostEvent): subscription_id: str, owner_id: str, url_host: str, reason: str, use_outbox: ClassVar[bool]

```

## exceptions.py
```
# Webhook exception hierarchy.


cls WebhookError(Exception)
  # Base for every error raised inside ``webhook_engine``.
  __init__(message: str = '', **context: Any) -> None

cls WebhookStartupError(WebhookError)
  # Raised during registry discovery — app should fail to boot.

cls WebhookMetaMissing(WebhookStartupError)

cls WebhookEventCollision(WebhookStartupError)
  __init__(name: str, a: str, b: str) -> None

cls WebhookTenantFieldMissing(WebhookStartupError)
  __init__(event_name: str, event_cls: str, tenant_field: str) -> None

cls UnknownEventName(WebhookError)

cls InvalidWebhookTarget(WebhookError)
  __init__(reason: InvalidTargetReason, detail: str = '') -> None

cls WebhookPermissionDenied(WebhookError)

cls SubscriptionLimitExceeded(WebhookError)

cls EventLimitExceeded(WebhookError)

cls SubscriptionNotFound(WebhookError)

cls DeliveryNotFound(WebhookError)

cls DeliveryInFlight(WebhookError)
  # Admin redeliver blocked: current delivery is IN_FLIGHT.

cls SignatureVerificationError(Exception)
  # Consumer-side helper raised by :meth:`HmacSigner.verify`.

```

## fanout.py
```
# Fanout handler — one ``EventHandler`` per discovered webhook event.


cls WebhookFanoutHandler(EventHandler[BaseEvent])
  # Generic handler subclass. The installer synthesizes one concrete
  __init__(bus: BaseEventBus, reader: SubscriptionReaderProtocol, store: WebhookDeliveryStoreProtocol, dispatch_enabled: Callable[[], bool, clock: Callable[[], datetime) -> None
  async handle(event: BaseEvent, ctx: HandlerContext) -> None
  specialize() -> type[WebhookFanoutHandler]
    # Return a dedicated subclass bound to one event.

_event_to_raw(event: BaseEvent) -> dict[str, object]
  # Event → plain dict. Mirrors the outbox serializer so the same

```

## fanout_installer.py
```
# Installs one :class:`WebhookFanoutHandler` per catalog entry.


cls WebhookFanoutInstaller
  __init__() -> None
  install_all(catalog: WebhookCatalog? = None) -> int
    # Install handlers for every entry; return count of new installs.
  is_installed(event_name: str) -> bool

```

## host_semaphores.py
```
# Per-host asyncio.Semaphore registry (bulkhead).


cls HostSemaphorePool
  __init__(limits: WebhookLimitsConfig) -> None
  async acquire(host: HostName) -> AsyncIterator[None]
  active_hosts() -> list[HostName]
  size() -> int

```

## http_sender.py
```
# Default :class:`HttpSenderProtocol` implementation over ``httpx``.


cls HttpxSender
  # Structural impl of :class:`HttpSenderProtocol`.
  __init__(client: httpx.AsyncClient) -> None
  async post() -> HttpSendResult

```

## meta.py
```
# ``WebhookMeta`` marker + ``@webhook_event`` decorator.

T = TypeVar('T')

cls WebhookMeta: name: str, description: str, scopes: tuple[WebhookScope, ...], tenant_field: str | None, since: str, sample_payload: dict[str, Any] | None, redacted_fields: tuple[str, ...], allow_sensitive: bool
  # Attached to a ``BaseEvent`` subclass to expose it to subscribers.

webhook_event(meta: WebhookMeta) -> Callable[[type[T]], type[T]]
  # Attach ``webhook_meta`` to an event class. Opt-in marker.

```

## policy.py
```
# Retry classification + next-delay computation.


cls DeliveryPolicy
  classify_status(status: int) -> RetryReason | None
  classify_exception(exc: Exception) -> RetryReason
  retry_on(reason: RetryReason, attempt: int, cfg: WebhookRetryConfig) -> bool
  next_delay_s(attempt: int, cfg: WebhookRetryConfig, rng: random.Random? = None) -> float
  outcome_status(reason: RetryReason?, will_retry: bool) -> DeliveryStatus

```

## protocols.py
```
# Protocol contracts — services don't couple to redis / httpx.


cls SecretMaterial: current: str, previous: str | None, previous_expires_at: datetime | None
  # Plaintext secret material resolved at dispatch time.

cls WebhookDeliveryStoreProtocol(ABC)
  async enqueue_many(records: list[DeliveryRecord) -> None
  async claim_batch(now: datetime, lease_ttl_s: int, batch_size: int) -> list[DeliveryRecord]
  async reclaim_stale(now: datetime) -> int
  async mark_sent(delivery_id: DeliveryId, attempt: AttemptRecord) -> None
  async schedule_retry(delivery_id: DeliveryId, fire_at: datetime, attempt: AttemptRecord) -> None
  async mark_dead(delivery_id: DeliveryId, attempt: AttemptRecord) -> None
  async append_attempt(delivery_id: DeliveryId, attempt: AttemptRecord) -> None
  async get(delivery_id: DeliveryId) -> DeliveryRecord | None
  async recent_for_subscription(sub_id: str, limit: int) -> list[DeliveryRecord]

cls HttpSenderProtocol(Protocol)
  async post() -> HttpSendResult

cls SubscriptionReaderProtocol(ABC)
  async for_event(event_name: str, tenant_id: str?) -> list[SubscriptionSnapshot]

cls SignerProtocol(ABC)
  # How a delivery is signed — and how a receiver checks that signature.
  sign() -> SignedHeaders
  verify() -> None
    # Raise :class:`~webhook_engine.exceptions.SignatureVerificationError`

cls TargetResolverProtocol(ABC)
  # Optional per-attempt DNS re-resolution.
  async resolve(host: str) -> str | None
    # Return the current address for ``host``, or ``None`` for "unknown".

cls SecretReaderProtocol(ABC)
  async get(subscription_id: str) -> SecretMaterial
  invalidate(subscription_id: str) -> None

```

## registry.py
```
# Event catalog discovery.


cls WebhookEventEntry: event_cls: type, meta: WebhookMeta

cls WebhookCatalog: entries: tuple[WebhookEventEntry, ...]

cls WebhookEventRegistry
  discover(roots: tuple[type, ... = ...) -> WebhookCatalog

```

## signer.py
```
# HMAC-SHA256 signing for webhook deliveries.


cls HmacSigner(SignerProtocol)
  __init__(version: SignatureVersion = SignatureVersion.V1) -> None
  sign() -> SignatureHeaders
  verify() -> None
    # Consumer-side helper exported for user-docs + tests.

```

## ssrf_guard.py
```
# Pure URL / IP safety checks. No DNS I/O — the caller resolves the host and


cls BlockedCidrSet: nets_v4: tuple[ipaddress.IPv4Network, ...], nets_v6: tuple[ipaddress.IPv6Network, ...]

cls UrlSafetyValidator
  # Pure (no I/O) URL + IP safety checks.
  __init__(security: WebhookSecurityConfig) -> None
  validate(url: str, resolved_ips: tuple[str, ...) -> ResolvedTarget
  check_ip(ip: str) -> None
    # Re-validation hook used by the dispatcher before POST.

_host_matches(host: str, patterns: tuple[str, ...) -> bool
  # Exact name or ``*.suffix``, case-insensitive. Used by allowed_hosts.

_is_trusted_private(host: str, port: int?, patterns: tuple[str, ...) -> bool
  # Exact ``host`` or ``host:port``. No wildcards — see the config docstring.

_normalise_host(host: str) -> str
  # Lower-case, drop the FQDN trailing dot, fold IDN to its ASCII form.

```

## types.py
```
# Type aliases + small public dataclasses for webhook_engine.


cls AttemptRecord: attempt: int, attempted_at: datetime, http_code: int | None, duration_ms: int, error: str | None, response_snippet: str

cls DeliveryRecord: delivery_id: DeliveryId, subscription_id: SubscriptionId, owner_id: OwnerId, event_name: EventName, event_id: str, schema_version: str, tenant_id: str | None, data_json: bytes, emitted_at: datetime, target_url: str, … (16)
  # Persisted queue row — everything needed to sign + POST one delivery.

cls HttpSendResult: status: int, headers: dict[str, str], body_snippet: str, duration_ms: int

cls DispatchOutcome: status: DeliveryStatus, next_fire_at: datetime | None, attempt: AttemptRecord

cls SignedHeaders(Protocol)
  # What a signer hands back: the outbound headers for one delivery.
  as_dict() -> dict[str, str]

cls SignatureHeaders: webhook_id: str, webhook_event: str, webhook_event_id: str, webhook_timestamp: str, webhook_signature: str
  # The shipped ``Webhook-*`` header set — what :class:`HmacSigner` returns.

cls ResolvedTarget: host: HostName, port: int, scheme: str, ips: tuple[str, ...]

cls SubscriptionSnapshot: id: SubscriptionId, owner_id: OwnerId, url: str, resolved_ip: str, retry_overrides: WebhookRetryConfig | None
  # Tenant-scoped subscription view used by the fanout handler.

```

## verify.py
```
# Receiver-side signature verification — pure, dependency-free.

DEFAULT_WINDOW_S = 300

cls WebhookVerificationError(Exception)
  # Raised when an incoming webhook fails signature or freshness checks.

_header(headers: Mapping[str, str, name: str) -> str

_verify_via_signer() -> None

verify_webhook() -> None
  # Validate an incoming webhook delivery in constant time.

```
