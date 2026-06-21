# Reference — core library

The framework-agnostic core. No FastAPI, no transport coupling.

## `WebhookEngine`

The tick loop. Per `tick()`: reclaim stale leases, claim a batch, dispatch each
record concurrently under a semaphore. `drain(timeout_s)` awaits in-flight work
on shutdown. Constructor: `store`, `dispatcher`, `config` (`WebhookDispatchConfig`),
`clock`.

## `WebhookDispatcher`

Orchestrates one delivery: body-size guard → resolve secret → re-check resolved IP
against the CIDR block set → sign → POST under the per-host bulkhead → classify
into sent / retry / dead-letter. Pure orchestration; all I/O is behind protocols.

## Stores

| Class | Backend | Use |
|---|---|---|
| `RedisWebhookDeliveryStore` | Redis | default production store |
| `PgWebhookDeliveryStore` | Postgres (`[pg]`) | strongest durability |
| `MemoryWebhookDeliveryStore` | in-process dict | tests, local dev |

All satisfy `WebhookDeliveryStoreProtocol`. Claim/reclaim is atomic in the Redis
and Postgres impls.

## `HmacSigner`

HMAC-SHA256 over the envelope body plus a timestamp; emits the signature headers.
Supports a current + previous secret for graceful rotation.

## `UrlSafetyValidator`

SSRF guard. Blocks RFC-1918 / link-local / loopback CIDRs (configurable via
`WebhookSecurityConfig.blocked_cidrs`) and is re-checked at send time to catch
DNS rebinding between enqueue and delivery.

## Config

`WebhookPolicyConfig` aggregates `retry`, `security`, `limits`, `dispatch`, and
`storage` sub-configs — all frozen dataclasses, every tunable in one place.

## Protocols (extension points)

`WebhookDeliveryStoreProtocol`, `SubscriptionReaderProtocol`,
`SecretReaderProtocol`, `HttpSenderProtocol`. Implement these to plug in your own
backend, subscription source, secret resolver, or HTTP client.
