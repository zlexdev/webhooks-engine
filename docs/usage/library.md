# Using webhook-engine as a library

When you already have a worker / process supervisor, skip the FastAPI layer and
embed the core directly. You own the tick loop; the library owns delivery.

## Minimal wiring

```python
from datetime import UTC, datetime

from redis.asyncio import Redis

from webhook_engine import (
    WebhookEngine, WebhookDispatcher, RedisWebhookDeliveryStore,
    HmacSigner, HttpxSender, DeliveryPolicy, HostSemaphorePool,
    UrlSafetyValidator, WebhookPolicyConfig, WebhookStorageConfig,
)

cfg = WebhookPolicyConfig()
redis = Redis.from_url("redis://localhost:6379/0")
store = RedisWebhookDeliveryStore(redis, WebhookStorageConfig())

def now_utc() -> datetime:          # any zero-arg () -> aware datetime works
    return datetime.now(UTC)

dispatcher = WebhookDispatcher(
    signer=HmacSigner(),
    http_sender=HttpxSender(cfg),
    policy=DeliveryPolicy(cfg.retry),
    config=cfg,
    store=store,
    secret_reader=my_secret_reader,        # your SecretReaderProtocol impl
    ssrf_guard=UrlSafetyValidator(cfg.security),
    host_sem=HostSemaphorePool(cfg.limits.per_host_concurrency),
    clock=now_utc,
)

engine = WebhookEngine(store=store, dispatcher=dispatcher,
                       config=cfg.dispatch, clock=now_utc)

# drive it from your loop:
while running:
    await engine.tick()
    await asyncio.sleep(cfg.dispatch.poll_interval_s)
await engine.drain(cfg.dispatch.drain_timeout_s)
```

## What you must provide

The library defines contracts (ABCs); you supply the impls that touch *your*
data:

| Contract | What it does | Ships |
|---|---|---|
| `WebhookDeliveryStoreProtocol` | durable queue | Redis, Postgres, in-memory |
| `SecretReaderProtocol` | resolve a subscription's signing secret | — (yours) |
| `HttpSenderProtocol` | the actual HTTP POST | `HttpxSender` |
| `SubscriptionReaderProtocol` | "who is subscribed to event X" | — (yours), **only for `[bus]` fan-out** |

`SubscriptionReaderProtocol` is **not** taken by `WebhookDispatcher` or
`WebhookEngine`. You need it only if you let the engine fan an event out to
subscribers over an event bus (the `[bus]` extra). Enqueuing deliveries
yourself, you never implement it.

The microservice layer (`service/subscription_store.py`) is a worked reference
implementation of the reader contracts — copy or adapt it.

## Optional seams

Every one of these defaults to today's behaviour; pass nothing and nothing
changes.

### Your own signature format

`WebhookDispatcher(signer=...)` takes any `SignerProtocol`. Migrating off your
own delivery code, keep the signature your receivers already verify:

```python
from dataclasses import dataclass
from webhook_engine import SignerProtocol
from webhook_engine.exceptions import SignatureVerificationError

@dataclass(frozen=True, slots=True)
class GithubHeaders:              # inherits nothing from the package
    signature: str
    def as_dict(self) -> dict[str, str]:
        return {"X-Hub-Signature-256": self.signature,
                "Content-Type": "application/json"}

class GithubStyleSigner(SignerProtocol):
    def sign(self, *, delivery_id, event_name, event_id, body, secret, now):
        mac = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        return GithubHeaders(f"sha256={mac}")

    def verify(self, *, body, headers, secret, now, window_s):
        # headers arrive as the framework produced them — match case-insensitively
        provided = {k.lower(): v for k, v in headers.items()}
        ...
        raise SignatureVerificationError("bad mac")
```

The header *names* travel with your object, so the dispatcher never learns them.
Receivers use the same signer: `verify_webhook(..., signer=GithubStyleSigner())`.

### Seeing what the receiver answered

`tick()` returns counters. To read the receiver's response — a `202` body, an
idempotency key echoed back, a verdict — pass a callback:

```python
async def on_outcome(record, outcome):
    if outcome.status is DeliveryStatus.SENT:
        handle(record.event_id, outcome.attempt.response_snippet)

engine = WebhookEngine(..., on_outcome=on_outcome)
```

It runs after the delivery is committed and outside the concurrency gate, under
`dispatch.on_outcome_timeout_s` (default 5s). A callback that hangs or raises is
logged and ignored — it cannot change a delivery or the tick counters.

### Reaching a receiver on a private network

The SSRF guard denies any target resolving into `security.blocked_cidrs`, which
covers the Docker / Kubernetes ranges. To reach a service on your own network,
name it — do **not** delete the range:

```python
WebhookSecurityConfig(trusted_private_hosts=("receiver:9000",))
```

Entries are exact `host` or `host:port`; no wildcards, because the target URL is
tenant-supplied. A portless entry trusts every port on that host — name the port
unless you mean it. Standalone service: `WHE_TRUSTED_PRIVATE_HOSTS='["receiver:9000"]'`.

### Re-resolving DNS between attempts

By default the address is pinned when the delivery is enqueued — the strongest
defence against DNS rebinding. On a long retry ladder (8 attempts, hour-long
backoff) a receiver that moved becomes unreachable and the journal only says
`network_error`. Supply a resolver to trade some of that protection for
reachability:

```python
class Resolver(TargetResolverProtocol):
    async def resolve(self, host: str) -> str | None:
        ...  # None means "unknown" — the pinned IP is kept

WebhookDispatcher(..., resolver=Resolver())
```

Whatever it returns still passes the same `check_ip` before anything is sent.

## Swapping the store

Any backend works as long as it satisfies `WebhookDeliveryStoreProtocol` and
provides atomic claim/reclaim. For tests, use the bundled in-memory store:

```python
from webhook_engine.stores.memory import MemoryWebhookDeliveryStore
store = MemoryWebhookDeliveryStore()
```

It implements the full contract with no external dependency — ideal for unit
tests of your dispatch wiring.
