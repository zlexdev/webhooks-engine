# Using webhook-engine as a library

When you already have a worker / process supervisor, skip the FastAPI layer and
embed the core directly. You own the tick loop; the library owns delivery.

## Minimal wiring

```python
from redis.asyncio import Redis

from libs.shared.time import now_utc
from webhook_engine import (
    WebhookEngine, WebhookDispatcher, RedisWebhookDeliveryStore,
    HmacSigner, HttpxSender, DeliveryPolicy, HostSemaphorePool,
    UrlSafetyValidator, WebhookPolicyConfig, WebhookStorageConfig,
)

cfg = WebhookPolicyConfig()
redis = Redis.from_url("redis://localhost:6379/0")
store = RedisWebhookDeliveryStore(redis, WebhookStorageConfig())

dispatcher = WebhookDispatcher(
    signer=HmacSigner(),
    http_sender=HttpxSender(cfg),
    policy=DeliveryPolicy(cfg.retry),
    config=cfg,
    store=store,
    secret_reader=my_secret_reader,        # your SecretReaderProtocol impl
    ssrf_guard=UrlSafetyValidator(cfg.security.blocked_cidrs),
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
| `SubscriptionReaderProtocol` | "who is subscribed to event X" | — (yours) |
| `SecretReaderProtocol` | resolve a subscription's signing secret | — (yours) |
| `HttpSenderProtocol` | the actual HTTP POST | `HttpxSender` |

The microservice layer (`service/subscription_store.py`) is a worked reference
implementation of the reader contracts on Redis — copy or adapt it.

## Swapping the store

Any backend works as long as it satisfies `WebhookDeliveryStoreProtocol` and
provides atomic claim/reclaim. For tests, use the bundled in-memory store:

```python
from webhook_engine.stores.memory import MemoryWebhookDeliveryStore
store = MemoryWebhookDeliveryStore()
```

It implements the full contract with no external dependency — ideal for unit
tests of your dispatch wiring.
