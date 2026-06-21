# webhooks/stores/

Persistence layer for the webhook delivery queue + subscription reads.

## Purpose

A typed store contract with two interchangeable backends. Keeps the
delivery queue (schedule, per-row state, attempt history) and the
subscription-config reads behind narrow protocols, so the dispatcher
never couples to a concrete datastore.

## Contract

- `WebhookDeliveryStoreProtocol` (`libs/webhooks/protocols.py`) — the
  delivery-queue ABC: `enqueue_many`, `claim_batch`, `reclaim_stale`,
  `mark_sent` / `schedule_retry` / `mark_dead`, `append_attempt`,
  `get`, `recent_for_subscription`. `BaseWebhookDeliveryStore` in
  `base.py` is a name alias kept for back-compat.
- `SubscriptionReaderProtocol` — `for_event(event_name, tenant_id)`
  resolves the subscriptions a fan-out should deliver to.
- All I/O crosses the boundary as `DeliveryRecord` / `AttemptRecord` /
  `SubscriptionSnapshot` DTOs (`libs/webhooks/types.py`) — never raw
  backend rows.

## Backends

### `pg.py` — live production store

`PgWebhookDeliveryStore` + `PgSubscriptionReader`. Wired in
`app/bootstrap/wiring.py` and held on `AppContext`. `libs` stays
app-agnostic: the app injects a `session_factory` plus repo
constructors that satisfy `WebhookDeliveryRepoProtocol` /
`WebhookAttemptRepoProtocol` / `WebhookSubscriptionRepoProtocol`
(implemented in `app/db/repos/webhook.py`). Each store method opens its
**own** short transaction — the store owns the session, callers never
share one. Delivery dedup is the repo's `(subscription_id, event_id)`
`ON CONFLICT DO NOTHING`; `claim_batch` leases due rows with
`FOR UPDATE SKIP LOCKED`.

### `redis.py` — alternative backend (implemented, not wired)

`RedisWebhookDeliveryStore` — a complete Redis implementation that is
**not currently wired**; bootstrap uses the PG store. Kept as an opt-in
backend for a future Redis-backed deployment.

- ZSET `wh:sched` is the fire-at schedule; `claim_batch` pops due rows
  atomically via the `CLAIM_BATCH_LUA` script.
- `RECLAIM_LUA` reclaims rows whose lease expired (worker crash).
- Per-subscription history via `XADD` + `MAXLEN`; dead-delivery TTLs.

If it is ever wired, the trade-off is that the queue is ephemeral to
Redis — a full Redis loss drops in-flight deliveries, recoverable by
replaying from the event outbox.

## Redis keying convention (`redis.py`)

- `wh:sched` — global ZSET, score = fire-at ms, member = delivery id
- `wh:del:<id>` — per-delivery hash (envelope bytes + metadata)
- `wh:hist:<sub_id>` — per-subscription XSTREAM, capped via MAXLEN
- `wh:lease:<id>` — per-delivery lease key with TTL

All keys namespaced with the configured prefix so multi-tenant Redis
deployments stay isolated.

## Layout

- `base.py` — `BaseWebhookDeliveryStore` alias + protocol/DTO re-exports.
- `pg.py` — `PgWebhookDeliveryStore`, `PgSubscriptionReader`, and the
  three repo protocols the app layer must satisfy.
- `redis.py` — `RedisWebhookDeliveryStore` + the `CLAIM_BATCH_LUA` /
  `RECLAIM_LUA` scripts.

## Do-not

- Don't add a third "convenience" store (e.g. in-memory) without
  honouring atomic claim semantics — a naive store that passes tests
  will deadlock or double-dispatch in prod.
- Don't inline the Redis Lua scripts elsewhere — they're tuned for the
  exact key layout above; changing the layout means updating the
  scripts in lockstep.
- Don't leak backend types across the contract boundary — callers see
  only `DeliveryRecord` / `AttemptRecord` / `SubscriptionSnapshot`.
