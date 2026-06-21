# Deploy runbook

## One-liner (local / single host)

```bash
WHE_SOURCE_KEY=$(openssl rand -hex 32) docker compose up -d
```

Starts Redis (with AOF persistence) + the engine. Health: `curl localhost:8080/health`.

## Configuration

All config is environment variables prefixed `WHE_` (see `.env.example`). For a
real deployment, set at minimum:

```bash
WHE_SOURCE_KEY=<32-byte random hex>     # required — service-to-service auth
WHE_REDIS_URL=redis://<host>:6379/0     # point at managed Redis in prod
```

Generate the source key once and distribute it to every stream service that
emits events:

```bash
openssl rand -hex 32
```

## Scaling

The engine is **single-replica-safe by design**: claim/reclaim is atomic
(Redis Lua / Postgres `SKIP LOCKED`), so you *can* run multiple replicas against
one Redis/PG and they will not double-deliver. Each replica runs its own tick
loop and competes for the same queue.

- Vertical knobs: `WHE_BATCH_SIZE`, `WHE_WORKER_CONCURRENCY`.
- Horizontal: add replicas; they coordinate through the shared store.
- Per-host fairness: `per_host_concurrency` (in `WebhookLimitsConfig`) caps how
  many simultaneous requests hit any single subscriber host.

## Graceful shutdown

On `SIGTERM` the lifespan handler cancels the tick loop and calls
`engine.drain(WHE_DRAIN_TIMEOUT_S)`, letting in-flight deliveries finish (or
cancelling them past the deadline). Set the orchestrator's termination grace
period higher than `WHE_DRAIN_TIMEOUT_S`.

## Durability & recovery

- Redis store: enable AOF (the compose file does). A crash mid-delivery leaves
  the record in the inflight set; the next `reclaim_stale` returns it to ready
  after the lease expires.
- Postgres store (`[pg]` extra): deliveries survive a full process loss; claims
  use `FOR UPDATE SKIP LOCKED`.

## Rollback

The service is stateless apart from the delivery store. To roll back:

```bash
docker compose down
# redeploy the previous image tag
docker compose up -d
```

In-flight deliveries persisted in Redis/PG are picked up by the new (old)
version on startup — no manual replay needed unless the queue schema changed
(it has not within a minor version).

## Monitoring

- `GET /ready` returns 503 when Redis is unreachable — wire it to your load
  balancer's health check.
- Tick stats (`claimed / sent / retried / dead / errors`) are logged per tick
  when any record is claimed. Ship structured logs to your aggregator.
