# webhook-engine ⚡

Durable, signed, per-tenant webhook delivery. Another service writes events into a shared database (Postgres or Mongo); this engine reads them, resolves subscriptions, and delivers signed HTTP webhooks with retries, dead-lettering, and SSRF protection. Usable as a **library** or a standalone **microservice**.

**Copy-paste install — run it instantly, no database, no Docker** (in-memory backend, for dev/eval).
Copy the whole block and run it:

```bash
git clone https://github.com/zlexdev/webhooks-engine
cd webhooks-engine
pip install -e ".[dev]"
WHE_BACKEND=memory WHE_SOURCE_KEY=secret123 webhook-engine   # serves on :8080
```

State is in-process and lost on restart; use Postgres/Mongo for anything real.

**Production — engine + Postgres, fully wired:**

```bash
WHE_SOURCE_KEY=$(openssl rand -hex 32) docker compose up
```

Or install and run against your own database:

```bash
pip install -e ".[pg]"                                   # or ".[mongo]"
python scripts/install.py --backend pg \
    --pg-dsn postgresql+asyncpg://user:pass@host/db       # writes .env
webhook-engine                                           # serves on :8080
```

> ⚠ **This project is largely vibe-coded.** Much of it was generated rather than hand-written and battle-tested. Treat it as a strong starting point, not production-hardened software: read the code, run the tests, and validate the delivery / security paths against your own requirements before relying on it.

## What it does

Your services already write to a database. `webhook-engine` turns that into webhook delivery: a producer inserts an **event row** (predefined schema) into the events table/collection — or POSTs it to `/v1/emit` — and the engine does the rest. It looks up every active subscription for that event, builds one durable delivery per subscriber, signs it (HMAC-SHA256), and POSTs it with exponential-backoff retries and dead-lettering. Everything — events, subscriptions, the delivery queue — lives in one database (all-Postgres or all-Mongo), so there is no extra moving part to operate.

```
producer ──insert event row──►  [ events table ]
                                       │  (engine polls, atomic claim)
                                       ▼
                                EventIngestor ──resolve subs──► [ delivery queue ]
                                                                       │
                                                          dispatcher ──signed POST──► subscriber
```

## Quick start

The bundled compose publishes the API on **`${WHE_HOST_PORT:-8090}`** (override to avoid clashes on a
shared box); inside the container it always listens on `8080`.

```bash
# 1. start it (bundles Postgres). Override WHE_HOST_PORT if 8090 is taken.
WHE_SOURCE_KEY=secret123 docker compose up -d

# 2. register a subscriber (returns a `secret` — store it!)
curl -X POST localhost:8090/v1/subscriptions/create \
  -H 'X-Source-Key: secret123' -H 'Content-Type: application/json' \
  -d '{"owner_id":"acme","url":"https://acme.example/hooks","events":["order.paid"]}'

# 3a. emit one event over HTTP …
curl -X POST localhost:8090/v1/emit \
  -H 'X-Source-Key: secret123' -H 'Content-Type: application/json' \
  -d '{"event":"order.paid","tenant_id":"acme","data":{"order_id":42}}'

# 3b. … emit up to 50 events in ONE request (batch) …
curl -X POST localhost:8090/v1/emit/batch \
  -H 'X-Source-Key: secret123' -H 'Content-Type: application/json' \
  -d '{"events":[
        {"event":"order.paid","tenant_id":"acme","data":{"order_id":42}},
        {"event":"order.paid","tenant_id":"acme","data":{"order_id":43}}
      ]}'

# 3c. … or just let your producer INSERT a row — the engine reads it the same way
#     (see "Producing events from a neighbour service" for the full SQL/Mongo recipes).
```

Interactive API docs at `http://localhost:8090/docs`.

## Producing events from a neighbour service

A sibling service emits events one of two ways — both land in the **same** event store and are fanned
out identically. Use HTTP when you want validation + an idempotency receipt; write to the DB directly
when the producer already owns a transaction and wants the event in the *same commit* (the outbox
pattern — no extra hop, no lost events on crash).

### Over HTTP (the stream transport)

```python
import httpx

WHE = "http://wh-api:8090"          # service name on the shared docker network, or the public URL
KEY = "secret123"                     # WHE_SOURCE_KEY
H = {"X-Source-Key": KEY, "Content-Type": "application/json"}

async with httpx.AsyncClient(base_url=WHE, headers=H, timeout=10) as c:
    # single
    r = await c.post("/v1/emit", json={
        "event": "order.paid", "tenant_id": "acme", "data": {"order_id": 42},
    })
    r.raise_for_status()                         # 202; body: {"accepted": true, "event_id": "..."}

    # batch — up to 50 per request; pass your own event_id for idempotent retries
    r = await c.post("/v1/emit/batch", json={"events": [
        {"event": "order.paid", "tenant_id": "acme", "event_id": "ord-42", "data": {"order_id": 42}},
        {"event": "order.refunded", "tenant_id": "acme", "event_id": "ref-7", "data": {"order_id": 39}},
    ]})
    r.raise_for_status()                         # 202; body: {"accepted": true, "count": 2, "event_ids": [...]}
```

> `event_id` is the idempotency key — re-POSTing the same id is a no-op, so producer retries never
> double-emit. Omit it and the engine generates one. Batch rejects empty or >50 with `422`.

### Direct to Postgres (`WHE_BACKEND=pg`)

Insert straight into the events table (default `wh_events`) — ideally in the **same transaction** as
your business write, so the event is durable iff the business change committed.

```sql
-- single event
INSERT INTO wh_events (event_id, event, tenant_id, schema_version, data, status, created_at)
VALUES (gen_random_uuid()::text, 'order.paid', 'acme', '1.0',
        '{"order_id": 42}'::jsonb, 'pending', now())
ON CONFLICT (event_id) DO NOTHING;            -- idempotent on event_id

-- batch (one multi-row INSERT — the engine claims them on its next tick)
INSERT INTO wh_events (event_id, event, tenant_id, schema_version, data, status, created_at)
VALUES
  ('ord-42', 'order.paid',     'acme', '1.0', '{"order_id": 42}'::jsonb, 'pending', now()),
  ('ref-7',  'order.refunded', 'acme', '1.0', '{"order_id": 39}'::jsonb, 'pending', now())
ON CONFLICT (event_id) DO NOTHING;
```

`status` MUST be `'pending'` (the engine only claims pending rows); `data` is `jsonb`; `created_at`
drives claim ordering. Leave `locked_at`/`error` null.

### Direct to MongoDB (`WHE_BACKEND=mongo`)

The event document's `_id` **is** the `event_id` (that's what makes it idempotent).

```javascript
// single
db.wh_events.insertOne({
  _id: "ord-42", event: "order.paid", tenant_id: "acme", schema_version: "1.0",
  data: { order_id: 42 }, status: "pending", created_at: new Date()
});

// batch — ordered:false so a duplicate _id (retry) skips that doc and the rest still insert
db.wh_events.insertMany([
  { _id: "ord-42", event: "order.paid",     tenant_id: "acme", schema_version: "1.0",
    data: { order_id: 42 }, status: "pending", created_at: new Date() },
  { _id: "ref-7",  event: "order.refunded", tenant_id: "acme", schema_version: "1.0",
    data: { order_id: 39 }, status: "pending", created_at: new Date() }
], { ordered: false });
```

## Key concepts

- **Event store** (`BaseEventStore`) — the producer→engine queue. Ships `PgEventStore` and `MongoEventStore`; a producer writes `IncomingEvent` rows/documents, the engine claims them atomically (`FOR UPDATE SKIP LOCKED` / `find_one_and_update`).
- **Ingestor** (`EventIngestor`) — reads claimed events, resolves subscriptions, enqueues one signed delivery per subscriber. Idempotent by `event_id`.
- **Delivery store** (`WebhookDeliveryStoreProtocol`) — durable delivery queue. Postgres, Mongo, and Redis backends.
- **Dispatcher + engine** — sign, POST, classify into sent / retry / dead-letter; a tick loop drains the queue.
- **Signing** (`HmacSigner` / `verify_webhook`) — HMAC-SHA256 with a timestamp; a dependency-free `verify_webhook` helper ships for receivers.
- **SSRF guard** (`UrlSafetyValidator`) — blocks RFC-1918 / link-local targets, re-checked at send time.

## Install options

| Command | Backend |
|---|---|
| `pip install -e ".[pg]"` | all-Postgres profile (`WHE_BACKEND=pg`) |
| `pip install -e ".[mongo]"` | all-Mongo profile (`WHE_BACKEND=mongo`) |
| `pip install -e ".[dev]"` | test + lint toolchain |

> `asyncbus` is a private dependency used only for the optional dead-letter event; the service runs with it disabled. See `Dockerfile` for the PAT-gated install line.

## Configuration

Every `WHE_*` env var, with defaults, lives in [`.env.example`](.env.example) — copy it to `.env` and
fill in the backend you're using, or generate one with `python scripts/install.py --backend pg`.

## Deploy

The stack is collision-proof on a shared box: containers are prefixed (`wh-api`, `wh-db`), the volume
is `wh_pg_data`, Postgres publishes **no** host port (reached as `wh-db:5432` over the compose net),
and the API port is `${WHE_HOST_PORT:-8090}`. Pin the compose project with `-p wh`.

**Install / update (copy-paste — idempotent, re-run to update):**

```bash
git clone https://github.com/zlexdev/webhooks-engine /etc/webhooks-engine
# .env: a stable source key + a free host port (override 8090 if it's taken)
printf 'WHE_SOURCE_KEY=%s\nWHE_HOST_PORT=8090\n' "$(openssl rand -hex 32)" > /etc/webhooks-engine/.env

# pick a working compose invocation: v2 plugin, legacy docker-compose, or install the plugin
if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
elif command -v apt-get >/dev/null 2>&1 && apt-get update -qq && apt-get install -y docker-compose-plugin && docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
else
  curl -fsSL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-$(uname -m)" \
    -o /usr/local/bin/docker-compose && chmod +x /usr/local/bin/docker-compose
  COMPOSE=(docker-compose)
fi

"${COMPOSE[@]}" -p wh --project-directory /etc/webhooks-engine \
  --env-file /etc/webhooks-engine/.env \
  -f /etc/webhooks-engine/docker-compose.yml up -d --build
sleep 5 && curl -fsS "http://localhost:8090/health" && echo "  OK"
```

> No `docker compose` plugin on the box (old Docker install)? The snippet above detects it and
> falls back to `apt-get install docker-compose-plugin`, or a standalone `docker-compose` binary if
> `apt` isn't available — `docker compose -p wh ...` failing with `Usage: docker [OPTIONS] COMMAND`
> and `unknown shorthand flag: 'p'` is exactly that missing-plugin case.

**Uninstall (copy-paste — removes containers, network, image, DB volume and files):**

```bash
docker compose -p wh --project-directory /etc/webhooks-engine \
  -f /etc/webhooks-engine/docker-compose.yml down --remove-orphans 2>/dev/null || \
docker-compose -p wh --project-directory /etc/webhooks-engine \
  -f /etc/webhooks-engine/docker-compose.yml down --remove-orphans
docker volume rm wh_wh_pg_data 2>/dev/null || true   # drop the Postgres data volume (irreversible)
docker image rm wh_wh-api 2>/dev/null || true        # drop the built image
rm -rf /etc/webhooks-engine                           # drop the project files
```

> On a host with only `docker-compose` v1 (1.29) on Docker 25+/29, in-place recreate hits
> `KeyError: 'ContainerConfig'` — use `docker-compose -p wh down --remove-orphans` then `up -d --build`
> instead of recreate.

Scaling, durability, rollback: [docs/runbooks/deploy.md](docs/runbooks/deploy.md).

## Docs

- [Quickstart](docs/usage/quickstart.md) · [Library wiring](docs/usage/library.md) · [Microservice & HTTP API](docs/usage/microservice.md)
- [Reference](docs/reference/) · [Runbooks](docs/runbooks/deploy.md)

## Changelog

See [CHANGELOG.md](CHANGELOG.md).

## License

MIT.
