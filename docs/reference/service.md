# Reference — service layer

The `webhook_engine.service` package is the **admin/ops HTTP surface**. It is
optional and sits on top of the core library — every route only authenticates,
validates, and delegates to the library.

## Layer role

The service layer is **not** the delivery critical path. The critical path is:

```
stream → WebhookEngine.tick() → WebhookDispatcher → subscriber URL
```

The HTTP surface exists for operators and dashboards only:

| Surface | Role |
|---|---|
| `/v1/subscriptions/*` | CRUD + pause/resume for subscription config |
| `/v1/deliveries/*` | inspect delivery state, trigger manual redeliver, send test ping |
| `/v1/emit` | convenience ingress — HTTP → event store. **Optional**: if all producers can write to the event store directly, this endpoint is redundant and can be omitted. |
| `/health`, `/ready` | liveness + readiness probes |

None of these endpoints are in the hot path. Downtime of the HTTP surface does not
affect ongoing webhook delivery.

## `create_app(settings: Settings | None = None) -> FastAPI`

Application factory. Builds the dependency graph in a lifespan handler, mounts the
routers, and starts the background tick loop. Pass an explicit `Settings` for tests;
otherwise it is read from the environment.

## `main() -> None`

CLI entry point (the `webhook-engine` console script). Runs uvicorn with the
factory. Reads host/port/debug from `Settings`.

## `Settings`

Pydantic-settings model, env prefix `WHE_`. `source_key` is required; everything
else has a default. See `.env.example`.

## `build_deps(settings) -> ServiceDeps`

Assembles the Redis connection, delivery store, subscription store, dispatcher,
SSRF guard, and engine. Returns a `ServiceDeps` dataclass placed on `app.state`.

## `RedisSubscriptionStore`

CRUD for subscriptions on Redis; also satisfies `SubscriptionReaderProtocol`
(via `for_event`) and `SecretReaderProtocol` (via `get_secret`). Use
`make_readers(store)` to get the two ABC adapters.

## Routes

| Router | Endpoints | Kind |
|---|---|---|
| `routes/health.py` | `GET /health/live`, `GET /health/ready` | probe |
| `routes/emit.py` | `POST /v1/emit` | optional ingress |
| `routes/subscriptions.py` | `POST /v1/subscriptions/create`, `GET /v1/subscriptions/list`, `GET /v1/subscriptions/get`, `POST /v1/subscriptions/delete`, `POST /v1/subscriptions/pause`, `POST /v1/subscriptions/resume` | admin |
| `routes/deliveries.py` | `GET /v1/deliveries/list`, `GET /v1/deliveries/get`, `POST /v1/deliveries/redeliver`, `POST /v1/subscriptions/ping` | admin |

HTTP discipline: `POST`/`GET` only; verb is in the path; mutations take the id in the
JSON body; reads take it in the query string.

## `run_tick_loop(engine, poll_interval_s)`

Coroutine driving `WebhookEngine.tick()` on a fixed cadence until cancelled.
Started in the app lifespan; cancelled and drained on shutdown.
