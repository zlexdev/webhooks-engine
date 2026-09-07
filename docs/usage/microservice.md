# Microservice & HTTP API

The service wraps the core library in a FastAPI app with a background tick loop.
It is the adapter layer — all business logic lives in the library; these routes
only authenticate, validate, and delegate.

## Architecture

```
stream service ──POST /v1/emit──► [ emit route ] ──enqueue──► Redis delivery queue
                                                                     │
                                              background tick loop ◄──┘
                                                     │ claim batch
                                                     ▼
                                              WebhookDispatcher ──signed POST──► subscriber URL
```

- **emit route** resolves subscriptions, runs the SSRF guard, enqueues delivery records.
- **tick loop** (`service/worker.py`) drives `WebhookEngine.tick()` every `WHE_POLL_INTERVAL_S`.
- **dispatcher** signs and POSTs each claimed delivery, classifying the outcome.

## Authentication

Every endpoint requires the `X-Source-Key` header to match `WHE_SOURCE_KEY`.
This authenticates *trusted internal callers* (your stream services), not the
webhook receivers — receivers authenticate deliveries via the HMAC signature.

## Endpoints

POST/GET only — the path carries the verb, never the HTTP method. Reads take
their id in the query string; mutations take it in the JSON body. Collection
reads return a `Page` envelope (`items` / `total` / `limit` / `offset`).

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/emit` | Emit an event; fan out to all active subscriptions |
| `POST` | `/v1/emit/batch` | Append up to 50 events in one write, each idempotent on its own `event_id` |
| `POST` | `/v1/subscriptions/create` | Create a subscription (returns the secret once) |
| `GET` | `/v1/subscriptions/list?owner_id=&limit=&offset=` | List an owner's subscriptions |
| `GET` | `/v1/subscriptions/get?id=` | Fetch one subscription |
| `POST` | `/v1/subscriptions/delete` | Delete a subscription (body: `{"id": …}`) |
| `POST` | `/v1/subscriptions/pause` | Stop delivery without deleting (body: `{"id": …}`) |
| `POST` | `/v1/subscriptions/resume` | Resume a paused subscription (body: `{"id": …}`) |
| `POST` | `/v1/subscriptions/ping` | Send a test delivery (body: `{"subscription_id": …}`) |
| `GET` | `/v1/deliveries/list?subscription_id=&limit=&offset=` | Recent deliveries for a subscription |
| `GET` | `/v1/deliveries/get?id=` | Inspect one delivery |
| `POST` | `/v1/deliveries/redeliver` | Replay a past delivery (body: `{"id": …}`) |
| `GET` | `/health` | Liveness (always 200 if process is up) |
| `GET` | `/ready` | Readiness (200 only if the backend is reachable) |

## Emit payload

```json
{
  "event": "order.paid",
  "event_id": "optional-idempotency-key",
  "tenant_id": "acme",
  "schema_version": "1.0",
  "data": { "order_id": 42 }
}
```

`event_id` defaults to a random hex if omitted. Supplying your own makes the
emit idempotent — safe to retry on network failure.

## Calling it from Python

`webhook_engine.client` ships with the package — same repo, same commit as the
routes above, so it cannot drift from them.

```python
from webhook_engine import EmitEvent, WebhookEngineClient

async with WebhookEngineClient(base_url="http://engine:8080", source_key=KEY) as client:
    await client.emit(EmitEvent(event="order.paid", tenant_id="acme", data={"order_id": 42}))
```

Errors are this package's own exceptions where the meaning matches in-process
(`SubscriptionNotFound`, `DeliveryNotFound`, `InvalidWebhookTarget` — the last
carrying its `InvalidTargetReason`). The four that exist only over HTTP are
separate types because they need opposite handling: `WebhookAuthError` (wrong
key — retrying cannot fix it), `WebhookServiceError` (answered; `status` decides
whether to retry), `WebhookTransportError` (never arrived), `WebhookProtocolError`
(a 2xx that is not this service).

## Verifying signatures

`verify_webhook()` ships with the package — receivers should call it rather
than reimplement the pseudocode below.

Each outgoing delivery includes:

| Header | Meaning |
|---|---|
| `Webhook-Signature` | `<version>=<hex mac>`, e.g. `v1=3f9c…` |
| `Webhook-Timestamp` | UTC unix seconds at signing time |
| `Webhook-Id` | The delivery id |
| `Webhook-Event` | The event name |
| `Webhook-Event-Id` | The event id — **part of the signed message**, so a receiver needs it to rebuild the MAC |

**No `X-` prefix on any of them, and the signature is not a bare digest.** Both
were wrong here until 07.09.2026, and a receiver following the old text failed
every verification.

**The signed message is not the body.** It is
`f"{timestamp}.{event_id}.{sha256_hex(body)}"` (`signer.py`), which is what
binds the timestamp into the MAC and makes the replay window enforceable.

Use the shipped verifier rather than reimplementing it:

```python
from webhook_engine import verify_webhook
```

**`Webhook-Event` is NOT covered by the signature.** Route on the event name in
the *body*; treat the header as a routing hint only.

## Configuration

All settings are environment variables prefixed `WHE_`. See `.env.example` for
the full list. The only required one is `WHE_SOURCE_KEY`.
