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

## Verifying signatures

Each outgoing delivery includes:

| Header | Meaning |
|---|---|
| `X-Webhook-Signature-256` | `hmac_sha256(secret, body)` hex digest |
| `X-Webhook-Timestamp` | UTC unix seconds at signing time |
| `X-Webhook-Id` | The delivery id |
| `X-Webhook-Event` | The event name |

Receiver pseudocode:

```python
import hashlib, hmac, time

def verify(secret: str, body: bytes, sig: str, ts: str, window_s: int = 300) -> bool:
    if abs(time.time() - int(ts)) > window_s:
        return False  # stale — replay protection
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, sig)
```

(Confirm the exact header names and signing input against `webhook-engine/signer.py`
for your version.)

## Configuration

All settings are environment variables prefixed `WHE_`. See `.env.example` for
the full list. The only required one is `WHE_SOURCE_KEY`.
