# Quickstart

`webhook-engine` runs two ways: as an embedded **library** inside your own async
service, or as a standalone **microservice** that other services feed events to.
This page covers the microservice path — the fastest way to see it work.

## 1. Run it

The repo ships a `docker-compose.yml` that starts the engine and a Redis instance:

```bash
WHE_SOURCE_KEY=$(openssl rand -hex 32) docker compose up -d
```

`WHE_SOURCE_KEY` is the shared secret stream services present in the
`X-Source-Key` header. It is the only required configuration. The service listens
on `:8080`; interactive OpenAPI docs are at `http://localhost:8080/docs`.

Without Docker:

```bash
pip install -e ".[service]"
WHE_SOURCE_KEY=secret123 webhook-engine
```

## 2. Register a subscription

A subscription maps an owner + target URL to a set of event names.

```bash
curl -X POST localhost:8080/v1/subscriptions/create \
  -H 'X-Source-Key: secret123' \
  -H 'Content-Type: application/json' \
  -d '{
    "owner_id": "acme",
    "url": "https://acme.example/webhooks",
    "events": ["order.paid", "order.refunded"]
  }'
```

The response includes a `secret` — **store it**. It is returned only once and is
the key the engine uses to sign deliveries to this subscriber.

## 3. Emit an event

Any stream service POSTs an event; the engine looks up every active subscription
for that event name and queues a signed delivery for each.

```bash
curl -X POST localhost:8080/v1/emit \
  -H 'X-Source-Key: secret123' \
  -H 'Content-Type: application/json' \
  -d '{
    "event": "order.paid",
    "tenant_id": "acme",
    "data": {"order_id": 42, "amount": "19.99"}
  }'
```

Response (`202 Accepted`):

```json
{"queued": 1, "event_id": "9f...c2", "delivery_ids": ["3a...e1"]}
```

`event_id` is the idempotency key. POSTing the same `event_id` again will not
double-deliver to a subscription that already received it.

## 4. Verify the signature on the receiving end

Each delivery carries `X-Webhook-Signature-256` and `X-Webhook-Timestamp`
headers. Recompute the HMAC over the raw body with your stored secret and
compare in constant time. See [microservice.md](microservice.md#verifying-signatures).

## Next steps

- Embed it in your own worker loop → [library.md](library.md)
- HTTP API reference + signature spec → [microservice.md](microservice.md)
- Production deployment → [../runbooks/deploy.md](../runbooks/deploy.md)
