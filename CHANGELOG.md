# Changelog

All notable changes to this project are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning: [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Ingestion layer** — producers write events to a shared DB; the engine reads and fans out.
  - `BaseEventStore` seam + `IncomingEvent` contract + `EventIngestStatus`.
  - `PgEventStore` (`FOR UPDATE SKIP LOCKED`) and `MongoEventStore` (`find_one_and_update`) backends.
  - `EventIngestor` + background ingest worker (claim → resolve subscriptions → enqueue → ack).
- **All-Postgres / all-Mongo profiles** — events, subscriptions, and the delivery queue in one DB:
  - `PgSubscriptionStore` / `MongoSubscriptionStore` (subscriptions live in the DB).
  - `MongoWebhookDeliveryStore` + concrete PG delivery repos (`make_pg_delivery_store`).
  - `WHE_BACKEND=pg|mongo` selects the profile; table/collection names overridable.
- **Operational endpoints** — delivery inspection, manual redelivery (replay), and test ping.
- **`verify_webhook`** — dependency-free receiver-side signature verification helper.
- **`scripts/install.py`** — one-command `.env` generator with unique timestamped table names.
- **`scripts/fix_imports.py`** — migrated the package from the `libs.webhooks` monorepo alias to a self-named `webhook_engine` package.

### Fixed
- **Signature verification was impossible**: `HmacSigner` signed over `event_id` but never emitted it in a header. Added the `Webhook-Event-Id` header so receivers can reconstruct the MAC.
- `MemoryWebhookDeliveryStore.reclaim_stale` reclaimed actively-dispatching records (used `fire_at` instead of a tracked lease expiry) — would have caused double-delivery.
- `RedisSubscriptionStore.pause`/`resume` always returned `True` even for a missing subscription (the PG/Mongo ports return a real boolean).
- Wrong `DeliveryStatus` members (`QUEUED`/`LEASED`) referenced in new code — corrected to `PENDING`/`IN_FLIGHT`.

### Changed
- `POST /v1/emit` now appends one `IncomingEvent` to the event store (same path as a DB producer) instead of duplicating fan-out logic.
- Dropped the `service` extra (FastAPI is a base dependency); added `mongo` extra.

## [0.1.0] — 2026-06-21

### Added

- Core library: `WebhookEngine`, `WebhookDispatcher`, `WebhookFanoutHandler`, `WebhookEventRegistry`.
- Storage contracts: `WebhookDeliveryStoreProtocol`, `SubscriptionReaderProtocol`, `SecretReaderProtocol`.
- Redis store (`RedisWebhookDeliveryStore`) with Lua atomic claim/reclaim and ZSET scheduling.
- PostgreSQL store (`PgWebhookDeliveryStore`) with `FOR UPDATE SKIP LOCKED` batch claims.
- In-memory store (`MemoryWebhookDeliveryStore`) for tests and local development.
- HMAC-SHA256 envelope signing with secret rotation support (`HmacSigner`).
- SSRF guard (`UrlSafetyValidator`) blocking RFC-1918 and link-local CIDRs.
- Opt-in event marker (`@webhook_event`) and `WebhookEventRegistry` catalog.
- Standalone microservice (`webhook-engine serve`): FastAPI app with `/v1/emit`, `/v1/subscriptions` CRUD, `/health`, `/ready`.
- One-liner deploy via `docker compose up` (Redis included).
- `libs/shared/` shims so the package is importable outside the original monorepo.
