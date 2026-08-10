# Changelog

All notable changes to this project are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning: [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] — 2026-08-10

First tagged release — `0.1.0` was documented but never tagged.

### Breaking
- `libs.shared.logging` / `libs.shared.time` are gone; the shims live at
  `webhook_engine._shared.*` and the `libs` package is no longer installed.
  Anything importing `libs.shared` breaks at import. Nothing else did, or
  should have — it is private now, and named so.
- `HmacSigner.verify` / `verify_webhook` now reject a signature whose version
  label is missing or not `v1`. A correctly produced `v1=<mac>` is unaffected;
  a bare mac with no label used to pass and no longer does.

### Added
- **`SignerProtocol`** — the signature scheme is substitutable; `HmacSigner` stays the default. A host keeps the format its receivers already verify (GitHub/Stripe-style `sha256=<hex>`, a legacy scheme, a future `v2`) instead of subclassing `HmacSigner`. Headers travel as a structural `SignedHeaders` (anything with `as_dict()`), so a custom signer names its own headers and inherits nothing from this package. `verify_webhook(signer=...)` mirrors it on the receiving side.
- **`WebhookEngine(on_outcome=...)`** — per-delivery `DispatchOutcome` (including what the receiver answered, `attempt.response_snippet`) instead of counters only. Runs after the delivery is committed and outside the concurrency gate, under `dispatch.on_outcome_timeout_s`; a callback that hangs or raises is logged and cannot affect the delivery.
- **`WebhookSecurityConfig.trusted_private_hosts`** — exempt a *named* host from `blocked_cidrs` (`WHE_TRUSTED_PRIVATE_HOSTS`), so a receiver on a Docker/Kubernetes network is reachable without deleting a whole range from the deny-list. Entries are exact `host` or `host:port`; no wildcards, because the target URL is tenant-supplied.
- **`TargetResolverProtocol`** — optional per-attempt DNS re-resolution. Pinning stays the default; with a resolver, a receiver that moved mid-retry-ladder is still reachable. The returned address passes the same `check_ip` and is the one actually POSTed to.
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

### Security
- `0.0.0.0` and `::` were not in `blocked_cidrs` — both reach the local machine on Linux and neither falls inside the loopback ranges. A subscription pointing at `http://0.0.0.0:<port>/` reached the worker's own services with no configuration needed.
- `HmacSigner.verify` accepted any version label: `v99=<valid v1 mac>` verified as `v1`. The label is now checked, in both `HmacSigner.verify` and `verify_webhook`; a bare mac with no label is rejected rather than accepted.
- The `Host` header sent alongside a pinned IP was set with `setdefault`, so a signer's headers could override it and redirect the request at a vhost of its choosing. It is now assigned by the transport.

### Fixed
- `HmacSigner.verify` matched header names case-sensitively and so rejected every valid delivery from a real HTTP server, which lower-cases them.
- `redis[asyncio]` requested an extra that no longer exists (`redis>=4.2` ships the async client in the base package) — resolvers warned, and warnings become errors.
- `docs/usage/library.md` listed `SubscriptionReaderProtocol` as required; neither `WebhookDispatcher` nor `WebhookEngine` takes one — it is needed only for `[bus]` fan-out. The same page's `UrlSafetyValidator(...)` example passed the wrong argument type.
- **Signature verification was impossible**: `HmacSigner` signed over `event_id` but never emitted it in a header. Added the `Webhook-Event-Id` header so receivers can reconstruct the MAC.
- `MemoryWebhookDeliveryStore.reclaim_stale` reclaimed actively-dispatching records (used `fire_at` instead of a tracked lease expiry) — would have caused double-delivery.
- `RedisSubscriptionStore.pause`/`resume` always returned `True` even for a missing subscription (the PG/Mongo ports return a real boolean).
- Wrong `DeliveryStatus` members (`QUEUED`/`LEASED`) referenced in new code — corrected to `PENDING`/`IN_FLIGHT`.

### Changed
- The shared logging/time shims moved from a top-level `libs.shared` to `webhook_engine._shared`, and `libs` is no longer installed. The distribution claimed a name generic enough to collide with a consumer's own `libs/`, and such a collision surfaces as a wrong import rather than an install error.
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
