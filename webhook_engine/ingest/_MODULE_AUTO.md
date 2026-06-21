# ingest/
<!-- AUTO-GENERATED. Do not edit. Run gen_module_auto.py to update. -->

> Ingestion layer — producers write events to a shared DB; the engine reads.

## __init__.py
```
# Ingestion layer — producers write events to a shared DB; the engine reads.


```

## base.py
```
# Ingestion seam — the durable producer→consumer event queue.


cls BaseEventStore(ABC)
  async append(event: IncomingEvent) -> None
  async claim_events(now: datetime, lease_ttl_s: int, batch_size: int) -> list[IncomingEvent]
  async ack(event_ids: list[str) -> None
    # Mark events DONE once their deliveries are enqueued.
  async mark_failed(event_id: str, error: str) -> None
    # Park a permanently un-ingestable event (e.g. malformed payload).
  async reclaim_stale(now: datetime) -> int
    # Return CLAIMED events whose lease expired to PENDING. Returns count.
  async aclose() -> None
    # Release the backend connection. Idempotent.

```

## events.py
```
# Producer-facing event contract.


cls EventIngestStatus(StrEnum): PENDING, CLAIMED, DONE, FAILED
  # Lifecycle of an ingested event row in the shared store.

cls IncomingEvent: event_id: str, event: str, data: dict[str, Any], tenant_id: str | None, schema_version: str, created_at: datetime | None

```

## ingestor.py
```
# Claimed-event → queued-delivery fan-out.


cls IngestTickStats: claimed: int, fanned_out: int, failed: int

cls EventIngestor
  __init__() -> None
  async tick() -> IngestTickStats

```

## memory.py
```
# In-memory event store — the dependency-free backend for dev and tests.


cls _Row: event: IncomingEvent, status: EventIngestStatus, lease_until: datetime | None

cls MemoryEventStore(BaseEventStore): _rows: dict[str, _Row]

```

## mongo.py
```
# MongoDB-backed ingestion event store.


cls MongoEventStore(BaseEventStore)
  __init__(collection: AsyncIOMotorCollection[Any, lock_ttl_s: int = 30) -> None
  async ensure_schema() -> None
  async append(event: IncomingEvent) -> None
    # Persist *event* as PENDING.  Duplicate ``event_id`` is a no-op.
  async claim_events(now: datetime, lease_ttl_s: int, batch_size: int) -> list[IncomingEvent]
  async ack(event_ids: list[str) -> None
  async mark_failed(event_id: str, error: str) -> None
    # Park *event_id* as FAILED with *error* detail.
  async reclaim_stale(now: datetime) -> int
  async aclose() -> None

```

## pg.py
```
# Postgres-backed ingestion event store.


cls PgEventStore(BaseEventStore)
  __init__(engine: AsyncEngine, table: str, lock_ttl_s: int = 30) -> None
  async ensure_schema() -> None
  async append(event: IncomingEvent) -> None
    # Persist *event* as PENDING.  Duplicate ``event_id`` is a no-op.
  async claim_events(now: datetime, lease_ttl_s: int, batch_size: int) -> list[IncomingEvent]
  async ack(event_ids: list[str) -> None
  async mark_failed(event_id: str, error: str) -> None
    # Park *event_id* as FAILED with *error* detail.
  async reclaim_stale(now: datetime) -> int
  async aclose() -> None
    # Dispose the engine's connection pool.  Idempotent.

```
