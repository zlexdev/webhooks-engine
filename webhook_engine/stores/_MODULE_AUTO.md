# stores/
<!-- AUTO-GENERATED. Do not edit. Run gen_module_auto.py to update. -->

> Persistence stores for webhook delivery — pick a backend at bootstrap.

## __init__.py
```
# Persistence stores for webhook delivery — pick a backend at bootstrap.


```

## base.py
```
# Store contracts — ABC alias + protocol re-exports.


```

## memory.py
```
# In-memory delivery store — default for tests and local dev.


cls MemoryWebhookDeliveryStore(BaseWebhookDeliveryStore)
  __init__() -> None
  async enqueue_many(records: list[DeliveryRecord) -> None
  async mark_sent(delivery_id: DeliveryId, attempt: AttemptRecord) -> None
  async schedule_retry(delivery_id: DeliveryId, fire_at: datetime, attempt: AttemptRecord) -> None
  async mark_dead(delivery_id: DeliveryId, attempt: AttemptRecord) -> None
  async append_attempt(delivery_id: DeliveryId, attempt: AttemptRecord) -> None
  async get(delivery_id: DeliveryId) -> DeliveryRecord | None
  async recent_for_subscription(sub_id: str, limit: int) -> list[DeliveryRecord]
  async claim_batch(now: datetime, lease_ttl_s: int, batch_size: int) -> list[DeliveryRecord]
  async reclaim_stale(now: datetime) -> int

```

## mongo.py
```
# MongoDB-backed webhook delivery store.

_CLAIMABLE = …

cls MongoWebhookDeliveryStore(BaseWebhookDeliveryStore)
  __init__(collection: AsyncIOMotorCollection[Any, attempts_limit: int = 20) -> None
  async ensure_schema() -> None
  async enqueue_many(records: list[DeliveryRecord) -> None
    # Insert delivery records, ignoring duplicates (idempotent by _id).
  async claim_batch(now: datetime, lease_ttl_s: int, batch_size: int) -> list[DeliveryRecord]
  async reclaim_stale(now: datetime) -> int
  async mark_sent(delivery_id: DeliveryId, attempt: AttemptRecord) -> None
    # Transition delivery to SENT and append the final attempt record.
  async schedule_retry(delivery_id: DeliveryId, fire_at: datetime, attempt: AttemptRecord) -> None
    # Move delivery to FAILED_RETRY with an updated fire_at and attempt count.
  async mark_dead(delivery_id: DeliveryId, attempt: AttemptRecord) -> None
    # Transition delivery to DEAD_LETTERED and record the final attempt.
  async append_attempt(delivery_id: DeliveryId, attempt: AttemptRecord) -> None
  async get(delivery_id: DeliveryId) -> DeliveryRecord | None
    # Fetch a single delivery record by id, or ``None`` if not found.
  async recent_for_subscription(sub_id: str, limit: int) -> list[DeliveryRecord]
    # Return the most recent *limit* deliveries for a subscription, newest first.

_ensure_utc(dt: datetime) -> datetime

_record_to_doc(record: DeliveryRecord) -> dict[str, Any]

_doc_to_record(doc: dict[str, Any) -> DeliveryRecord

_attempt_to_subdoc(attempt: AttemptRecord) -> dict[str, Any]

```

## pg.py
```
# Postgres-backed webhook stores — concrete side of the webhook persistence protocols.


cls WebhookDeliveryRepoProtocol(ABC)
  async enqueue_many(records: Sequence[DeliveryRecord) -> int
  async claim_batch() -> Sequence[DeliveryRecord]
  async reclaim_stale() -> int
  async mark_sent(delivery_id: str, attempt: AttemptRecord) -> None
  async schedule_retry(delivery_id: str, fire_at: datetime, attempt: AttemptRecord) -> None
  async mark_dead(delivery_id: str, attempt: AttemptRecord) -> None
  async bump_attempt(delivery_id: str, attempt: AttemptRecord) -> None
  async get_record(delivery_id: str) -> DeliveryRecord | None
  async recent_for_subscription(sub_id: str, limit: int) -> Sequence[DeliveryRecord]

cls WebhookAttemptRepoProtocol(ABC)
  async append(delivery_id: str, attempt: AttemptRecord) -> None

cls WebhookSubscriptionRepoProtocol(ABC)
  async for_event(event_name: str, tenant_id: str?) -> Sequence[SubscriptionSnapshot]

cls PgWebhookDeliveryStore(WebhookDeliveryStoreProtocol)
  __init__() -> None
  async enqueue_many(records: list[DeliveryRecord) -> None
  async claim_batch(now: datetime, lease_ttl_s: int, batch_size: int) -> list[DeliveryRecord]
  async reclaim_stale(now: datetime) -> int
  async mark_sent(delivery_id: DeliveryId, attempt: AttemptRecord) -> None
  async schedule_retry(delivery_id: DeliveryId, fire_at: datetime, attempt: AttemptRecord) -> None
  async mark_dead(delivery_id: DeliveryId, attempt: AttemptRecord) -> None
  async append_attempt(delivery_id: DeliveryId, attempt: AttemptRecord) -> None
  async get(delivery_id: DeliveryId) -> DeliveryRecord | None
  async recent_for_subscription(sub_id: str, limit: int) -> list[DeliveryRecord]

cls PgSubscriptionReader(SubscriptionReaderProtocol)
  __init__() -> None
  async for_event(event_name: str, tenant_id: str?) -> list[SubscriptionSnapshot]

```

## redis.py
```
# Redis-backed delivery store.

CLAIM_BATCH_LUA = …
RECLAIM_LUA = …

cls RedisWebhookDeliveryStore(BaseWebhookDeliveryStore)
  __init__(redis: Redis, config: WebhookStorageConfig) -> None
  async enqueue_many(records: list[DeliveryRecord) -> None
  async claim_batch(now: datetime, lease_ttl_s: int, batch_size: int) -> list[DeliveryRecord]
  async reclaim_stale(now: datetime) -> int
  async mark_sent(delivery_id: str, attempt: AttemptRecord) -> None
  async schedule_retry(delivery_id: str, fire_at: datetime, attempt: AttemptRecord) -> None
  async mark_dead(delivery_id: str, attempt: AttemptRecord) -> None
  async append_attempt(delivery_id: str, attempt: AttemptRecord) -> None
  async get(delivery_id: str) -> DeliveryRecord | None
  async recent_for_subscription(sub_id: str, limit: int) -> list[DeliveryRecord]

_to_ms(dt: datetime) -> int

_from_ms(ms: int) -> datetime

_record_to_hash(record: DeliveryRecord) -> dict[str, str]

_hash_to_record(h: dict[bytes | str, bytes | str) -> DeliveryRecord

_attempt_to_json(a: AttemptRecord) -> str

```
