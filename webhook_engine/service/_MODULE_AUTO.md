# service/
<!-- AUTO-GENERATED. Do not edit. Run gen_module_auto.py to update. -->

> Standalone HTTP microservice layer — optional, behind the ``service`` extra.

## Submodules

- [`routes/`](routes\_MODULE_AUTO.md) (4 py, 8 cls, 19 fn)

## __init__.py
```
# Standalone HTTP microservice layer — optional, behind the ``service`` extra.


```

## app.py
```
# FastAPI application factory and CLI entry point.


create_app(settings: Settings? = None) -> FastAPI

main() -> None

```

## delivery_admin.py
```
# Operational webhook management — inspection, manual redelivery, test ping.

PING_EVENT = 'webhook.ping'

cls DeliveryAdminService
  __init__(store: WebhookDeliveryStoreProtocol, sub_store: SubscriptionAdminStore, ssrf_guard: UrlSafetyValidator) -> None
  async list_for_subscription(sub_id: str, limit: int) -> list[DeliveryRecord]
  async get(delivery_id: str) -> DeliveryRecord
  async redeliver(delivery_id: str) -> DeliveryRecord
  async ping(sub_id: str) -> DeliveryRecord

```

## deps.py
```
# Dependency graph wiring for the FastAPI service.


cls SubscriptionAdminStore(Protocol)
  # Structural type for the admin surface shared by the PG / Mongo stores.
  async for_event() -> list[SubscriptionSnapshot]
  async get_secret(subscription_id: str) -> SecretMaterial
  async create(owner_id: str, url: str, events: list[str, secret: str? = None) -> Subscription
  async get(sub_id: str) -> Subscription | None
  async list_for_owner(owner_id: str) -> list[Subscription]
  async delete(sub_id: str) -> bool
  async pause(sub_id: str) -> bool
  async resume(sub_id: str) -> bool

cls _Backends: event_store: BaseEventStore, sub_store: SubscriptionAdminStore, sub_reader: SubscriptionReaderProtocol, secret_reader: SecretReaderProtocol, delivery_store: WebhookDeliveryStoreProtocol, aclose: Callable[[], Awaitable[None]]

cls ServiceDeps: event_store: BaseEventStore, delivery_store: WebhookDeliveryStoreProtocol, sub_store: SubscriptionAdminStore, dispatch_engine: WebhookEngine, ingestor: EventIngestor, ssrf_guard: UrlSafetyValidator, settings: Settings, _aclose: Callable[[], Awaitable[None]]

async build_deps(settings: Settings) -> ServiceDeps

async _build_pg(settings: Settings) -> _Backends

async _build_mongo(settings: Settings) -> _Backends

async _build_memory(settings: Settings) -> _Backends

get_deps(request: Request) -> ServiceDeps

```

## ingest_worker.py
```
# Background ingestion loop — drives :meth:`EventIngestor.tick`.


async run_ingest_loop(ingestor: EventIngestor, poll_interval_s: float) -> None

```

## pg_repos.py
```
# Postgres-backed repo implementations for webhook delivery persistence.

_SAFE_IDENTIFIER_RE = re.compile('^[a-zA-Z_][a-zA-Z0-9_]{0,62}$')

cls PgDeliveryRepo(WebhookDeliveryRepoProtocol)
  __init__(session: AsyncSession, table: str) -> None
  async enqueue_many(records: Sequence[DeliveryRecord) -> int
    # Bulk-insert *records* as PENDING; duplicate delivery_id is a no-op.
  async claim_batch() -> Sequence[DeliveryRecord]
  async reclaim_stale() -> int
    # Return expired in_flight rows (lease_expiry < now) back to pending.
  async mark_sent(delivery_id: str, attempt: AttemptRecord) -> None
    # Transition *delivery_id* to SENT (terminal).
  async schedule_retry(delivery_id: str, fire_at: datetime, attempt: AttemptRecord) -> None
    # Transition *delivery_id* to FAILED_RETRY, scheduled at *fire_at*.
  async mark_dead(delivery_id: str, attempt: AttemptRecord) -> None
    # Transition *delivery_id* to DEAD_LETTERED (terminal).
  async bump_attempt(delivery_id: str, attempt: AttemptRecord) -> None
    # Increment the attempts counter without changing status.
  async get_record(delivery_id: str) -> DeliveryRecord | None
    # Fetch a single delivery row by primary key, or ``None``.
  async recent_for_subscription(sub_id: str, limit: int) -> Sequence[DeliveryRecord]
    # Return the most-recent *limit* deliveries for *sub_id*, newest first.

cls PgAttemptRepo(WebhookAttemptRepoProtocol)
  __init__(session: AsyncSession, table: str) -> None
  async append(delivery_id: str, attempt: AttemptRecord) -> None
    # Insert one attempt row; duplicate (delivery_id, attempt) is a no-op.

_validate_table_name(name: str) -> str
  # Raise ``ValueError`` if *name* is not a safe SQL identifier.

_ensure_utc(dt: datetime) -> datetime

_row_to_delivery(row: object) -> DeliveryRecord
  # Map a SQLAlchemy ``Row`` to :class:`DeliveryRecord`.

async ensure_delivery_schema(engine: AsyncEngine, deliveries_table: str, attempts_table: str) -> None

make_pg_delivery_store(engine: AsyncEngine, deliveries_table: str, attempts_table: str) -> PgWebhookDeliveryStore

```

## schemas.py
```
# Shared response envelopes for the HTTP surface.


cls Page(BaseModel)

```

## settings.py
```
# Service settings from environment (prefix ``WHE_``).


cls EventBackend(StrEnum): PG, MONGO, MEMORY

cls Settings(BaseSettings)

```

## subscription_mongo.py
```
# MongoDB-backed subscription store (Motor async driver).


cls MongoSubscriptionStore
  # CRUD for subscriptions backed by a MongoDB collection (Motor).
  __init__(collection: AsyncIOMotorCollection) -> None
  async ensure_schema() -> None
    # Create indexes on the collection (idempotent).
  async for_event(event_name: str, _tenant_id: str?) -> list[SubscriptionSnapshot]
  async get_secret(subscription_id: str) -> SecretMaterial
  invalidate_secret(_subscription_id: str) -> None
    # No-op — Mongo store has no in-process secret cache.
  async create(owner_id: str, url: str, events: list[str, secret: str? = None) -> Subscription
  async get(sub_id: str) -> Subscription | None
  async list_for_owner(owner_id: str) -> list[Subscription]
  async delete(sub_id: str) -> bool
  async pause(sub_id: str) -> bool
  async resume(sub_id: str) -> bool

cls _MongoSubscriptionReader(SubscriptionReaderProtocol)
  __init__(store: MongoSubscriptionStore) -> None
  async for_event(event_name: str, tenant_id: str?) -> list[SubscriptionSnapshot]

cls _MongoSecretReader(SecretReaderProtocol)
  __init__(store: MongoSubscriptionStore) -> None
  async get(subscription_id: str) -> SecretMaterial
  invalidate(subscription_id: str) -> None

make_mongo_readers(store: MongoSubscriptionStore) -> tuple[SubscriptionReaderProtocol, SecretReaderProtocol]

_doc_to_sub(doc: dict[str, Any) -> Subscription
  # Convert a Motor document to a ``Subscription`` dataclass.

```

## subscription_pg.py
```
# PostgreSQL-backed subscription store.

_DEFAULT_TABLE = 'wh_subscriptions'
_IDENT_RE = re.compile('^[A-Za-z_][A-Za-z0-9_$]{0,62}$')

cls PgSubscriptionStore
  __init__(engine: AsyncEngine, table: str = _DEFAULT_TABLE) -> None
  async ensure_schema() -> None
    # Create the subscriptions table and indexes if they do not exist.
  async for_event(event_name: str, tenant_id: str?) -> list[SubscriptionSnapshot]
  async get_secret(subscription_id: str) -> SecretMaterial
  invalidate_secret(subscription_id: str) -> None
    # No-op — PG store has no in-process secret cache.
  async create(owner_id: str, url: str, events: list[str, secret: str? = None) -> Subscription
  async get(sub_id: str) -> Subscription | None
  async list_for_owner(owner_id: str) -> list[Subscription]
  async delete(sub_id: str) -> bool
  async pause(sub_id: str) -> bool
  async resume(sub_id: str) -> bool

cls _PgSubscriptionReader(SubscriptionReaderProtocol)
  __init__(store: PgSubscriptionStore) -> None
  async for_event(event_name: str, tenant_id: str?) -> list[SubscriptionSnapshot]

cls _PgSecretReader(SecretReaderProtocol)
  __init__(store: PgSubscriptionStore) -> None
  async get(subscription_id: str) -> SecretMaterial
  invalidate(subscription_id: str) -> None

_validate_identifier(name: str) -> str
  # Return *name* unchanged or raise ``ValueError`` if it is not a safe PG identifier.

make_pg_readers(store: PgSubscriptionStore) -> tuple[SubscriptionReaderProtocol, SecretReaderProtocol]

_row_to_sub(row: object) -> Subscription
  # Convert a SQLAlchemy ``RowMapping`` to a ``Subscription`` dataclass.

```

## subscription_store.py
```
# Redis-backed subscription management + protocol implementations.

_PREFIX = 'whe'

cls Subscription: id: str, owner_id: str, url: str, secret: str, events: tuple[str, ...], status: SubscriptionStatus, created_at: datetime, resolved_ip: str

cls RedisSubscriptionStore
  # CRUD for subscriptions; doubles as the reader and secret resolver.
  __init__(redis: Redis[bytes) -> None
  async for_event(event_name: str, _tenant_id: str?) -> list[SubscriptionSnapshot]
  async get_secret(subscription_id: str) -> SecretMaterial
  invalidate_secret(_subscription_id: str) -> None
  async create(owner_id: str, url: str, events: list[str, secret: str? = None) -> Subscription
  async get(sub_id: str) -> Subscription | None
  async list_for_owner(owner_id: str) -> list[Subscription]
  async delete(sub_id: str) -> bool
  async pause(sub_id: str) -> bool
  async resume(sub_id: str) -> bool

cls _SubscriptionReader(SubscriptionReaderProtocol)
  __init__(store: RedisSubscriptionStore) -> None
  async for_event(event_name: str, tenant_id: str?) -> list[SubscriptionSnapshot]

cls _SecretReader(SecretReaderProtocol)
  __init__(store: RedisSubscriptionStore) -> None
  async get(subscription_id: str) -> SecretMaterial
  invalidate(subscription_id: str) -> None

make_readers(store: RedisSubscriptionStore) -> tuple[SubscriptionReaderProtocol, SecretReaderProtocol]

async _resolve_ip(url: str) -> str

_row_to_sub(row: dict[bytes, bytes) -> Subscription

```

## worker.py
```
# Background tick loop — runs inside the FastAPI process.


async run_tick_loop(engine: WebhookEngine, poll_interval_s: float) -> None
  # Drive the engine tick loop until cancelled.

```
