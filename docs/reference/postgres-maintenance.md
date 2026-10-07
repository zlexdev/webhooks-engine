# PostgreSQL maintenance

`delivery_schema_sql(deliveries, attempts)` exports validated table DDL and indexes as a list of SQL statements for migration owners.
`ensure_delivery_schema` executes the same statements; schema definitions have one source.

`PgDeliveryRepo.dead_unnotified(limit)` locks unacknowledged dead deliveries in its session.
Enqueue the failure notification in that transaction, call `ack_failure_notification(delivery_id, now)`, then commit.
Schema SQL adds `failure_notified_at` and its partial dead/unnotified index.

`purge_sent(older_than, limit, attempts_table)` removes bounded sent deliveries and their attempts, retaining rows with recent attempts.
It locks selected deliveries with `SKIP LOCKED`; table names are validated SQL identifiers.

`WebhookEngine.tick()` includes lease-reclaim failures in `EngineTickStats.errors`, including ticks with no claimed deliveries.
Health observers can distinguish an empty successful tick from failed reclaim.

PostgreSQL migration and concurrent maintenance acceptance are required before production use.
