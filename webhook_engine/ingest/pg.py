"""Postgres-backed ingestion event store.

Uses SQLAlchemy async core (``AsyncEngine`` + ``text()``); no ORM models.
The table name is configurable so multiple tenants or environments can
share one PG cluster without collisions. Call ``ensure_schema()`` once
at startup before any reads or writes.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from libs.shared.logging import get_logger
from webhook_engine.ingest.base import BaseEventStore
from webhook_engine.ingest.events import IncomingEvent

__all__ = ["PgEventStore"]

logger = get_logger("webhooks.ingest.pg")


class PgEventStore(BaseEventStore):
    """Postgres-backed durable queue for :class:`~webhook_engine.ingest.events.IncomingEvent`.

    Args:
        engine: A live :class:`~sqlalchemy.ext.asyncio.AsyncEngine` pointed at
            the target database.
        table: Table name. Typically ``wh_events_<tenant_hex>``; must be a safe
            identifier (the caller is responsible for sanitisation before
            construction).
        lock_ttl_s: Seconds after which a CLAIMED lease is considered stale
            and eligible for reclamation. Defaults to 30.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        table: str,
        lock_ttl_s: int = 30,
    ) -> None:
        self._engine = engine
        self._table = table
        self._lock_ttl_s = lock_ttl_s
        self._closed = False

    async def ensure_schema(self) -> None:
        """CREATE TABLE IF NOT EXISTS and supporting index.

        Safe to call on every startup — all DDL is idempotent.
        """
        create_table = text(f"""
            CREATE TABLE IF NOT EXISTS {self._table} (
                event_id       TEXT        NOT NULL PRIMARY KEY,
                event          TEXT        NOT NULL,
                tenant_id      TEXT,
                schema_version TEXT        NOT NULL,
                data           JSONB       NOT NULL,
                status         TEXT        NOT NULL DEFAULT 'pending',
                locked_at      TIMESTAMPTZ,
                error          TEXT,
                created_at     TIMESTAMPTZ NOT NULL
            )
        """)
        create_index = text(f"""
            CREATE INDEX IF NOT EXISTS {self._table}_status_created_at_idx
                ON {self._table} (status, created_at)
        """)
        async with self._engine.begin() as conn:
            await conn.execute(create_table)
            await conn.execute(create_index)
        logger.info("pg_event_store.schema_ensured", table=self._table)

    async def append(self, event: IncomingEvent) -> None:
        """Persist *event* as PENDING.  Duplicate ``event_id`` is a no-op."""
        created_at = event.created_at or datetime.now(UTC)
        stmt = text(f"""
            INSERT INTO {self._table}
                (event_id, event, tenant_id, schema_version, data, status, created_at)
            VALUES
                (:event_id, :event, :tenant_id, :schema_version, CAST(:data AS jsonb),
                 'pending', :created_at)
            ON CONFLICT (event_id) DO NOTHING
        """)
        async with self._engine.begin() as conn:
            await conn.execute(
                stmt,
                {
                    "event_id": event.event_id,
                    "event": event.event,
                    "tenant_id": event.tenant_id,
                    "schema_version": event.schema_version,
                    "data": json.dumps(event.data),
                    "created_at": created_at,
                },
            )
        logger.debug(
            "pg_event_store.appended",
            event_id=event.event_id,
            event_name=event.event,
        )

    async def append_many(self, events: list[IncomingEvent]) -> None:
        """Persist a batch as PENDING in one round trip (executemany).

        Reuses the single-row statement with a parameter list, so
        ``ON CONFLICT (event_id) DO NOTHING`` still dedupes each row. Empty
        input is a no-op.
        """
        if not events:
            return
        stmt = text(f"""
            INSERT INTO {self._table}
                (event_id, event, tenant_id, schema_version, data, status, created_at)
            VALUES
                (:event_id, :event, :tenant_id, :schema_version, CAST(:data AS jsonb),
                 'pending', :created_at)
            ON CONFLICT (event_id) DO NOTHING
        """)
        params = [
            {
                "event_id": e.event_id,
                "event": e.event,
                "tenant_id": e.tenant_id,
                "schema_version": e.schema_version,
                "data": json.dumps(e.data),
                "created_at": e.created_at or datetime.now(UTC),
            }
            for e in events
        ]
        async with self._engine.begin() as conn:
            await conn.execute(stmt, params)
        logger.debug("pg_event_store.appended_many", count=len(events), table=self._table)

    async def claim_events(
        self, now: datetime, lease_ttl_s: int, batch_size: int
    ) -> list[IncomingEvent]:
        """Atomically lease up to *batch_size* PENDING events.

        Uses ``FOR UPDATE SKIP LOCKED`` so concurrent engine replicas never
        claim the same row.  The SELECT and UPDATE run in a single transaction.
        """
        _ = lease_ttl_s
        select_stmt = text(f"""
            SELECT event_id, event, tenant_id, schema_version, data, created_at
            FROM {self._table}
            WHERE status = 'pending'
            ORDER BY created_at
            LIMIT :batch_size
            FOR UPDATE SKIP LOCKED
        """)
        update_stmt = text(f"""
            UPDATE {self._table}
            SET status = 'claimed', locked_at = :now
            WHERE event_id = ANY(:ids)
        """)
        async with self._engine.begin() as conn:
            result = await conn.execute(select_stmt, {"batch_size": batch_size})
            rows = result.fetchall()
            if not rows:
                return []
            ids = [row.event_id for row in rows]
            await conn.execute(update_stmt, {"now": now, "ids": ids})

        events: list[IncomingEvent] = []
        for row in rows:
            raw_data: Any = row.data
            data: dict[str, Any] = raw_data if isinstance(raw_data, dict) else json.loads(raw_data)
            created_at: datetime = row.created_at
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=UTC)
            events.append(
                IncomingEvent(
                    event_id=row.event_id,
                    event=row.event,
                    tenant_id=row.tenant_id,
                    schema_version=row.schema_version,
                    data=data,
                    created_at=created_at,
                )
            )
        logger.debug(
            "pg_event_store.claimed",
            count=len(events),
            table=self._table,
        )
        return events

    async def ack(self, event_ids: list[str]) -> None:
        if not event_ids:
            return
        stmt = text(f"""
            UPDATE {self._table}
            SET status = 'done'
            WHERE event_id = ANY(:ids)
        """)
        async with self._engine.begin() as conn:
            await conn.execute(stmt, {"ids": event_ids})
        logger.debug(
            "pg_event_store.acked",
            count=len(event_ids),
            table=self._table,
        )

    async def mark_failed(self, event_id: str, error: str) -> None:
        """Park *event_id* as FAILED with *error* detail."""
        stmt = text(f"""
            UPDATE {self._table}
            SET status = 'failed', error = :error
            WHERE event_id = :event_id
        """)
        async with self._engine.begin() as conn:
            await conn.execute(stmt, {"event_id": event_id, "error": error})
        logger.warning(
            "pg_event_store.marked_failed",
            event_id=event_id,
            error=error,
            table=self._table,
        )

    async def reclaim_stale(self, now: datetime) -> int:
        """Return CLAIMED events whose lease expired back to PENDING.

        Returns the number of rows reclaimed.
        """
        cutoff = now - timedelta(seconds=self._lock_ttl_s)
        stmt = text(f"""
            UPDATE {self._table}
            SET status = 'pending', locked_at = NULL
            WHERE status = 'claimed'
              AND locked_at < :cutoff
        """)
        async with self._engine.begin() as conn:
            result = await conn.execute(stmt, {"cutoff": cutoff})
            count: int = result.rowcount
        if count:
            logger.info(
                "pg_event_store.reclaimed_stale",
                count=count,
                table=self._table,
            )
        return count

    async def aclose(self) -> None:
        """Dispose the engine's connection pool.  Idempotent."""
        if self._closed:
            return
        self._closed = True
        await self._engine.dispose()
        logger.info("pg_event_store.closed", table=self._table)
