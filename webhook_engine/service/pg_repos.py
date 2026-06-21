"""Postgres-backed repo implementations for webhook delivery persistence.

Uses SQLAlchemy async core (``AsyncEngine`` + ``text()``); no ORM models.
Table names are configurable and validated against a safe-identifier regex
to prevent SQL injection.

Call ``ensure_delivery_schema()`` once at startup before any reads or writes.
Use ``make_pg_delivery_store()`` to wire everything together in one call.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from sqlalchemy import text
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from libs.shared.logging import get_logger
from webhook_engine.enums import DeliveryStatus
from webhook_engine.stores.pg import (
    PgWebhookDeliveryStore,
    WebhookAttemptRepoProtocol,
    WebhookDeliveryRepoProtocol,
)
from webhook_engine.types import AttemptRecord, DeliveryRecord

__all__ = [
    "PgAttemptRepo",
    "PgDeliveryRepo",
    "ensure_delivery_schema",
    "make_pg_delivery_store",
]

logger = get_logger("webhooks.stores.pg_repos")


_SAFE_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$")


def _validate_table_name(name: str) -> str:
    """Raise ``ValueError`` if *name* is not a safe SQL identifier."""
    if not _SAFE_IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Table name {name!r} is not a safe SQL identifier. "
            "Use only letters, digits, and underscores; must start with a "
            "letter or underscore; max 63 chars."
        )
    return name


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


def _row_to_delivery(row: object) -> DeliveryRecord:
    """Map a SQLAlchemy ``Row`` to :class:`DeliveryRecord`."""
    return DeliveryRecord(
        delivery_id=row.delivery_id,  # type: ignore[attr-defined]
        subscription_id=row.subscription_id,  # type: ignore[attr-defined]
        owner_id=row.owner_id,  # type: ignore[attr-defined]
        event_name=row.event_name,  # type: ignore[attr-defined]
        event_id=row.event_id,  # type: ignore[attr-defined]
        schema_version=row.schema_version,  # type: ignore[attr-defined]
        tenant_id=row.tenant_id,  # type: ignore[attr-defined]
        data_json=bytes(row.data_json) if row.data_json is not None else b"",  # type: ignore[attr-defined]
        emitted_at=_ensure_utc(row.emitted_at),  # type: ignore[attr-defined]
        target_url=row.target_url,  # type: ignore[attr-defined]
        resolved_ip=row.resolved_ip,  # type: ignore[attr-defined]
        redelivery_of=row.redelivery_of,  # type: ignore[attr-defined]
        attempts=row.attempts,  # type: ignore[attr-defined]
        fire_at=_ensure_utc(row.fire_at),  # type: ignore[attr-defined]
        created_at=_ensure_utc(row.created_at),  # type: ignore[attr-defined]
        status=DeliveryStatus(row.status),  # type: ignore[attr-defined]
    )


class PgDeliveryRepo(WebhookDeliveryRepoProtocol):
    """SQLAlchemy-async ``text()`` implementation of :class:`WebhookDeliveryRepoProtocol`.

    Args:
        session: Active :class:`~sqlalchemy.ext.asyncio.AsyncSession`.
            The caller (``PgWebhookDeliveryStore``) owns the transaction
            lifecycle; this repo never commits or rolls back.
        table: Validated SQL identifier for the deliveries table.
    """

    def __init__(self, session: AsyncSession, table: str) -> None:
        self._s = session
        self._t = table

    async def enqueue_many(self, records: Sequence[DeliveryRecord]) -> int:
        """Bulk-insert *records* as PENDING; duplicate delivery_id is a no-op."""
        if not records:
            return 0

        stmt = text(f"""
            INSERT INTO {self._t} (
                delivery_id, subscription_id, owner_id, event_name, event_id,
                schema_version, tenant_id, data_json, emitted_at, target_url,
                resolved_ip, redelivery_of, attempts, fire_at, created_at,
                status, lease_expiry
            ) VALUES (
                :delivery_id, :subscription_id, :owner_id, :event_name, :event_id,
                :schema_version, :tenant_id, :data_json, :emitted_at, :target_url,
                :resolved_ip, :redelivery_of, :attempts, :fire_at, :created_at,
                :status, NULL
            )
            ON CONFLICT (delivery_id) DO NOTHING
        """)

        params = [
            {
                "delivery_id": r.delivery_id,
                "subscription_id": r.subscription_id,
                "owner_id": r.owner_id,
                "event_name": r.event_name,
                "event_id": r.event_id,
                "schema_version": r.schema_version,
                "tenant_id": r.tenant_id,
                "data_json": r.data_json,
                "emitted_at": r.emitted_at,
                "target_url": r.target_url,
                "resolved_ip": r.resolved_ip,
                "redelivery_of": r.redelivery_of,
                "attempts": r.attempts,
                "fire_at": r.fire_at,
                "created_at": r.created_at,
                "status": DeliveryStatus.PENDING.value,
            }
            for r in records
        ]

        result: CursorResult[Any] = cast(CursorResult[Any], await self._s.execute(stmt, params))
        inserted: int = result.rowcount
        logger.debug(
            "pg_delivery_repo.enqueued",
            table=self._t,
            requested=len(records),
            inserted=inserted,
        )
        return inserted

    async def claim_batch(
        self,
        *,
        now: datetime,
        lease_ttl_s: int,
        batch_size: int,
    ) -> Sequence[DeliveryRecord]:
        """Atomically claim up to *batch_size* claimable deliveries.

        Claimable = status in (pending, failed_retry) AND fire_at <= now.
        Uses ``FOR UPDATE SKIP LOCKED`` so concurrent workers never race.
        Sets status → in_flight and stamps lease_expiry = now + lease_ttl_s.
        """
        lease_expiry = now + timedelta(seconds=lease_ttl_s)

        select_stmt = text(f"""
            SELECT delivery_id
            FROM {self._t}
            WHERE status IN ('pending', 'failed_retry')
              AND fire_at <= :now
            ORDER BY fire_at
            LIMIT :batch_size
            FOR UPDATE SKIP LOCKED
        """)

        update_stmt = text(f"""
            UPDATE {self._t}
            SET status = 'in_flight',
                lease_expiry = :lease_expiry
            WHERE delivery_id = ANY(:ids)
            RETURNING
                delivery_id, subscription_id, owner_id, event_name, event_id,
                schema_version, tenant_id, data_json, emitted_at, target_url,
                resolved_ip, redelivery_of, attempts, fire_at, created_at,
                status, lease_expiry
        """)

        result = await self._s.execute(select_stmt, {"now": now, "batch_size": batch_size})
        rows = result.fetchall()
        if not rows:
            return []

        ids = [row.delivery_id for row in rows]
        updated = await self._s.execute(
            update_stmt,
            {"lease_expiry": lease_expiry, "ids": ids},
        )
        records = [_row_to_delivery(r) for r in updated.fetchall()]

        logger.debug(
            "pg_delivery_repo.claimed",
            table=self._t,
            count=len(records),
            lease_expiry=lease_expiry.isoformat(),
        )
        return records

    async def reclaim_stale(self, *, now: datetime) -> int:
        """Return expired in_flight rows (lease_expiry < now) back to pending."""
        stmt = text(f"""
            UPDATE {self._t}
            SET status = 'pending',
                lease_expiry = NULL
            WHERE status = 'in_flight'
              AND lease_expiry < :now
        """)
        result: CursorResult[Any] = cast(
            CursorResult[Any], await self._s.execute(stmt, {"now": now})
        )
        count: int = result.rowcount
        if count:
            logger.info(
                "pg_delivery_repo.reclaimed_stale",
                table=self._t,
                count=count,
            )
        return count

    async def mark_sent(self, delivery_id: str, attempt: AttemptRecord) -> None:
        """Transition *delivery_id* to SENT (terminal)."""
        stmt = text(f"""
            UPDATE {self._t}
            SET status = 'sent',
                attempts = :attempts,
                lease_expiry = NULL
            WHERE delivery_id = :delivery_id
        """)
        await self._s.execute(
            stmt,
            {"delivery_id": delivery_id, "attempts": attempt.attempt},
        )
        logger.debug(
            "pg_delivery_repo.marked_sent",
            delivery_id=delivery_id,
            attempt=attempt.attempt,
        )

    async def schedule_retry(
        self,
        delivery_id: str,
        fire_at: datetime,
        attempt: AttemptRecord,
    ) -> None:
        """Transition *delivery_id* to FAILED_RETRY, scheduled at *fire_at*."""
        stmt = text(f"""
            UPDATE {self._t}
            SET status = 'failed_retry',
                fire_at = :fire_at,
                attempts = :attempts,
                lease_expiry = NULL
            WHERE delivery_id = :delivery_id
        """)
        await self._s.execute(
            stmt,
            {
                "delivery_id": delivery_id,
                "fire_at": fire_at,
                "attempts": attempt.attempt,
            },
        )
        logger.debug(
            "pg_delivery_repo.scheduled_retry",
            delivery_id=delivery_id,
            attempt=attempt.attempt,
            fire_at=fire_at.isoformat(),
        )

    async def mark_dead(self, delivery_id: str, attempt: AttemptRecord) -> None:
        """Transition *delivery_id* to DEAD_LETTERED (terminal)."""
        stmt = text(f"""
            UPDATE {self._t}
            SET status = 'dead_lettered',
                attempts = :attempts,
                lease_expiry = NULL
            WHERE delivery_id = :delivery_id
        """)
        await self._s.execute(
            stmt,
            {"delivery_id": delivery_id, "attempts": attempt.attempt},
        )
        logger.info(
            "pg_delivery_repo.marked_dead",
            delivery_id=delivery_id,
            attempt=attempt.attempt,
        )

    async def bump_attempt(self, delivery_id: str, attempt: AttemptRecord) -> None:
        """Increment the attempts counter without changing status."""
        stmt = text(f"""
            UPDATE {self._t}
            SET attempts = :attempts
            WHERE delivery_id = :delivery_id
        """)
        await self._s.execute(
            stmt,
            {"delivery_id": delivery_id, "attempts": attempt.attempt},
        )
        logger.debug(
            "pg_delivery_repo.bumped_attempt",
            delivery_id=delivery_id,
            attempt=attempt.attempt,
        )

    async def get_record(self, delivery_id: str) -> DeliveryRecord | None:
        """Fetch a single delivery row by primary key, or ``None``."""
        stmt = text(f"""
            SELECT
                delivery_id, subscription_id, owner_id, event_name, event_id,
                schema_version, tenant_id, data_json, emitted_at, target_url,
                resolved_ip, redelivery_of, attempts, fire_at, created_at,
                status, lease_expiry
            FROM {self._t}
            WHERE delivery_id = :delivery_id
        """)
        result = await self._s.execute(stmt, {"delivery_id": delivery_id})
        row = result.fetchone()
        return _row_to_delivery(row) if row is not None else None

    async def recent_for_subscription(
        self,
        sub_id: str,
        limit: int,
    ) -> Sequence[DeliveryRecord]:
        """Return the most-recent *limit* deliveries for *sub_id*, newest first."""
        stmt = text(f"""
            SELECT
                delivery_id, subscription_id, owner_id, event_name, event_id,
                schema_version, tenant_id, data_json, emitted_at, target_url,
                resolved_ip, redelivery_of, attempts, fire_at, created_at,
                status, lease_expiry
            FROM {self._t}
            WHERE subscription_id = :sub_id
            ORDER BY created_at DESC
            LIMIT :limit
        """)
        result = await self._s.execute(stmt, {"sub_id": sub_id, "limit": limit})
        rows = result.fetchall()
        return [_row_to_delivery(r) for r in rows]


class PgAttemptRepo(WebhookAttemptRepoProtocol):
    """SQLAlchemy-async ``text()`` implementation of :class:`WebhookAttemptRepoProtocol`.

    Args:
        session: Active :class:`~sqlalchemy.ext.asyncio.AsyncSession`.
        table: Validated SQL identifier for the attempts table.
    """

    def __init__(self, session: AsyncSession, table: str) -> None:
        self._s = session
        self._t = table

    async def append(self, delivery_id: str, attempt: AttemptRecord) -> None:
        """Insert one attempt row; duplicate (delivery_id, attempt) is a no-op."""
        stmt = text(f"""
            INSERT INTO {self._t} (
                delivery_id, attempt, attempted_at,
                http_code, duration_ms, error, response_snippet
            ) VALUES (
                :delivery_id, :attempt, :attempted_at,
                :http_code, :duration_ms, :error, :response_snippet
            )
            ON CONFLICT DO NOTHING
        """)
        await self._s.execute(
            stmt,
            {
                "delivery_id": delivery_id,
                "attempt": attempt.attempt,
                "attempted_at": attempt.attempted_at,
                "http_code": attempt.http_code,
                "duration_ms": attempt.duration_ms,
                "error": attempt.error,
                "response_snippet": attempt.response_snippet,
            },
        )
        logger.debug(
            "pg_attempt_repo.appended",
            delivery_id=delivery_id,
            attempt=attempt.attempt,
        )


async def ensure_delivery_schema(
    engine: AsyncEngine,
    deliveries_table: str,
    attempts_table: str,
) -> None:
    """CREATE TABLE IF NOT EXISTS for deliveries + attempts, plus all indexes.

    Safe to call on every startup — all DDL is idempotent.

    Args:
        engine: Live async engine pointed at the target database.
        deliveries_table: Safe SQL identifier for the deliveries table.
        attempts_table: Safe SQL identifier for the attempts table.
    """
    _validate_table_name(deliveries_table)
    _validate_table_name(attempts_table)

    ddl_deliveries = text(f"""
        CREATE TABLE IF NOT EXISTS {deliveries_table} (
            delivery_id    TEXT        NOT NULL PRIMARY KEY,
            subscription_id TEXT       NOT NULL,
            owner_id       TEXT        NOT NULL,
            event_name     TEXT        NOT NULL,
            event_id       TEXT        NOT NULL,
            schema_version TEXT        NOT NULL,
            tenant_id      TEXT,
            data_json      BYTEA       NOT NULL,
            emitted_at     TIMESTAMPTZ NOT NULL,
            target_url     TEXT        NOT NULL,
            resolved_ip    TEXT        NOT NULL,
            redelivery_of  TEXT,
            attempts       INT         NOT NULL DEFAULT 0,
            fire_at        TIMESTAMPTZ NOT NULL,
            created_at     TIMESTAMPTZ NOT NULL,
            status         TEXT        NOT NULL DEFAULT 'pending',
            lease_expiry   TIMESTAMPTZ
        )
    """)

    ddl_deliveries_idx_status = text(f"""
        CREATE INDEX IF NOT EXISTS {deliveries_table}_status_fire_at_idx
            ON {deliveries_table} (status, fire_at)
    """)

    ddl_deliveries_idx_sub = text(f"""
        CREATE INDEX IF NOT EXISTS {deliveries_table}_subscription_created_at_idx
            ON {deliveries_table} (subscription_id, created_at)
    """)

    ddl_attempts = text(f"""
        CREATE TABLE IF NOT EXISTS {attempts_table} (
            id               BIGSERIAL   PRIMARY KEY,
            delivery_id      TEXT        NOT NULL,
            attempt          INT         NOT NULL,
            attempted_at     TIMESTAMPTZ NOT NULL,
            http_code        INT,
            duration_ms      INT         NOT NULL,
            error            TEXT,
            response_snippet TEXT
        )
    """)

    ddl_attempts_idx = text(f"""
        CREATE INDEX IF NOT EXISTS {attempts_table}_delivery_id_idx
            ON {attempts_table} (delivery_id)
    """)

    async with engine.begin() as conn:
        await conn.execute(ddl_deliveries)
        await conn.execute(ddl_deliveries_idx_status)
        await conn.execute(ddl_deliveries_idx_sub)
        await conn.execute(ddl_attempts)
        await conn.execute(ddl_attempts_idx)

    logger.info(
        "pg_repos.schema_ensured",
        deliveries_table=deliveries_table,
        attempts_table=attempts_table,
    )


def make_pg_delivery_store(
    engine: AsyncEngine,
    deliveries_table: str,
    attempts_table: str,
) -> PgWebhookDeliveryStore:
    """Build a fully-wired :class:`PgWebhookDeliveryStore` from an engine.

    Validates both table names, constructs an ``async_sessionmaker``, and
    wires the two repo constructors (each captures the validated table name).

    Args:
        engine: Live async engine pointed at the target database.
        deliveries_table: Safe SQL identifier for the deliveries table.
        attempts_table: Safe SQL identifier for the attempts table.

    Returns:
        A ready-to-use :class:`PgWebhookDeliveryStore`.

    Example::

        store = make_pg_delivery_store(engine, "wh_deliveries", "wh_attempts")
        await ensure_delivery_schema(engine, "wh_deliveries", "wh_attempts")
    """
    _validate_table_name(deliveries_table)
    _validate_table_name(attempts_table)

    session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
        autobegin=True,
    )

    def _delivery_repo_ctor(session: AsyncSession) -> PgDeliveryRepo:
        return PgDeliveryRepo(session, deliveries_table)

    def _attempt_repo_ctor(session: AsyncSession) -> PgAttemptRepo:
        return PgAttemptRepo(session, attempts_table)

    return PgWebhookDeliveryStore(
        session_factory=session_factory,
        delivery_repo_ctor=_delivery_repo_ctor,
        attempt_repo_ctor=_attempt_repo_ctor,
    )
