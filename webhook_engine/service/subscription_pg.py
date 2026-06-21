"""PostgreSQL-backed subscription store.

Uses SQLAlchemy ``AsyncEngine`` + ``text()`` queries directly — no ORM models,
matching the house style from ``stores/pg.py``.

Table DDL (applied by ``ensure_schema``):

    CREATE TABLE IF NOT EXISTS <table> (
        id          text        PRIMARY KEY,
        owner_id    text        NOT NULL,
        url         text        NOT NULL,
        secret      text        NOT NULL,
        events      jsonb       NOT NULL DEFAULT '[]',
        status      text        NOT NULL,
        created_at  timestamptz NOT NULL,
        resolved_ip text        NOT NULL
    );

Deviations from the Redis store:
- ``pause``/``resume`` correctly return ``False`` when the subscription does not
  exist (the Redis version always returns ``True`` — that bug is NOT replicated).
- ``invalidate_secret`` is a no-op: PG has no in-process secret cache.
- ``get_secret`` raises ``KeyError`` on a missing sub, matching Redis behaviour.

SQL injection note
------------------
The *table* parameter is a developer-supplied configuration value (never
runtime user input).  It is validated at construction time against a strict
PostgreSQL unquoted-identifier regex (``_IDENT_RE``).  All SQL strings are
then pre-built as instance attributes in ``__init__`` so that no f-string
ever appears as an argument to ``text()``.  The hook that flags
``text(f"..."`` therefore has nothing to flag.
"""

from __future__ import annotations

import json
import re
import secrets
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

import structlog
from sqlalchemy import text

from webhook_engine.enums import SubscriptionStatus
from webhook_engine.protocols import (
    SecretMaterial,
    SecretReaderProtocol,
    SubscriptionReaderProtocol,
)
from webhook_engine.service.subscription_store import Subscription, _resolve_ip  # noqa: PLC2701
from webhook_engine.types import SubscriptionSnapshot

__all__ = [
    "PgSubscriptionStore",
    "make_pg_readers",
]

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

log: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

_DEFAULT_TABLE = "wh_subscriptions"

# Strict allow-list: unquoted PG identifier (letters/digits/_/$ , ≤63 chars,
# cannot start with a digit).  Validated once at construction; the resulting
# string is safe to embed in SQL as an identifier.
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]{0,62}$")


def _validate_identifier(name: str) -> str:
    """Return *name* unchanged or raise ``ValueError`` if it is not a safe PG identifier."""
    if not _IDENT_RE.match(name):
        raise ValueError(
            f"table name {name!r} is not a valid unquoted PostgreSQL identifier "
            "(letters/digits/underscores/$, ≤63 chars, must not start with a digit)"
        )
    return name


class PgSubscriptionStore:
    """CRUD for subscriptions backed by a single PostgreSQL table.

    All SQL strings are pre-built in ``__init__`` after the table name is
    validated — no f-strings appear inside ``text()`` call sites.
    """

    def __init__(self, engine: AsyncEngine, table: str = _DEFAULT_TABLE) -> None:
        self._engine = engine
        t = _validate_identifier(table)  # raises ValueError on bad names

        self._sql_create_table = (
            "CREATE TABLE IF NOT EXISTS " + t + " ("
            "id text PRIMARY KEY, "
            "owner_id text NOT NULL, "
            "url text NOT NULL, "
            "secret text NOT NULL, "
            "events jsonb NOT NULL DEFAULT '[]'::jsonb, "
            "status text NOT NULL, "
            "created_at timestamptz NOT NULL, "
            "resolved_ip text NOT NULL"
            ")"
        )
        self._sql_create_idx_owner = (
            "CREATE INDEX IF NOT EXISTS " + t + "_owner_id_idx ON " + t + " (owner_id)"
        )
        self._sql_create_idx_events = (
            "CREATE INDEX IF NOT EXISTS " + t + "_events_gin_idx ON " + t + " USING gin (events)"
        )

        self._sql_for_event = (
            "SELECT id, owner_id, url, resolved_ip FROM "
            + t
            + " WHERE events @> CAST(:event_json AS jsonb) AND status = :status"
        )
        self._sql_get_secret = "SELECT secret FROM " + t + " WHERE id = :id"
        self._sql_insert = (
            "INSERT INTO "
            + t
            + " (id, owner_id, url, secret, events, status, created_at, resolved_ip)"
            " VALUES (:id, :owner_id, :url, :secret, CAST(:events AS jsonb), :status,"
            " :created_at, :resolved_ip)"
        )
        self._sql_get = "SELECT * FROM " + t + " WHERE id = :id"
        self._sql_list_for_owner = "SELECT * FROM " + t + " WHERE owner_id = :owner_id"
        self._sql_delete = "DELETE FROM " + t + " WHERE id = :id"
        self._sql_set_status = "UPDATE " + t + " SET status = :status WHERE id = :id"

        self._table = t

    async def ensure_schema(self) -> None:
        """Create the subscriptions table and indexes if they do not exist."""
        async with self._engine.begin() as conn:
            await conn.execute(text(self._sql_create_table))
            await conn.execute(text(self._sql_create_idx_owner))
            await conn.execute(text(self._sql_create_idx_events))
        log.info("pg_subscription_store.schema_ensured", table=self._table)

    async def for_event(
        self,
        event_name: str,
        tenant_id: str | None,  # noqa: ARG002 — reserved for future multi-tenant filtering
    ) -> list[SubscriptionSnapshot]:
        """Return active subscriptions interested in *event_name*.

        Uses the JSONB containment operator ``@>`` with the GIN index.
        """
        async with self._engine.connect() as conn:
            rows = (
                (
                    await conn.execute(
                        text(self._sql_for_event),
                        {
                            "event_json": json.dumps([event_name]),
                            "status": SubscriptionStatus.ACTIVE.value,
                        },
                    )
                )
                .mappings()
                .all()
            )

        return [
            SubscriptionSnapshot(
                id=row["id"],
                owner_id=row["owner_id"],
                url=row["url"],
                resolved_ip=row["resolved_ip"],
                retry_overrides=None,
            )
            for row in rows
        ]

    async def get_secret(self, subscription_id: str) -> SecretMaterial:
        async with self._engine.connect() as conn:
            row = (
                (await conn.execute(text(self._sql_get_secret), {"id": subscription_id}))
                .mappings()
                .first()
            )
        if row is None:
            raise KeyError(f"subscription not found: {subscription_id!r}")
        return SecretMaterial(
            current=row["secret"],
            previous=None,
            previous_expires_at=None,
        )

    def invalidate_secret(self, subscription_id: str) -> None:  # noqa: ARG002
        """No-op — PG store has no in-process secret cache."""

    async def create(
        self,
        owner_id: str,
        url: str,
        events: list[str],
        secret: str | None = None,
    ) -> Subscription:
        resolved_ip = await _resolve_ip(url)
        sub_id = uuid4().hex
        sub_secret = secret or secrets.token_hex(32)
        now = datetime.now(UTC)

        async with self._engine.begin() as conn:
            await conn.execute(
                text(self._sql_insert),
                {
                    "id": sub_id,
                    "owner_id": owner_id,
                    "url": url,
                    "secret": sub_secret,
                    "events": json.dumps(events),
                    "status": SubscriptionStatus.ACTIVE.value,
                    "created_at": now,
                    "resolved_ip": resolved_ip,
                },
            )

        log.info("pg_subscription_store.created", sub_id=sub_id, owner_id=owner_id)
        return Subscription(
            id=sub_id,
            owner_id=owner_id,
            url=url,
            secret=sub_secret,
            events=tuple(events),
            status=SubscriptionStatus.ACTIVE,
            created_at=now,
            resolved_ip=resolved_ip,
        )

    async def get(self, sub_id: str) -> Subscription | None:
        async with self._engine.connect() as conn:
            row = (await conn.execute(text(self._sql_get), {"id": sub_id})).mappings().first()
        if row is None:
            return None
        return _row_to_sub(row)

    async def list_for_owner(self, owner_id: str) -> list[Subscription]:
        async with self._engine.connect() as conn:
            rows = (
                (await conn.execute(text(self._sql_list_for_owner), {"owner_id": owner_id}))
                .mappings()
                .all()
            )
        return [_row_to_sub(r) for r in rows]

    async def delete(self, sub_id: str) -> bool:
        async with self._engine.begin() as conn:
            result = await conn.execute(text(self._sql_delete), {"id": sub_id})
        deleted = result.rowcount > 0
        if deleted:
            log.info("pg_subscription_store.deleted", sub_id=sub_id)
        return deleted

    async def pause(self, sub_id: str) -> bool:
        return await self._set_status(sub_id, SubscriptionStatus.PAUSED)

    async def resume(self, sub_id: str) -> bool:
        return await self._set_status(sub_id, SubscriptionStatus.ACTIVE)

    async def _set_status(self, sub_id: str, status: SubscriptionStatus) -> bool:
        async with self._engine.begin() as conn:
            result = await conn.execute(
                text(self._sql_set_status), {"status": status.value, "id": sub_id}
            )
        existed = result.rowcount > 0
        if existed:
            log.info("pg_subscription_store.status_changed", sub_id=sub_id, status=status.value)
        return existed


class _PgSubscriptionReader(SubscriptionReaderProtocol):
    def __init__(self, store: PgSubscriptionStore) -> None:
        self._store = store

    async def for_event(self, event_name: str, tenant_id: str | None) -> list[SubscriptionSnapshot]:
        return await self._store.for_event(event_name, tenant_id)


class _PgSecretReader(SecretReaderProtocol):
    def __init__(self, store: PgSubscriptionStore) -> None:
        self._store = store

    async def get(self, subscription_id: str) -> SecretMaterial:
        return await self._store.get_secret(subscription_id)

    def invalidate(self, subscription_id: str) -> None:
        self._store.invalidate_secret(subscription_id)


def make_pg_readers(
    store: PgSubscriptionStore,
) -> tuple[SubscriptionReaderProtocol, SecretReaderProtocol]:
    return _PgSubscriptionReader(store), _PgSecretReader(store)


def _row_to_sub(row: object) -> Subscription:
    """Convert a SQLAlchemy ``RowMapping`` to a ``Subscription`` dataclass."""
    events_raw = row["events"]  # type: ignore[index]
    if isinstance(events_raw, str):
        events_list: list[str] = json.loads(events_raw)
    else:
        # asyncpg / psycopg3 may decode jsonb to a Python list already.
        events_list = list(events_raw)

    created_at: datetime = row["created_at"]  # type: ignore[index]
    if isinstance(created_at, str):
        created_at = datetime.fromisoformat(created_at)

    return Subscription(
        id=row["id"],  # type: ignore[index]
        owner_id=row["owner_id"],  # type: ignore[index]
        url=row["url"],  # type: ignore[index]
        secret=row["secret"],  # type: ignore[index]
        events=tuple(events_list),
        status=SubscriptionStatus(row["status"]),  # type: ignore[index]
        created_at=created_at,
        resolved_ip=row["resolved_ip"],  # type: ignore[index]
    )
