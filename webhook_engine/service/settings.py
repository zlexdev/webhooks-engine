"""Service settings from environment (prefix ``WHE_``).

One profile per process: ``WHE_BACKEND=pg`` or ``WHE_BACKEND=mongo`` selects the
database that holds *everything* — ingested events, subscriptions, and the
delivery queue. Producers write :class:`IncomingEvent` rows/documents into the
events table/collection; the engine reads them and fans out.

Table / collection names are overridable so the engine never collides with a
host project's schema. The installer (`scripts/install.py`) writes unique
timestamped defaults into ``.env``.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = ["EventBackend", "Settings"]


class EventBackend(StrEnum):
    PG = "pg"
    MONGO = "mongo"
    MEMORY = "memory"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="WHE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    backend: EventBackend = EventBackend.PG

    # Shared secret authenticating producer / admin calls (X-Source-Key header).
    source_key: str = Field(...)

    pg_dsn: str = "postgresql+asyncpg://localhost:5432/webhooks"
    pg_events_table: str = "wh_events"
    pg_subscriptions_table: str = "wh_subscriptions"
    pg_deliveries_table: str = "wh_deliveries"
    pg_attempts_table: str = "wh_delivery_attempts"

    mongo_dsn: str = "mongodb://localhost:27017"
    mongo_db: str = "webhooks"
    mongo_events_collection: str = "wh_events"
    mongo_subscriptions_collection: str = "wh_subscriptions"
    mongo_deliveries_collection: str = "wh_deliveries"

    host: str = "0.0.0.0"
    port: int = 8080
    debug: bool = False

    poll_interval_s: float = 1.0
    ingest_poll_interval_s: float = 1.0
    batch_size: int = 50
    ingest_batch_size: int = 100
    worker_concurrency: int = 20
    lease_ttl_s: int = 30
    drain_timeout_s: float = 10.0
