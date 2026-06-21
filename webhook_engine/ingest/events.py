"""Producer-facing event contract.

:class:`IncomingEvent` is the *only* shape a producing service must write into
the shared store (PG row / Mongo document). The engine reads it, resolves
subscriptions, and fans out signed webhooks. Keep this stable — it is the
cross-service contract.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

__all__ = ["EventIngestStatus", "IncomingEvent"]


class EventIngestStatus(StrEnum):
    """Lifecycle of an ingested event row in the shared store."""

    PENDING = "pending"
    CLAIMED = "claimed"
    DONE = "done"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class IncomingEvent:
    """One event a producer wrote for fan-out.

    ``event_id`` is the producer-supplied idempotency key — the engine dedupes
    deliveries by ``(subscription_id, event_id)``, so re-writing the same
    ``event_id`` never double-delivers. ``data`` is the arbitrary payload the
    subscriber receives (after redaction).
    """

    event_id: str
    event: str
    data: dict[str, Any]
    tenant_id: str | None = None
    schema_version: str = "1.0"
    created_at: datetime | None = None
