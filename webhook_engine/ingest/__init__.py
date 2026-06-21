"""Ingestion layer — producers write events to a shared DB; the engine reads.

Public surface: the contract (:class:`IncomingEvent`, :class:`BaseEventStore`),
the two backends (:class:`PgEventStore`, :class:`MongoEventStore`), and the
:class:`EventIngestor` that turns claimed events into queued deliveries.
"""

from webhook_engine.ingest.base import BaseEventStore
from webhook_engine.ingest.events import EventIngestStatus, IncomingEvent
from webhook_engine.ingest.ingestor import EventIngestor, IngestTickStats

__all__ = [
    "BaseEventStore",
    "EventIngestStatus",
    "EventIngestor",
    "IncomingEvent",
    "IngestTickStats",
]
