"""Background ingestion loop — drives :meth:`EventIngestor.tick`.

Runs as its own asyncio task next to the dispatch worker: one reads producer
events into the delivery queue, the other drains that queue to subscribers.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from libs.shared.logging import get_logger

__all__ = ["run_ingest_loop"]

if TYPE_CHECKING:
    from webhook_engine.ingest.ingestor import EventIngestor

_log = get_logger("webhooks.ingest_worker")


async def run_ingest_loop(ingestor: EventIngestor, poll_interval_s: float) -> None:
    _log.info("ingest_worker_start", poll_interval_s=poll_interval_s)
    while True:
        try:
            stats = await ingestor.tick()
            if stats.claimed:
                _log.info(
                    "ingest_tick",
                    claimed=stats.claimed,
                    fanned_out=stats.fanned_out,
                    failed=stats.failed,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — ingest loop must never die
            _log.error("ingest_tick_unhandled", error=str(exc), exc_info=True)
        await asyncio.sleep(poll_interval_s)
