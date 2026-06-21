"""Background tick loop — runs inside the FastAPI process.

One ``asyncio.Task`` drives :meth:`WebhookEngine.tick` on
``poll_interval_s`` cadence. On shutdown the app cancels the task
and calls :meth:`WebhookEngine.drain` to finish in-flight deliveries.
"""

from __future__ import annotations

import asyncio

from libs.shared.logging import get_logger
from webhook_engine.engine import WebhookEngine

__all__ = ["run_tick_loop"]

_log = get_logger("webhooks.worker")


async def run_tick_loop(engine: WebhookEngine, poll_interval_s: float) -> None:
    """Drive the engine tick loop until cancelled."""
    _log.info("worker_start", poll_interval_s=poll_interval_s)
    while True:
        try:
            stats = await engine.tick()
            if stats.claimed:
                _log.info(
                    "tick",
                    claimed=stats.claimed,
                    sent=stats.sent,
                    retried=stats.retried,
                    dead=stats.dead,
                    errors=stats.errors,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — tick loop must never die
            _log.error("tick_unhandled", error=str(exc), exc_info=True)
        await asyncio.sleep(poll_interval_s)
