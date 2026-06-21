"""Framework-agnostic tick loop.

Single-replica contract:

1. ``reclaim_stale`` moves any lease that expired while we slept back
   to the ready queue.
2. ``claim_batch`` atomically pulls up to ``batch_size`` records.
3. ``asyncio.gather`` over those records under a
   ``worker_concurrency``-sized semaphore — one slow target never
   blocks the batch.
4. ``drain(timeout_s)`` waits for outstanding tasks on shutdown,
   cancelling anything still running past the deadline.

The app worker (:class:`api.workers.webhook_dispatch.WebhookDispatchWorker`)
is a thin ``BaseLoopWorker`` that delegates to ``tick()`` / ``drain()``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from libs.shared.logging import get_logger
from libs.shared.time import now_utc
from webhook_engine.config import WebhookDispatchConfig
from webhook_engine.dispatcher import WebhookDispatcher
from webhook_engine.enums import DeliveryStatus
from webhook_engine.protocols import Clock, WebhookDeliveryStoreProtocol
from webhook_engine.types import AttemptRecord, DeliveryRecord, DispatchOutcome

__all__ = ["EngineTickStats", "WebhookEngine"]


@dataclass(frozen=True, slots=True)
class EngineTickStats:
    claimed: int
    sent: int
    retried: int
    dead: int
    errors: int


class WebhookEngine:
    def __init__(
        self,
        store: WebhookDeliveryStoreProtocol,
        dispatcher: WebhookDispatcher,
        config: WebhookDispatchConfig,
        clock: Clock,
    ) -> None:
        self._store = store
        self._dispatcher = dispatcher
        self._cfg = config
        self._clock = clock
        self._gate = asyncio.Semaphore(config.worker_concurrency)
        self._inflight: set[asyncio.Task[DispatchOutcome]] = set()
        self._log = get_logger("webhooks.engine")

    async def tick(self) -> EngineTickStats:
        now = self._clock()
        reclaimed = 0
        try:
            reclaimed = await self._store.reclaim_stale(now)
        except Exception as exc:  # noqa: BLE001 - reclaim failure mustn't kill loop
            self._log.warn("reclaim_failed", error=str(exc), exc_info=True)

        records: list[DeliveryRecord] = []
        try:
            records = await self._store.claim_batch(
                self._clock(),
                self._cfg.lease_ttl_s,
                self._cfg.batch_size,
            )
        except Exception as exc:  # noqa: BLE001 - transient Redis errors
            self._log.warn("claim_failed", error=str(exc), exc_info=True)
            return EngineTickStats(claimed=0, sent=0, retried=0, dead=0, errors=1)

        if not records:
            if reclaimed:
                self._log.debug("reclaimed_only", count=reclaimed)
            return EngineTickStats(claimed=0, sent=0, retried=0, dead=0, errors=0)

        tasks: list[asyncio.Task[DispatchOutcome]] = []
        for record in records:
            task = asyncio.create_task(self._dispatch_one(record))
            self._inflight.add(task)
            task.add_done_callback(self._inflight.discard)
            tasks.append(task)

        outcomes = await asyncio.gather(*tasks, return_exceptions=True)
        sent = retried = dead = errors = 0
        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                errors += 1
                continue
            status = outcome.status
            if status == DeliveryStatus.SENT:
                sent += 1
            elif status == DeliveryStatus.FAILED_RETRY:
                retried += 1
            elif status == DeliveryStatus.DEAD_LETTERED:
                dead += 1
        if errors:
            self._log.warn(
                "tick_partial",
                claimed=len(records),
                errors=errors,
                sent=sent,
                retried=retried,
                dead=dead,
            )
        return EngineTickStats(
            claimed=len(records),
            sent=sent,
            retried=retried,
            dead=dead,
            errors=errors,
        )

    async def _dispatch_one(self, record: DeliveryRecord) -> DispatchOutcome:
        async with self._gate:
            try:
                return await self._dispatcher.send(record)
            except Exception as exc:  # noqa: BLE001 - engine shields batch from one bad record
                self._log.error(
                    "dispatch_unhandled",
                    delivery_id=record.delivery_id,
                    subscription_id=record.subscription_id,
                    event_name=record.event_name,
                    error=str(exc),
                    error_type=exc.__class__.__name__,
                    exc_info=True,
                )
                await self._quarantine(record, exc)
                raise

    async def _quarantine(self, record: DeliveryRecord, exc: BaseException) -> None:
        """Best-effort salvage for an unhandled dispatcher exception.

        The dispatcher owns all normal ``mark_sent / schedule_retry /
        mark_dead`` transitions. Reaching this path means a bug or a
        transient Redis / protocol failure before any state transition
        landed — the record would otherwise sit IN_FLIGHT until
        ``reclaim_stale`` moves it back, with no evidence in the
        attempts stream.

        Strategy:
        1. Append a synthetic :class:`AttemptRecord` so operators see
           *why* the delivery stalled.
        2. On exhaustion (``attempts + 1 >= max_attempts``) force a
           dead-letter so the record does not loop forever.
        3. Otherwise let reclaim pick it up on the next tick — the
           store's ``append_attempt`` already bumped visibility.
        """
        attempt_no = record.attempts + 1
        attempt = AttemptRecord(
            attempt=attempt_no,
            attempted_at=now_utc(),
            http_code=None,
            duration_ms=0,
            error=f"engine_unhandled:{exc.__class__.__name__}",
            response_snippet="",
        )
        try:
            await self._store.append_attempt(record.delivery_id, attempt)
        except Exception as sub_exc:  # noqa: BLE001 - final safety net
            self._log.error(
                "quarantine_append_failed",
                delivery_id=record.delivery_id,
                error=str(sub_exc),
            )
            return
        if attempt_no >= self._cfg.max_unhandled_attempts:
            try:
                await self._store.mark_dead(record.delivery_id, attempt)
            except Exception as sub_exc:  # noqa: BLE001
                self._log.error(
                    "quarantine_mark_dead_failed",
                    delivery_id=record.delivery_id,
                    error=str(sub_exc),
                )

    async def drain(self, timeout_s: float) -> None:
        if not self._inflight:
            return
        self._log.info("drain_start", inflight=len(self._inflight), timeout_s=timeout_s)
        try:
            await asyncio.wait_for(
                asyncio.gather(*self._inflight, return_exceptions=True),
                timeout=timeout_s,
            )
        except asyncio.TimeoutError:
            self._log.warn(
                "drain_timeout",
                remaining=len(self._inflight),
            )
            for task in list(self._inflight):
                task.cancel()
        self._log.info("drain_complete")
