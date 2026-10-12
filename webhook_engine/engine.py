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

The bundled worker (:mod:`webhook_engine.service.worker`) is a thin loop that
delegates to ``tick()`` / ``drain()``.

Seeing the result of a delivery
-------------------------------

``tick()`` returns counters only; the per-delivery :class:`DispatchOutcome` —
including ``attempt.response_snippet``, i.e. what the receiver answered — would
otherwise be dropped on the floor. Pass ``on_outcome`` to observe it.

Three rules keep that hook off the hot path, and they are load-bearing:

1. it runs **after** the concurrency semaphore is released, so a slow callback
   never holds a worker slot against the other records in the batch;
2. it runs under ``dispatch.on_outcome_timeout_s``;
3. its failure — timeout or exception — is logged and otherwise invisible. The
   store transition already happened; a callback cannot undo a delivery, so it
   must not be able to change the outcome or the tick counters either.

Do not move the call inside the semaphore block "for symmetry" — that is the
mistake this note exists to prevent.

What rule 1 does *not* buy: ``tick()`` gathers all its tasks, so it still does
not return until every callback has finished or timed out, and the next batch is
not claimed before then. Deliveries inside the batch stop waiting on each other;
the tick as a whole does not.

TODO(debt): nothing validates ``on_outcome_timeout_s < lease_ttl_s`` — proper
fix: reject the combination in WebhookDispatchConfig — deferred: config
validation is not wired for any field yet, and the defaults (5s vs 30s) are
safe. Raise it if either default moves.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from webhook_engine._shared.logging import get_logger
from webhook_engine._shared.time import now_utc
from webhook_engine.config import WebhookDispatchConfig
from webhook_engine.dispatcher import WebhookDispatcher
from webhook_engine.enums import DeliveryStatus
from webhook_engine.protocols import Clock, WebhookDeliveryStoreProtocol
from webhook_engine.types import AttemptRecord, DeliveryRecord, DispatchOutcome

__all__ = ["EngineTickStats", "OutcomeCallback", "WebhookEngine"]

OutcomeCallback = Callable[[DeliveryRecord, DispatchOutcome], Awaitable[None]]


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
        on_outcome: OutcomeCallback | None = None,
    ) -> None:
        self._store = store
        self._dispatcher = dispatcher
        self._cfg = config
        self._clock = clock
        self._on_outcome = on_outcome
        self._gate = asyncio.Semaphore(config.worker_concurrency)
        self._inflight: set[asyncio.Task[DispatchOutcome]] = set()
        self._log = get_logger("webhooks.engine")

    async def tick(self) -> EngineTickStats:
        now = self._clock()
        reclaimed = 0
        reclaim_errors = 0
        try:
            reclaimed = await self._store.reclaim_stale(now)
        except Exception as exc:  # noqa: BLE001 - reclaim failure mustn't kill loop
            reclaim_errors = 1
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
            return EngineTickStats(claimed=0, sent=0, retried=0, dead=0, errors=reclaim_errors)

        tasks: list[asyncio.Task[DispatchOutcome]] = []
        for record in records:
            task = asyncio.create_task(self._dispatch_one(record))
            self._inflight.add(task)
            task.add_done_callback(self._inflight.discard)
            tasks.append(task)

        outcomes = await asyncio.gather(*tasks, return_exceptions=True)
        sent = retried = dead = 0
        errors = reclaim_errors
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
                outcome = await self._dispatcher.send(record)
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
        # Outside the semaphore on purpose — see the module docstring. There is
        # no outcome on the quarantine path above, so the hook is not called
        # there: a synthetic one would report a result that never happened.
        await self._notify(record, outcome)
        return outcome

    async def _notify(self, record: DeliveryRecord, outcome: DispatchOutcome) -> None:
        if self._on_outcome is None:
            return
        try:
            await asyncio.wait_for(
                self._on_outcome(record, outcome),
                timeout=self._cfg.on_outcome_timeout_s,
            )
        except TimeoutError:
            self._log.warn(
                "outcome_callback_timeout",
                delivery_id=record.delivery_id,
                timeout_s=self._cfg.on_outcome_timeout_s,
            )
        except Exception as exc:  # noqa: BLE001 - a host callback cannot break delivery
            self._log.warn(
                "outcome_callback_failed",
                delivery_id=record.delivery_id,
                error=str(exc),
                error_type=exc.__class__.__name__,
                exc_info=True,
            )

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
        except TimeoutError:
            self._log.warn(
                "drain_timeout",
                remaining=len(self._inflight),
            )
            for task in list(self._inflight):
                task.cancel()
        self._log.info("drain_complete")
