"""Retry classification + next-delay computation.

Pure: no I/O, no framework deps, inject ``random.Random`` for
deterministic tests. Dispatcher consumes the outcome; storage layer
translates it into queue transitions.
"""

from __future__ import annotations

import random
from typing import ClassVar

import httpx

from webhook_engine.config import WebhookRetryConfig
from webhook_engine.enums import DeliveryStatus, RetryReason

__all__ = ["DeliveryPolicy"]


class DeliveryPolicy:
    RETRYABLE_REASONS: ClassVar[frozenset[RetryReason]] = frozenset(
        {
            RetryReason.HTTP_5XX,
            RetryReason.HTTP_408,
            RetryReason.HTTP_429,
            RetryReason.NETWORK_ERROR,
            RetryReason.TIMEOUT,
        }
    )

    def classify_status(self, status: int) -> RetryReason | None:
        if 200 <= status < 300:
            return None
        if status >= 500:
            return RetryReason.HTTP_5XX
        if status == 408:
            return RetryReason.HTTP_408
        if status == 429:
            return RetryReason.HTTP_429
        return RetryReason.NON_RETRYABLE_4XX

    def classify_exception(self, exc: Exception) -> RetryReason:
        if isinstance(exc, httpx.TimeoutException):
            return RetryReason.TIMEOUT
        return RetryReason.NETWORK_ERROR

    def retry_on(
        self,
        reason: RetryReason,
        attempt: int,
        cfg: WebhookRetryConfig,
    ) -> bool:
        return reason in self.RETRYABLE_REASONS and attempt < cfg.max_attempts

    def next_delay_s(
        self,
        attempt: int,
        cfg: WebhookRetryConfig,
        rng: random.Random | None = None,
    ) -> float:
        r = rng or random
        base = cfg.base_delay_s * (cfg.backoff_factor ** max(attempt - 1, 0))
        capped = min(base, cfg.max_delay_s)
        jitter = capped * cfg.jitter_pct
        return max(0.0, capped + r.uniform(-jitter, jitter))

    def outcome_status(self, reason: RetryReason | None, will_retry: bool) -> DeliveryStatus:
        if reason is None:
            return DeliveryStatus.SENT
        return DeliveryStatus.FAILED_RETRY if will_retry else DeliveryStatus.DEAD_LETTERED
