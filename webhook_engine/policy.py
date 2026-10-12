"""Retry classification + next-delay computation.

Pure: no I/O, no framework deps, inject ``random.Random`` for
deterministic tests. Dispatcher consumes the outcome; storage layer
translates it into queue transitions.
"""

from __future__ import annotations

import random
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import ClassVar

import httpx

from webhook_engine.config import WebhookRetryConfig
from webhook_engine.enums import DeliveryStatus, RetryReason

__all__ = ["DeliveryPolicy", "parse_retry_after"]


def parse_retry_after(raw: str | None, *, now: datetime) -> float | None:
    """`Retry-After` as seconds or an HTTP-date; anything else is ignored (`None`)."""
    if raw is None or not raw.strip():
        return None
    text = raw.strip()
    if text.isdigit():
        return float(text)
    try:
        at = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    if at.tzinfo is None:
        return None
    return max(0.0, (at - now).total_seconds())


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
        *,
        retry_after_s: float | None = None,
    ) -> float:
        """`retry_after_s` — the receiver named the wait: honoured up to the cap, no jitter."""
        if retry_after_s is not None:
            return min(retry_after_s, cfg.max_delay_s)
        r = rng or random
        base = cfg.base_delay_s * (cfg.backoff_factor ** max(attempt - 1, 0))
        capped = min(base, cfg.max_delay_s)
        jitter = capped * cfg.jitter_pct
        return max(0.0, capped + r.uniform(-jitter, jitter))

    def outcome_status(self, reason: RetryReason | None, will_retry: bool) -> DeliveryStatus:
        if reason is None:
            return DeliveryStatus.SENT
        return DeliveryStatus.FAILED_RETRY if will_retry else DeliveryStatus.DEAD_LETTERED
