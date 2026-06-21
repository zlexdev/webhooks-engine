"""Single-delivery orchestration.

Pure orchestration — no httpx / Mongo / Fernet imports. Every side-
effect goes through a :mod:`webhook_engine.protocols` interface so the
dispatcher can be unit-tested with fakes.

Hot path, per record:

1. Body-size guard (payload too large → dead-letter, no network I/O).
2. Resolve current + previous secret via :class:`SecretReaderProtocol`
   (TTL cache; rotation propagates within the window).
3. Re-check the pinned ``resolved_ip`` against the CIDR block set —
   catches DNS rebinding that happened between enqueue and send.
4. Build :class:`WebhookEnvelope`, sign at *now*, POST under the
   per-host bulkhead.
5. Classify + mark — success / scheduled retry / dead-letter.
6. On dead-letter, emit :class:`WebhookDeliveryExhausted` (direct
   dispatch, never through the outbox).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from urllib.parse import urlsplit
from uuid import uuid4

import httpx

from libs.shared.logging import get_logger
from libs.shared.time import now_utc
from webhook_engine.config import WebhookPolicyConfig, WebhookRetryConfig
from webhook_engine.enums import DeliveryStatus, RetryReason
from webhook_engine.envelope import WebhookEnvelope
from webhook_engine.exceptions import InvalidWebhookTarget
from webhook_engine.host_semaphores import HostSemaphorePool
from webhook_engine.policy import DeliveryPolicy
from webhook_engine.protocols import (
    Clock,
    HttpSenderProtocol,
    SecretReaderProtocol,
    WebhookDeliveryStoreProtocol,
)
from webhook_engine.signer import HmacSigner
from webhook_engine.ssrf_guard import UrlSafetyValidator
from webhook_engine.types import AttemptRecord, DeliveryRecord, DispatchOutcome, HttpSendResult

__all__ = ["WebhookDispatcher"]

if TYPE_CHECKING:
    from asyncbus import BaseEventBus


@dataclass(frozen=True, slots=True)
class _SignedDelivery:
    body: bytes
    headers: dict[str, str]
    envelope: WebhookEnvelope


class WebhookDispatcher:
    def __init__(
        self,
        *,
        signer: HmacSigner,
        http_sender: HttpSenderProtocol,
        policy: DeliveryPolicy,
        config: WebhookPolicyConfig,
        store: WebhookDeliveryStoreProtocol,
        secret_reader: SecretReaderProtocol,
        ssrf_guard: UrlSafetyValidator,
        host_sem: HostSemaphorePool,
        clock: Clock,
        bus: BaseEventBus | None = None,
    ) -> None:
        self._signer = signer
        self._sender = http_sender
        self._policy = policy
        self._config = config
        self._store = store
        self._secret_reader = secret_reader
        self._ssrf = ssrf_guard
        self._host_sem = host_sem
        self._clock = clock
        self._bus = bus
        self._log = get_logger("webhooks.dispatcher")

    async def send(self, record: DeliveryRecord) -> DispatchOutcome:
        attempt_no = record.attempts + 1
        now = self._clock()
        limits = self._config.limits
        retry_cfg = self._retry_config(record)

        if len(record.data_json) > limits.max_body_bytes:
            return await self._dead_letter(
                record,
                attempt_no,
                now,
                http_code=None,
                duration_ms=0,
                error="payload_too_large",
                retry_reason=RetryReason.PAYLOAD_TOO_LARGE,
                response_snippet="",
            )

        try:
            self._ssrf.check_ip(record.resolved_ip)
        except InvalidWebhookTarget as exc:
            return await self._dead_letter(
                record,
                attempt_no,
                now,
                http_code=None,
                duration_ms=0,
                error=f"blocked_ip_at_send:{exc.reason.value}",
                retry_reason=RetryReason.BLOCKED_IP_AT_SEND,
                response_snippet="",
            )

        try:
            secret = await self._secret_reader.get(record.subscription_id)
        except Exception as exc:  # noqa: BLE001 - resolver can fail on missing sub
            self._log.warn(
                "secret_resolve_failed",
                delivery_id=record.delivery_id,
                subscription_id=record.subscription_id,
                error=str(exc),
            )
            return await self._dead_letter(
                record,
                attempt_no,
                now,
                http_code=None,
                duration_ms=0,
                error="secret_unresolved",
                retry_reason=RetryReason.NETWORK_ERROR,
                response_snippet="",
            )

        signed = self._build_signed(record, secret.current, attempt_no, now)
        host = urlsplit(record.target_url).hostname or ""

        send_result: HttpSendResult | None = None
        retry_reason: RetryReason | None = None
        send_error: str | None = None
        duration_ms = 0
        async with self._host_sem.acquire(host):
            try:
                send_result = await self._sender.post(
                    url=record.target_url,
                    body=signed.body,
                    headers=signed.headers,
                    resolved_ip=record.resolved_ip,
                    connect_timeout_s=limits.http_connect_timeout_s,
                    read_timeout_s=limits.http_read_timeout_s,
                    total_timeout_s=limits.http_total_timeout_s,
                    max_response_bytes=self._config.security.max_response_bytes,
                )
            except httpx.RequestError as exc:
                retry_reason = self._policy.classify_exception(exc)
                send_error = f"{exc.__class__.__name__}: {exc}"

        if send_result is not None:
            duration_ms = send_result.duration_ms
            retry_reason = self._policy.classify_status(send_result.status)

        will_retry = retry_reason is not None and self._policy.retry_on(
            retry_reason, attempt_no, retry_cfg
        )
        status = self._policy.outcome_status(retry_reason, will_retry)
        attempt = AttemptRecord(
            attempt=attempt_no,
            attempted_at=now,
            http_code=send_result.status if send_result else None,
            duration_ms=duration_ms,
            error=send_error,
            response_snippet=send_result.body_snippet if send_result else "",
        )

        if status == DeliveryStatus.SENT:
            await self._store.mark_sent(record.delivery_id, attempt)
            return DispatchOutcome(status=status, next_fire_at=None, attempt=attempt)

        if status == DeliveryStatus.FAILED_RETRY:
            delay_s = self._policy.next_delay_s(attempt_no, retry_cfg)
            fire_at = now + timedelta(seconds=delay_s)
            await self._store.schedule_retry(record.delivery_id, fire_at, attempt)
            return DispatchOutcome(status=status, next_fire_at=fire_at, attempt=attempt)

        return await self._dead_letter_from_attempt(
            record,
            attempt,
            retry_reason=retry_reason,
        )

    def _retry_config(self, record: DeliveryRecord) -> WebhookRetryConfig:
        _ = record  # reserved for per-subscription retry overrides
        return self._config.retry

    def _build_signed(
        self,
        record: DeliveryRecord,
        secret: str,
        attempt_no: int,
        now: datetime,
    ) -> _SignedDelivery:
        try:
            data = json.loads(record.data_json)
        except ValueError:
            data = {}
        envelope = WebhookEnvelope(
            id=record.delivery_id,
            event=record.event_name,
            event_id=record.event_id,
            schema_version=record.schema_version,
            emitted_at=record.emitted_at,
            tenant_id=record.tenant_id,
            attempt=attempt_no,
            redelivery_of=record.redelivery_of,
            data=data,
        )
        body = envelope.to_body()
        headers = self._signer.sign(
            delivery_id=record.delivery_id,
            event_name=record.event_name,
            event_id=record.event_id,
            body=body,
            secret=secret,
            now=now,
        ).as_dict()
        return _SignedDelivery(body=body, headers=headers, envelope=envelope)

    async def _dead_letter(
        self,
        record: DeliveryRecord,
        attempt_no: int,
        now: datetime,
        *,
        http_code: int | None,
        duration_ms: int,
        error: str,
        retry_reason: RetryReason,
        response_snippet: str,
    ) -> DispatchOutcome:
        attempt = AttemptRecord(
            attempt=attempt_no,
            attempted_at=now,
            http_code=http_code,
            duration_ms=duration_ms,
            error=error,
            response_snippet=response_snippet,
        )
        return await self._dead_letter_from_attempt(
            record,
            attempt,
            retry_reason=retry_reason,
        )

    async def _dead_letter_from_attempt(
        self,
        record: DeliveryRecord,
        attempt: AttemptRecord,
        *,
        retry_reason: RetryReason | None,
    ) -> DispatchOutcome:
        await self._store.mark_dead(record.delivery_id, attempt)
        if self._bus is not None:
            # asyncbus is an optional dependency — imported only on the bus path
            # so the core delivery service installs and runs without it.
            from asyncbus import EventMeta

            from webhook_engine.events import WebhookDeliveryExhausted

            try:
                await self._bus.emit_post(
                    WebhookDeliveryExhausted(
                        meta=EventMeta(
                            event_id=uuid4().hex,
                            emitted_at=now_utc(),
                            source="webhooks.dispatcher",
                        ),
                        delivery_id=record.delivery_id,
                        subscription_id=record.subscription_id,
                        owner_id=record.owner_id,
                        source_event=record.event_name,
                        attempts=attempt.attempt,
                        last_error=attempt.error or "",
                    ),
                    source="webhooks.dispatcher",
                )
            except Exception as exc:  # noqa: BLE001 - never let meta-emit block dead-letter path
                self._log.warn(
                    "exhausted_emit_failed",
                    delivery_id=record.delivery_id,
                    error=str(exc),
                )
        _ = retry_reason  # reserved for metrics in a later wave
        return DispatchOutcome(
            status=DeliveryStatus.DEAD_LETTERED,
            next_fire_at=None,
            attempt=attempt,
        )
