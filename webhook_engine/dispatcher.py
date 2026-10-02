"""Single-delivery orchestration.

Pure orchestration — no httpx / Mongo / Fernet imports. Every side-
effect goes through a :mod:`webhook_engine.protocols` interface so the
dispatcher can be unit-tested with fakes.

Hot path, per record:

1. Body-size guard (payload too large → dead-letter, no network I/O).
2. Resolve current + previous secret via :class:`SecretReaderProtocol`
   (TTL cache; rotation propagates within the window).
3. Pick the target address (pinned ``resolved_ip``, or a fresh one if a
   ``TargetResolverProtocol`` was supplied) and re-check it against the
   CIDR block set — catches DNS rebinding between enqueue and send.
4. Encode the body with the record's profile (envelope by default), sign at
   *now*, POST under the per-host bulkhead.
5. Classify + mark — success / scheduled retry / dead-letter.
6. On dead-letter, emit :class:`WebhookDeliveryExhausted` (direct
   dispatch, never through the outbox).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit
from uuid import uuid4

import httpx

from webhook_engine._shared.logging import get_logger
from webhook_engine._shared.time import now_utc
from webhook_engine.config import WebhookPolicyConfig
from webhook_engine.enums import DeliveryStatus, RetryReason
from webhook_engine.exceptions import InvalidWebhookTarget
from webhook_engine.host_semaphores import HostSemaphorePool
from webhook_engine.policy import DeliveryPolicy, parse_retry_after
from webhook_engine.profiles import DeliveryProfile, ProfileRegistry
from webhook_engine.protocols import (
    Clock,
    HttpSenderProtocol,
    SecretReaderProtocol,
    SignerProtocol,
    TargetResolverProtocol,
    WebhookDeliveryStoreProtocol,
)
from webhook_engine.ssrf_guard import UrlSafetyValidator
from webhook_engine.types import AttemptRecord, DeliveryRecord, DispatchOutcome, HttpSendResult

__all__ = ["WebhookDispatcher"]

_RETRY_AFTER = "retry-after"


def _header(headers: dict[str, str], name: str) -> str | None:
    """Header lookup that ignores case: a sender may hand back its own casing."""
    return next((v for k, v in headers.items() if k.lower() == name), None)


if TYPE_CHECKING:
    from asyncbus import BaseEventBus


@dataclass(frozen=True, slots=True)
class _SignedDelivery:
    body: bytes
    headers: dict[str, str]


class WebhookDispatcher:
    def __init__(
        self,
        *,
        signer: SignerProtocol,
        http_sender: HttpSenderProtocol,
        policy: DeliveryPolicy,
        config: WebhookPolicyConfig,
        store: WebhookDeliveryStoreProtocol,
        secret_reader: SecretReaderProtocol,
        ssrf_guard: UrlSafetyValidator,
        host_sem: HostSemaphorePool,
        clock: Clock,
        bus: BaseEventBus | None = None,
        resolver: TargetResolverProtocol | None = None,
        profiles: ProfileRegistry[Any] | None = None,
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
        self._resolver = resolver
        self._profiles: ProfileRegistry[Any] = profiles or ProfileRegistry.single()
        self._log = get_logger("webhooks.dispatcher")

    async def send(self, record: DeliveryRecord) -> DispatchOutcome:
        attempt_no = record.attempts + 1
        now = self._clock()
        limits = self._config.limits
        profile = self._profiles.of(record)
        retry_cfg = profile.retry or self._config.retry
        max_response = profile.max_response_bytes or self._config.security.max_response_bytes
        parsed_target = urlsplit(record.target_url)
        host = parsed_target.hostname or ""
        target_port = parsed_target.port or (443 if parsed_target.scheme == "https" else 80)

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

        target_ip = await self._current_ip(record, host)
        try:
            self._ssrf.check_ip(target_ip, host=host, port=target_port)
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
            secret = await self._secret_reader.get_versioned(
                record.subscription_id, record.secret_version
            )
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

        signed = self._build_signed(record, profile, secret.current, attempt_no, now)

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
                    resolved_ip=target_ip,
                    connect_timeout_s=limits.http_connect_timeout_s,
                    read_timeout_s=limits.http_read_timeout_s,
                    total_timeout_s=limits.http_total_timeout_s,
                    max_response_bytes=max_response,
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
            response_body=send_result.body if send_result and retry_reason is None else b"",
            response_truncated=send_result.body_truncated if send_result else False,
        )

        if status == DeliveryStatus.SENT:
            await self._store.mark_sent(record.delivery_id, attempt)
            return DispatchOutcome(status=status, next_fire_at=None, attempt=attempt)

        if status == DeliveryStatus.FAILED_RETRY:
            waited = None
            if send_result is not None and profile.honour_retry_after:
                waited = parse_retry_after(_header(send_result.headers, _RETRY_AFTER), now=now)
            delay_s = self._policy.next_delay_s(attempt_no, retry_cfg, retry_after_s=waited)
            fire_at = now + timedelta(seconds=delay_s)
            await self._store.schedule_retry(record.delivery_id, fire_at, attempt)
            return DispatchOutcome(status=status, next_fire_at=fire_at, attempt=attempt)

        return await self._dead_letter_from_attempt(
            record,
            attempt,
            retry_reason=retry_reason,
        )

    async def _current_ip(self, record: DeliveryRecord, host: str) -> str:
        """The address this attempt will POST to.

        The pinned ``record.resolved_ip`` unless a resolver was supplied and
        answers. Whatever comes back is still handed to ``check_ip`` by the
        caller — a resolver cannot talk the dispatcher into a blocked address.
        """
        if self._resolver is None or not host:
            return record.resolved_ip
        try:
            fresh = await self._resolver.resolve(host)
        except Exception as exc:  # noqa: BLE001 - a dead resolver must not kill the retry
            self._log.warn(
                "resolve_failed",
                delivery_id=record.delivery_id,
                host=host,
                error=str(exc),
            )
            return record.resolved_ip
        return fresh or record.resolved_ip

    def _build_signed(
        self,
        record: DeliveryRecord,
        profile: DeliveryProfile,
        secret: str,
        attempt_no: int,
        now: datetime,
    ) -> _SignedDelivery:
        body = profile.body.encode(record, attempt=attempt_no)
        signer = profile.signer or self._signer
        headers = signer.sign(
            delivery_id=record.delivery_id,
            event_name=record.event_name,
            event_id=record.event_id,
            body=body,
            secret=secret,
            now=now,
        ).as_dict()
        return _SignedDelivery(body=body, headers=headers)

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
