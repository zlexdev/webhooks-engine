"""Fakes for wiring a dispatcher/engine without Redis, Postgres or a network."""

from __future__ import annotations

from datetime import UTC, datetime

from webhook_engine import (
    DeliveryPolicy,
    DeliveryRecord,
    DeliveryStatus,
    HmacSigner,
    HostSemaphorePool,
    HttpSendResult,
    SecretMaterial,
    SecretReaderProtocol,
    UrlSafetyValidator,
    WebhookDispatcher,
    WebhookPolicyConfig,
)
from webhook_engine.stores.memory import MemoryWebhookDeliveryStore

PUBLIC_IP = "93.184.216.34"


def now_utc() -> datetime:
    return datetime.now(UTC)


class FakeSecretReader(SecretReaderProtocol):
    async def get(self, subscription_id: str) -> SecretMaterial:
        return SecretMaterial(current="s3cr3t", previous=None, previous_expires_at=None)

    def invalidate(self, subscription_id: str) -> None:
        return None


class RecordingSender:
    """Captures what would have gone on the wire; answers 202 with a body."""

    def __init__(self, status: int = 202) -> None:
        self.status = status
        self.calls: list[dict[str, object]] = []

    async def post(
        self,
        *,
        url: str,
        body: bytes,
        headers: dict[str, str],
        resolved_ip: str | None,
        connect_timeout_s: float,
        read_timeout_s: float,
        total_timeout_s: float,
        max_response_bytes: int,
    ) -> HttpSendResult:
        self.calls.append({"url": url, "headers": headers, "resolved_ip": resolved_ip})
        return HttpSendResult(
            status=self.status,
            headers={},
            body_snippet='{"verdict":"spam"}',
            duration_ms=1,
        )


def make_record(
    *,
    delivery_id: str = "d1",
    target_url: str = "https://example.com/hook",
    resolved_ip: str = PUBLIC_IP,
    attempts: int = 0,
) -> DeliveryRecord:
    stamp = now_utc()
    return DeliveryRecord(
        delivery_id=delivery_id,
        subscription_id="sub1",
        owner_id="own1",
        event_name="order.paid",
        event_id="ev1",
        schema_version="1",
        tenant_id=None,
        data_json=b'{"amount":10}',
        emitted_at=stamp,
        target_url=target_url,
        resolved_ip=resolved_ip,
        redelivery_of=None,
        attempts=attempts,
        fire_at=stamp,
        created_at=stamp,
        status=DeliveryStatus.PENDING,
    )


def make_dispatcher(
    *,
    store: MemoryWebhookDeliveryStore,
    sender: RecordingSender,
    config: WebhookPolicyConfig | None = None,
    resolver: object | None = None,
) -> WebhookDispatcher:
    cfg = config or WebhookPolicyConfig()
    return WebhookDispatcher(
        signer=HmacSigner(),
        http_sender=sender,
        policy=DeliveryPolicy(),
        config=cfg,
        store=store,
        secret_reader=FakeSecretReader(),
        ssrf_guard=UrlSafetyValidator(cfg.security),
        host_sem=HostSemaphorePool(cfg.limits),
        clock=now_utc,
        resolver=resolver,  # type: ignore[arg-type]
    )
