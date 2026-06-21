"""HMAC-SHA256 signing for webhook deliveries.

Signed message: ``f"{timestamp}.{event_id}.{sha256_hex(body)}"``.
``timestamp`` is the POSIX seconds at send time — consumers reject
messages outside ``security.signature_window_s``. ``compare_digest``
is used on verify for constant-time comparison.

Signing happens at send-time in the dispatcher (not at enqueue), so
a record sitting in retry backoff never arrives with a stale
timestamp.
"""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from datetime import UTC, datetime

from webhook_engine.enums import SignatureVersion
from webhook_engine.envelope import sha256_hex
from webhook_engine.exceptions import SignatureVerificationError

__all__ = ["HmacSigner", "SignatureHeaders"]


@dataclass(frozen=True, slots=True)
class SignatureHeaders:
    webhook_id: str
    webhook_event: str
    webhook_event_id: str
    webhook_timestamp: str
    webhook_signature: str

    def as_dict(self) -> dict[str, str]:
        return {
            "Webhook-Id": self.webhook_id,
            "Webhook-Event": self.webhook_event,
            # event_id is part of the signed message — the receiver needs it
            # to reconstruct the MAC, so it must travel in a header.
            "Webhook-Event-Id": self.webhook_event_id,
            "Webhook-Timestamp": self.webhook_timestamp,
            "Webhook-Signature": self.webhook_signature,
            "Content-Type": "application/json",
        }


class HmacSigner:
    def __init__(self, version: SignatureVersion = SignatureVersion.V1) -> None:
        self._version = version

    def sign(
        self,
        *,
        delivery_id: str,
        event_name: str,
        event_id: str,
        body: bytes,
        secret: str,
        now: datetime,
    ) -> SignatureHeaders:
        ts_int = now.astimezone(UTC) if now.tzinfo else now.replace(tzinfo=UTC)
        ts = str(int(ts_int.timestamp()))
        msg = f"{ts}.{event_id}.{sha256_hex(body)}".encode()
        mac = hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()
        return SignatureHeaders(
            webhook_id=delivery_id,
            webhook_event=event_name,
            webhook_event_id=event_id,
            webhook_timestamp=ts,
            webhook_signature=f"{self._version.value}={mac}",
        )

    def verify(
        self,
        *,
        body: bytes,
        headers: dict[str, str],
        secret: str,
        now: datetime,
        window_s: int,
    ) -> None:
        """Consumer-side helper exported for user-docs + tests.

        Raises :class:`SignatureVerificationError` on any mismatch.
        """
        ts_raw = headers.get("Webhook-Timestamp", "")
        event_id = headers.get("Webhook-Event-Id", "")
        sig = headers.get("Webhook-Signature", "")
        if not (ts_raw and sig):
            raise SignatureVerificationError("missing headers")
        try:
            ts = int(ts_raw)
        except ValueError as exc:
            raise SignatureVerificationError("bad timestamp") from exc
        now_utc = now.astimezone(UTC) if now.tzinfo else now.replace(tzinfo=UTC)
        if abs(int(now_utc.timestamp()) - ts) > window_s:
            raise SignatureVerificationError("timestamp out of window")
        expected = hmac.new(
            secret.encode(),
            f"{ts}.{event_id}.{sha256_hex(body)}".encode(),
            hashlib.sha256,
        ).hexdigest()
        provided = sig.split("=", 1)[1] if "=" in sig else ""
        if not hmac.compare_digest(expected, provided):
            raise SignatureVerificationError("bad mac")
