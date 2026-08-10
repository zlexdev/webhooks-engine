"""HMAC-SHA256 signing for webhook deliveries.

Signed message: ``f"{timestamp}.{event_id}.{sha256_hex(body)}"``.
``timestamp`` is the POSIX seconds at send time — consumers reject
messages outside ``security.signature_window_s``. ``compare_digest``
is used on verify for constant-time comparison.

Signing happens at send-time in the dispatcher (not at enqueue), so
a record sitting in retry backoff never arrives with a stale
timestamp.

This is the *default* signer, not the only one — the dispatcher takes
any :class:`~webhook_engine.protocols.SignerProtocol`.
"""

from __future__ import annotations

import hashlib
import hmac
from datetime import UTC, datetime

from webhook_engine.enums import SignatureVersion
from webhook_engine.envelope import sha256_hex
from webhook_engine.exceptions import SignatureVerificationError
from webhook_engine.protocols import SignerProtocol
from webhook_engine.types import SignatureHeaders

__all__ = ["HmacSigner", "SignatureHeaders"]


class HmacSigner(SignerProtocol):
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
        # HTTP header names are case-insensitive and most frameworks hand them
        # over lower-cased — matching on the exact case would 401 every valid
        # delivery from a real server.
        lowered = {k.lower(): v for k, v in headers.items()}
        ts_raw = lowered.get("webhook-timestamp", "")
        event_id = lowered.get("webhook-event-id", "")
        sig = lowered.get("webhook-signature", "")
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
        version, _, provided = sig.partition("=")
        # Check the version label, not just the mac: without this a future
        # "v2=<mac>" — or an attacker's "v99=" — is accepted as v1.
        if version != self._version.value or not provided:
            raise SignatureVerificationError("unsupported signature version")
        if not hmac.compare_digest(expected, provided):
            raise SignatureVerificationError("bad mac")
