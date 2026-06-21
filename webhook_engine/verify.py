"""Receiver-side signature verification — pure, dependency-free.

Ship this to webhook *consumers* so they can validate incoming deliveries
without importing the whole engine or reconstructing the signing scheme by
hand. Mirrors :meth:`webhook_engine.signer.HmacSigner.sign` exactly.

The signed message is ``f"{timestamp}.{event_id}.{sha256_hex(body)}"`` and the
``Webhook-Signature`` header carries ``v1=<hex mac>``. All inputs a receiver
needs travel in headers:

- ``Webhook-Id``         — delivery id
- ``Webhook-Event``      — event name
- ``Webhook-Event-Id``   — source event id (part of the MAC)
- ``Webhook-Timestamp``  — POSIX seconds at signing time
- ``Webhook-Signature``  — ``v1=<hmac-sha256 hex>``

Example (framework-agnostic)::

    from webhook_engine.verify import verify_webhook, WebhookVerificationError

    try:
        verify_webhook(secret=my_secret, body=raw_request_body, headers=request.headers)
    except WebhookVerificationError:
        return 401
"""

from __future__ import annotations

import hashlib
import hmac
import time
from collections.abc import Mapping

__all__ = ["DEFAULT_WINDOW_S", "WebhookVerificationError", "verify_webhook"]

DEFAULT_WINDOW_S = 300


class WebhookVerificationError(Exception):
    """Raised when an incoming webhook fails signature or freshness checks."""


def _header(headers: Mapping[str, str], name: str) -> str:
    # HTTP headers are case-insensitive; callers may pass a plain dict.
    if name in headers:
        return headers[name]
    lowered = {k.lower(): v for k, v in headers.items()}
    return lowered.get(name.lower(), "")


def verify_webhook(
    *,
    secret: str,
    body: bytes,
    headers: Mapping[str, str],
    now_ts: int | None = None,
    window_s: int = DEFAULT_WINDOW_S,
) -> None:
    """Validate an incoming webhook delivery in constant time.

    Raises :class:`WebhookVerificationError` on any mismatch — missing
    headers, stale timestamp (replay), or a bad MAC. Returns ``None`` on
    success.

    ``now_ts`` defaults to the current POSIX seconds; pass an explicit value
    in tests to make the freshness window deterministic.
    """
    ts_raw = _header(headers, "Webhook-Timestamp")
    event_id = _header(headers, "Webhook-Event-Id")
    sig = _header(headers, "Webhook-Signature")
    if not ts_raw or not sig:
        raise WebhookVerificationError("missing Webhook-Timestamp or Webhook-Signature header")

    try:
        ts = int(ts_raw)
    except ValueError as exc:
        raise WebhookVerificationError("malformed Webhook-Timestamp") from exc

    current = now_ts if now_ts is not None else int(time.time())
    if abs(current - ts) > window_s:
        raise WebhookVerificationError("timestamp outside the freshness window (possible replay)")

    digest = hashlib.sha256(body).hexdigest()
    expected = hmac.new(
        secret.encode(),
        f"{ts}.{event_id}.{digest}".encode(),
        hashlib.sha256,
    ).hexdigest()
    provided = sig.split("=", 1)[1] if "=" in sig else sig
    if not hmac.compare_digest(expected, provided):
        raise WebhookVerificationError("signature mismatch")
