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
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from webhook_engine.protocols import SignerProtocol

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


def _verify_via_signer(
    *,
    signer: SignerProtocol,
    secret: str,
    body: bytes,
    headers: Mapping[str, str],
    now_ts: int | None,
    window_s: int,
) -> None:
    # The engine's own error type is imported here, not at module scope, so a
    # receiver that never passes `signer` keeps this module dependency-free.
    from webhook_engine.exceptions import SignatureVerificationError

    current = now_ts if now_ts is not None else int(time.time())
    try:
        signer.verify(
            body=body,
            headers=dict(headers),
            secret=secret,
            now=datetime.fromtimestamp(current, tz=UTC),
            window_s=window_s,
        )
    except SignatureVerificationError as exc:
        # One error surface for receivers: whichever signer rejected it, the
        # caller catches WebhookVerificationError and returns 401.
        raise WebhookVerificationError(str(exc)) from exc
    except WebhookVerificationError:
        raise
    except Exception as exc:  # noqa: BLE001 - a third-party signer may raise anything
        # Without this a custom signer's own error type escapes as a 500, the
        # sender reads 5xx as "retry", and a forged payload gets retried.
        raise WebhookVerificationError(f"{exc.__class__.__name__}: {exc}") from exc


def verify_webhook(
    *,
    secret: str,
    body: bytes,
    headers: Mapping[str, str],
    now_ts: int | None = None,
    window_s: int = DEFAULT_WINDOW_S,
    signer: SignerProtocol | None = None,
) -> None:
    """Validate an incoming webhook delivery in constant time.

    Raises :class:`WebhookVerificationError` on any mismatch — missing
    headers, stale timestamp (replay), or a bad MAC. Returns ``None`` on
    success.

    ``now_ts`` defaults to the current POSIX seconds; pass an explicit value
    in tests to make the freshness window deterministic.

    ``signer`` mirrors ``WebhookDispatcher(signer=...)``: pass the same
    :class:`~webhook_engine.protocols.SignerProtocol` the sender was built
    with and this function checks *that* scheme instead of the built-in one.
    Left ``None`` (the default) it stays dependency-free and verifies the
    shipped ``v1=`` HMAC inline, importing nothing from the engine.
    """
    if signer is not None:
        return _verify_via_signer(
            signer=signer,
            secret=secret,
            body=body,
            headers=headers,
            now_ts=now_ts,
            window_s=window_s,
        )

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
    version, _, provided = sig.partition("=")
    # Same rule as HmacSigner.verify — the two must not drift: a bare mac with
    # no version label, or a label this build does not implement, is a reject.
    if version != "v1" or not provided:
        raise WebhookVerificationError("unsupported signature version")
    if not hmac.compare_digest(expected, provided):
        raise WebhookVerificationError("signature mismatch")
