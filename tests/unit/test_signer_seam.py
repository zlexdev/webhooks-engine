"""SignerProtocol: a host substitutes its own signature scheme."""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from webhook_engine import (
    HmacSigner,
    SignedHeaders,
    SignerProtocol,
    WebhookVerificationError,
    verify_webhook,
)
from webhook_engine.exceptions import SignatureVerificationError

SECRET = "s3cr3t"
BODY = b'{"hello":"world"}'


@dataclass(frozen=True, slots=True)
class GithubHeaders:
    """Deliberately inherits nothing from the package — the seam is structural."""

    signature: str

    def as_dict(self) -> dict[str, str]:
        return {"X-Hub-Signature-256": self.signature, "Content-Type": "application/json"}


class GithubStyleSigner(SignerProtocol):
    """`sha256=<hex>` over the raw body — the GitHub / Stripe shape."""

    @staticmethod
    def _mac(secret: str, body: bytes) -> str:
        return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()

    def sign(
        self,
        *,
        delivery_id: str,
        event_name: str,
        event_id: str,
        body: bytes,
        secret: str,
        now: datetime,
    ) -> GithubHeaders:
        return GithubHeaders(f"sha256={self._mac(secret, body)}")

    def verify(
        self,
        *,
        body: bytes,
        headers: dict[str, str],
        secret: str,
        now: datetime,
        window_s: int,
    ) -> None:
        provided = headers.get("X-Hub-Signature-256", "").split("=", 1)[-1]
        if not hmac.compare_digest(self._mac(secret, body), provided):
            raise SignatureVerificationError("bad mac")


def _now() -> datetime:
    return datetime.now(UTC)


def test_shipped_signer_satisfies_the_protocol() -> None:
    assert isinstance(HmacSigner(), SignerProtocol)


def test_custom_signer_controls_header_names() -> None:
    headers = GithubStyleSigner().sign(
        delivery_id="d1",
        event_name="order.paid",
        event_id="ev1",
        body=BODY,
        secret=SECRET,
        now=_now(),
    )
    assert isinstance(headers, SignedHeaders)
    assert set(headers.as_dict()) == {"X-Hub-Signature-256", "Content-Type"}


def test_verify_webhook_delegates_to_the_given_signer() -> None:
    signer = GithubStyleSigner()
    headers = signer.sign(
        delivery_id="d1",
        event_name="order.paid",
        event_id="ev1",
        body=BODY,
        secret=SECRET,
        now=_now(),
    ).as_dict()

    verify_webhook(secret=SECRET, body=BODY, headers=headers, signer=signer)

    # A tampered body must surface as the receiver-facing error type, not the
    # engine's internal one — receivers catch exactly one class and return 401.
    with pytest.raises(WebhookVerificationError):
        verify_webhook(secret=SECRET, body=b"tampered", headers=headers, signer=signer)


def test_default_path_is_byte_for_byte_unchanged() -> None:
    """The seam must not move the shipped format — existing receivers keep working."""
    signed = (
        HmacSigner()
        .sign(
            delivery_id="d1",
            event_name="order.paid",
            event_id="ev1",
            body=BODY,
            secret=SECRET,
            now=_now(),
        )
        .as_dict()
    )

    assert set(signed) == {
        "Webhook-Id",
        "Webhook-Event",
        "Webhook-Event-Id",
        "Webhook-Timestamp",
        "Webhook-Signature",
        "Content-Type",
    }
    assert signed["Webhook-Signature"].startswith("v1=")
    verify_webhook(secret=SECRET, body=BODY, headers=signed)
