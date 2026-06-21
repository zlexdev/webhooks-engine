"""Outgoing envelope + payload redaction helpers.

The fanout handler never ships the raw event dict. ``redact_payload``
drops the internal ``meta`` housekeeping field (re-projected into
envelope-level keys) plus any sensitive field — either explicitly
listed in :attr:`WebhookMeta.redacted_fields` or auto-matched by name
suffix (``_enc`` / ``_token``) / substring (``password`` / ``secret``).

Auto-redaction is belt-and-suspenders: events in this repo carry
Fernet-encrypted tokens (``epic_refresh_token_enc`` etc.). Explicit
``redacted_fields`` is the documented signal, but the suffix rule
catches new sensitive fields added in future refactors without
reviewer attention. ``allow_sensitive=True`` is the escape hatch.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from webhook_engine.meta import WebhookMeta

__all__ = [
    "REDACTED_PLACEHOLDER",
    "WebhookEnvelope",
    "redact_payload",
    "sha256_hex",
    "should_auto_redact",
]

_AUTO_REDACT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"_enc$"),
    re.compile(r"_token$"),
    re.compile(r"password", re.IGNORECASE),
    re.compile(r"secret$", re.IGNORECASE),
)

REDACTED_PLACEHOLDER: str = "<redacted>"


def should_auto_redact(field_name: str) -> bool:
    """Whether a field name matches one of the auto-redact patterns."""
    return any(p.search(field_name) for p in _AUTO_REDACT_PATTERNS)


def redact_payload(data: dict[str, Any], meta: WebhookMeta) -> dict[str, Any]:
    """Drop sensitive fields from the outgoing payload.

    - Fields listed in ``meta.redacted_fields`` → always replaced.
    - Fields matching the auto-suffix rule → replaced unless
      ``meta.allow_sensitive=True``.
    - The internal ``meta`` housekeeping field is stripped.

    Returns a fresh dict. Never mutates the input.
    """
    redacted: dict[str, Any] = {}
    for k, v in data.items():
        if k == "meta":
            continue
        if k in meta.redacted_fields:
            redacted[k] = REDACTED_PLACEHOLDER
            continue
        if not meta.allow_sensitive and should_auto_redact(k):
            redacted[k] = REDACTED_PLACEHOLDER
            continue
        redacted[k] = v
    return redacted


@dataclass(frozen=True, slots=True)
class WebhookEnvelope:
    """Outer JSON shape every target receives.

    ``API_VERSION`` is the envelope schema itself — independent of the
    per-event ``schema_version`` so consumers can pin to a known shape
    even when individual event schemas evolve.
    """

    API_VERSION: ClassVar[str] = "1"

    id: str
    event: str
    event_id: str
    schema_version: str
    emitted_at: datetime
    tenant_id: str | None
    attempt: int
    redelivery_of: str | None
    data: dict[str, Any]

    def to_body(self) -> bytes:
        payload = {
            "id": self.id,
            "api_version": self.API_VERSION,
            "event": self.event,
            "event_id": self.event_id,
            "schema_version": self.schema_version,
            "emitted_at": self.emitted_at.isoformat(),
            "tenant_id": self.tenant_id,
            "attempt": self.attempt,
            "redelivery_of": self.redelivery_of,
            "data": self.data,
        }
        return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()


def sha256_hex(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()
