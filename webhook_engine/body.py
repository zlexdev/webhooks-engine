"""What goes on the wire for one delivery — the envelope, or the stored bytes as they are."""

from __future__ import annotations

import json
from abc import ABC, abstractmethod

from webhook_engine.envelope import WebhookEnvelope
from webhook_engine.types import DeliveryRecord

__all__ = ["BaseBodyEncoder", "EnvelopeEncoder", "RawEncoder"]


class BaseBodyEncoder(ABC):
    @abstractmethod
    def encode(self, record: DeliveryRecord, *, attempt: int) -> bytes: ...


class EnvelopeEncoder(BaseBodyEncoder):
    """The 0.2 body: `WebhookEnvelope` around `record.data_json`."""

    def encode(self, record: DeliveryRecord, *, attempt: int) -> bytes:
        try:
            data = json.loads(record.data_json)
        except ValueError:
            data = {}
        return WebhookEnvelope(
            id=record.delivery_id,
            event=record.event_name,
            event_id=record.event_id,
            schema_version=record.schema_version,
            emitted_at=record.emitted_at,
            tenant_id=record.tenant_id,
            attempt=attempt,
            redelivery_of=record.redelivery_of,
            data=data,
        ).to_body()


class RawEncoder(BaseBodyEncoder):
    """`record.data_json` verbatim: the receiver expects the host's own body, not an envelope."""

    def encode(self, record: DeliveryRecord, *, attempt: int) -> bytes:
        return record.data_json
