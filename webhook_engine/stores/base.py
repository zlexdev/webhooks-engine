"""Store contracts — ABC alias + protocol re-exports.

``WebhookDeliveryStoreProtocol`` is itself an ABC (PR5 storage-collapse);
``BaseWebhookDeliveryStore`` survives as a name alias so existing imports
keep working without re-declaring the abstract surface.
"""

from __future__ import annotations

from webhook_engine.protocols import (
    SubscriptionReaderProtocol,
    WebhookDeliveryStoreProtocol,
)
from webhook_engine.types import (
    AttemptRecord,
    DeliveryRecord,
    SubscriptionSnapshot,
)

__all__ = [
    "AttemptRecord",
    "BaseWebhookDeliveryStore",
    "DeliveryRecord",
    "SubscriptionReaderProtocol",
    "SubscriptionSnapshot",
    "WebhookDeliveryStoreProtocol",
]

BaseWebhookDeliveryStore = WebhookDeliveryStoreProtocol
