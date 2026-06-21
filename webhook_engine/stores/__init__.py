"""Persistence stores for webhook delivery — pick a backend at bootstrap.

- Redis: :class:`RedisWebhookDeliveryStore` (``stores/redis.py``).
- Postgres: :class:`PgWebhookDeliveryStore` + :class:`PgSubscriptionReader`
  (``stores/pg.py``) — session-factory + repo-ctor injection, no app
  imports inside libs.
"""

from webhook_engine.protocols import (
    SubscriptionReaderProtocol,
    WebhookDeliveryStoreProtocol,
)
from webhook_engine.stores.base import BaseWebhookDeliveryStore
from webhook_engine.stores.pg import (
    PgSubscriptionReader,
    PgWebhookDeliveryStore,
    WebhookAttemptRepoProtocol,
    WebhookDeliveryRepoProtocol,
    WebhookSubscriptionRepoProtocol,
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
    "PgSubscriptionReader",
    "PgWebhookDeliveryStore",
    "SubscriptionReaderProtocol",
    "SubscriptionSnapshot",
    "WebhookAttemptRepoProtocol",
    "WebhookDeliveryRepoProtocol",
    "WebhookDeliveryStoreProtocol",
    "WebhookSubscriptionRepoProtocol",
]
