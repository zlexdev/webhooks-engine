from __future__ import annotations

import pytest
from _fakes import RecordingSender

from webhook_engine.stores.memory import MemoryWebhookDeliveryStore


@pytest.fixture
def store() -> MemoryWebhookDeliveryStore:
    return MemoryWebhookDeliveryStore()


@pytest.fixture
def sender() -> RecordingSender:
    return RecordingSender()
