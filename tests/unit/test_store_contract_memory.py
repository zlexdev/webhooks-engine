"""WE-T4: the contract suite against the memory store, and proof the suite can fail."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime

import pytest

from webhook_engine.enums import DeliveryStatus
from webhook_engine.protocols import WebhookDeliveryStoreProtocol
from webhook_engine.stores.memory import MemoryWebhookDeliveryStore
from webhook_engine.testing import StoreContract
from webhook_engine.testing.store_contract import case_two_claimers_never_share_a_row
from webhook_engine.types import DeliveryRecord


class TestMemoryStore(StoreContract):
    async def make_store(self) -> WebhookDeliveryStoreProtocol:
        return MemoryWebhookDeliveryStore()


class _LeasesTwice(MemoryWebhookDeliveryStore):
    """Mutant: hands out claimable rows without marking them leased."""

    async def claim_batch(
        self, now: datetime, lease_ttl_s: int, batch_size: int
    ) -> list[DeliveryRecord]:
        ready = [
            r
            for r in self._records.values()
            if r.status in (DeliveryStatus.PENDING, DeliveryStatus.FAILED_RETRY)
            and r.fire_at <= now
        ]
        return [replace(r) for r in ready[:batch_size]]


async def test_a_store_that_leases_a_row_twice_fails_the_suite() -> None:
    with pytest.raises(AssertionError, match="leased twice"):
        await case_two_claimers_never_share_a_row(_LeasesTwice())
