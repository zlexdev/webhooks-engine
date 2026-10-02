"""WE-T4: the contract suite on the Postgres store; skipped without `WE_TEST_PG_URL`."""

from __future__ import annotations

import os
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from webhook_engine.protocols import WebhookDeliveryStoreProtocol
from webhook_engine.service.pg_repos import ensure_delivery_schema, make_pg_delivery_store
from webhook_engine.testing import StoreContract

_URL = os.environ.get("WE_TEST_PG_URL")

pytestmark = pytest.mark.skipif(not _URL, reason="WE_TEST_PG_URL not set")


class TestPgStore(StoreContract):
    async def make_store(self) -> WebhookDeliveryStoreProtocol:
        assert _URL is not None
        suffix = uuid4().hex[:8]
        deliveries, attempts = f"we_deliveries_{suffix}", f"we_attempts_{suffix}"
        engine = create_async_engine(_URL)
        await ensure_delivery_schema(engine, deliveries, attempts)
        return make_pg_delivery_store(engine, deliveries, attempts)
