"""Shared fixtures for the black-box e2e smoke suite.

The suite drives a *running* webhook-engine over HTTP. Point it at an instance
with WHE_BASE_URL / WHE_SOURCE_KEY; if nothing answers /health the whole suite
skips (so `pytest` stays green on a machine with no server up).
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import httpx
import pytest

BASE_URL = os.environ.get("WHE_BASE_URL", "http://localhost:8080").rstrip("/")
SOURCE_KEY = os.environ.get("WHE_SOURCE_KEY", "secret123")


@pytest.fixture(scope="session")
def client() -> Iterator[httpx.Client]:
    with httpx.Client(base_url=BASE_URL, timeout=10.0) as c:
        try:
            c.get("/health")
        except httpx.HTTPError:
            pytest.skip(f"no webhook-engine reachable at {BASE_URL}")
        yield c


@pytest.fixture
def auth() -> dict[str, str]:
    return {"X-Source-Key": SOURCE_KEY}
