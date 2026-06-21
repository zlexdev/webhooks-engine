"""Per-host asyncio.Semaphore registry (bulkhead).

One slow target doesn't starve quota for other hosts — each host gets
its own ``asyncio.Semaphore(per_host_concurrency)`` lazily on first
acquire. In-process only; a Redis-backed pool behind the same
interface can be swapped in later without dispatcher changes.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from webhook_engine.config import WebhookLimitsConfig
from webhook_engine.types import HostName

__all__ = ["HostSemaphorePool"]


class HostSemaphorePool:
    def __init__(self, limits: WebhookLimitsConfig) -> None:
        self._per_host: int = limits.per_host_concurrency
        self._pool: dict[HostName, asyncio.Semaphore] = {}
        self._guard: asyncio.Lock = asyncio.Lock()

    @asynccontextmanager
    async def acquire(self, host: HostName) -> AsyncIterator[None]:
        sem = await self._get(host.lower())
        async with sem:
            yield

    async def _get(self, host: HostName) -> asyncio.Semaphore:
        existing = self._pool.get(host)
        if existing is not None:
            return existing
        async with self._guard:
            sem = self._pool.get(host)
            if sem is None:
                sem = asyncio.Semaphore(self._per_host)
                self._pool[host] = sem
            return sem

    def active_hosts(self) -> list[HostName]:
        return list(self._pool.keys())

    def size(self) -> int:
        return len(self._pool)
