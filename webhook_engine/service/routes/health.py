"""Liveness and readiness probes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Response

from libs.shared.time import now_utc
from webhook_engine.service.deps import ServiceDeps, get_deps

__all__ = ["router"]

router = APIRouter(tags=["health"])


@router.get("/health")
async def liveness() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
async def readiness(
    response: Response,
    deps: ServiceDeps = Depends(get_deps),
) -> dict[str, str]:
    # reclaim_stale is a cheap, idempotent round-trip to the backend — it both
    # proves connectivity and does useful work (returns expired leases).
    try:
        await deps.event_store.reclaim_stale(now_utc())
        return {"status": "ok"}
    except Exception:  # noqa: BLE001 — readiness reports, never raises
        response.status_code = 503
        return {"status": "backend_unavailable"}
