"""FastAPI application factory and CLI entry point.

Usage:
    webhook-engine          # via pyproject.toml script
    python -m webhook_engine.service.app
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from webhook_engine.service.deps import build_deps
from webhook_engine.service.ingest_worker import run_ingest_loop
from webhook_engine.service.routes.deliveries import router as deliveries_router
from webhook_engine.service.routes.emit import router as emit_router
from webhook_engine.service.routes.health import router as health_router
from webhook_engine.service.routes.subscriptions import router as sub_router
from webhook_engine.service.settings import Settings
from webhook_engine.service.worker import run_tick_loop

__all__ = ["create_app", "main"]


def create_app(settings: Settings | None = None) -> FastAPI:
    cfg = settings or Settings()  # type: ignore[call-arg]  # source_key loaded from WHE_SOURCE_KEY env var

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        deps = await build_deps(cfg)
        app.state.deps = deps
        dispatch = asyncio.create_task(run_tick_loop(deps.dispatch_engine, cfg.poll_interval_s))
        ingest = asyncio.create_task(run_ingest_loop(deps.ingestor, cfg.ingest_poll_interval_s))
        try:
            yield
        finally:
            for task in (ingest, dispatch):
                task.cancel()
            await asyncio.gather(ingest, dispatch, return_exceptions=True)
            await deps.dispatch_engine.drain(cfg.drain_timeout_s)
            await deps.aclose()

    app = FastAPI(
        title="Webhook Engine",
        description="Durable, signed, per-tenant webhook delivery microservice",
        version="0.1.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    app.include_router(health_router)
    app.include_router(emit_router, prefix="/v1")
    app.include_router(sub_router, prefix="/v1")
    app.include_router(deliveries_router, prefix="/v1")

    return app


def main() -> None:
    cfg = Settings()  # type: ignore[call-arg]  # source_key loaded from WHE_SOURCE_KEY env var
    uvicorn.run(
        "webhook_engine.service.app:create_app",
        factory=True,
        host=cfg.host,
        port=cfg.port,
        reload=cfg.debug,
        log_level="debug" if cfg.debug else "info",
    )


if __name__ == "__main__":
    main()
