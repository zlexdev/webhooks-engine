"""POST /v1/emit — an HTTP producer path into the same event store.

Auth: ``X-Source-Key: <WHE_SOURCE_KEY>`` header (shared secret).

This is the HTTP-shaped twin of writing a row/document directly into the event
store: it ``append``\\s one :class:`IncomingEvent`, and the ingestor fans it out
on its next tick — the exact same path a DB producer takes. Idempotent on
``event_id`` (the store dedupes the append; the delivery store dedupes by
``(subscription_id, event_id)``).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import BaseModel, Field

from webhook_engine.ingest.events import IncomingEvent
from webhook_engine.service.deps import ServiceDeps, get_deps

__all__ = ["router"]

router = APIRouter(tags=["emit"])


class EmitRequest(BaseModel):
    event: str = Field(..., description="Registered event name, e.g. 'order.paid'")
    event_id: str = Field(
        default_factory=lambda: uuid4().hex,
        description="Idempotency key — appending the same event_id twice is a no-op",
    )
    tenant_id: str | None = Field(default=None)
    schema_version: str = Field(default="1.0")
    data: dict[str, Any] = Field(default_factory=dict)


class EmitResponse(BaseModel):
    accepted: bool
    event_id: str


MAX_BATCH = 50


class BatchEmitRequest(BaseModel):
    events: list[EmitRequest] = Field(
        ...,
        min_length=1,
        max_length=MAX_BATCH,
        description=f"1-{MAX_BATCH} events appended in one write; each idempotent on its event_id",
    )


class BatchEmitResponse(BaseModel):
    accepted: bool
    count: int
    event_ids: list[str]


@router.post(
    "/emit",
    response_model=EmitResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Append an event — the ingestor fans it out to subscriptions",
)
async def emit_event(
    body: EmitRequest,
    x_source_key: str = Header(..., alias="X-Source-Key"),
    deps: ServiceDeps = Depends(get_deps),
) -> EmitResponse:
    if x_source_key != deps.settings.source_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid source key")

    await deps.event_store.append(
        IncomingEvent(
            event_id=body.event_id,
            event=body.event,
            data=body.data,
            tenant_id=body.tenant_id,
            schema_version=body.schema_version,
            created_at=datetime.now(UTC),
        )
    )
    return EmitResponse(accepted=True, event_id=body.event_id)


@router.post(
    "/emit/batch",
    response_model=BatchEmitResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Append up to 50 events in one write — the ingestor fans each out",
)
async def emit_batch(
    body: BatchEmitRequest,
    x_source_key: str = Header(..., alias="X-Source-Key"),
    deps: ServiceDeps = Depends(get_deps),
) -> BatchEmitResponse:
    if x_source_key != deps.settings.source_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid source key")

    now = datetime.now(UTC)
    events = [
        IncomingEvent(
            event_id=item.event_id,
            event=item.event,
            data=item.data,
            tenant_id=item.tenant_id,
            schema_version=item.schema_version,
            created_at=now,
        )
        for item in body.events
    ]
    await deps.event_store.append_many(events)
    return BatchEmitResponse(
        accepted=True,
        count=len(events),
        event_ids=[e.event_id for e in events],
    )
