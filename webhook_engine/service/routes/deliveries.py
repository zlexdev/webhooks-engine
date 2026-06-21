"""Delivery operations — inspection, manual redelivery, test ping.

Thin HTTP layer over :class:`DeliveryAdminService`. Lets an operator see what
was delivered to a subscriber, replay a past delivery, or fire a synthetic
``webhook.ping`` so a subscriber can verify their endpoint end-to-end.

HTTP discipline (backend skill): POST/GET only, verb in the path, id in the
query string for reads and in the JSON body for mutations.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import BaseModel, Field

from webhook_engine.exceptions import DeliveryNotFound, InvalidWebhookTarget, SubscriptionNotFound
from webhook_engine.service.delivery_admin import DeliveryAdminService
from webhook_engine.service.deps import ServiceDeps, get_deps
from webhook_engine.service.schemas import Page
from webhook_engine.types import DeliveryRecord

__all__ = ["router"]

router = APIRouter(tags=["deliveries"])


class DeliveryView(BaseModel):
    delivery_id: str
    subscription_id: str
    event_name: str
    event_id: str
    status: str
    attempts: int
    target_url: str
    redelivery_of: str | None


class DeliveryIdRequest(BaseModel):
    id: str = Field(..., description="Delivery id")


class SubscriptionIdRequest(BaseModel):
    subscription_id: str = Field(..., description="Subscription id to ping")


def _view(record: DeliveryRecord) -> DeliveryView:
    return DeliveryView(
        delivery_id=record.delivery_id,
        subscription_id=record.subscription_id,
        event_name=record.event_name,
        event_id=record.event_id,
        status=record.status.value,
        attempts=record.attempts,
        target_url=record.target_url,
        redelivery_of=record.redelivery_of,
    )


def _admin(deps: ServiceDeps) -> DeliveryAdminService:
    return DeliveryAdminService(deps.delivery_store, deps.sub_store, deps.ssrf_guard)


def _auth(key: str, deps: ServiceDeps) -> None:
    if key != deps.settings.source_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid source key")


@router.get(
    "/deliveries/list",
    response_model=Page[DeliveryView],
    summary="Recent deliveries for a subscription",
)
async def list_deliveries(
    subscription_id: str,
    limit: int = 50,
    offset: int = 0,
    x_source_key: str = Header(..., alias="X-Source-Key"),
    deps: ServiceDeps = Depends(get_deps),
) -> Page[DeliveryView]:
    _auth(x_source_key, deps)
    records = await _admin(deps).list_for_subscription(subscription_id, offset + limit)
    window = records[offset : offset + limit]
    return Page(
        items=[_view(r) for r in window],
        total=len(records),
        limit=limit,
        offset=offset,
    )


@router.get(
    "/deliveries/get",
    response_model=DeliveryView,
    summary="Inspect a single delivery by id",
)
async def get_delivery(
    id: str,
    x_source_key: str = Header(..., alias="X-Source-Key"),
    deps: ServiceDeps = Depends(get_deps),
) -> DeliveryView:
    _auth(x_source_key, deps)
    try:
        record = await _admin(deps).get(id)
    except DeliveryNotFound as exc:
        raise HTTPException(status_code=404, detail="delivery not found") from exc
    return _view(record)


@router.post(
    "/deliveries/redeliver",
    response_model=DeliveryView,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Replay a past delivery",
)
async def redeliver(
    body: DeliveryIdRequest,
    x_source_key: str = Header(..., alias="X-Source-Key"),
    deps: ServiceDeps = Depends(get_deps),
) -> DeliveryView:
    _auth(x_source_key, deps)
    try:
        clone = await _admin(deps).redeliver(body.id)
    except DeliveryNotFound as exc:
        raise HTTPException(status_code=404, detail="delivery not found") from exc
    except InvalidWebhookTarget as exc:
        raise HTTPException(status_code=422, detail=f"target blocked: {exc.reason.value}") from exc
    return _view(clone)


@router.post(
    "/subscriptions/ping",
    response_model=DeliveryView,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Send a synthetic test delivery to a subscription",
)
async def ping(
    body: SubscriptionIdRequest,
    x_source_key: str = Header(..., alias="X-Source-Key"),
    deps: ServiceDeps = Depends(get_deps),
) -> DeliveryView:
    _auth(x_source_key, deps)
    try:
        record = await _admin(deps).ping(body.subscription_id)
    except SubscriptionNotFound as exc:
        raise HTTPException(status_code=404, detail="subscription not found") from exc
    except InvalidWebhookTarget as exc:
        raise HTTPException(status_code=422, detail=f"target blocked: {exc.reason.value}") from exc
    return _view(record)
