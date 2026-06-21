"""Subscription management — /v1/subscriptions/*.

Subscriptions map an ``owner_id`` + target ``url`` to a set of event names.
When an event is emitted, all active subscriptions for that event receive a
signed HTTP POST to their ``url``.

The ``secret`` returned on creation must be stored by the caller — it is never
returned again. Use it to verify ``X-Webhook-Signature-256`` on deliveries.

HTTP discipline (backend skill): POST/GET only, the path carries the verb,
mutations take the id in the JSON body, reads take it in the query string.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import BaseModel, Field, HttpUrl

from webhook_engine.service.deps import ServiceDeps, get_deps
from webhook_engine.service.schemas import Page
from webhook_engine.service.subscription_store import Subscription

__all__ = ["router"]

router = APIRouter(prefix="/subscriptions", tags=["subscriptions"])


class CreateSubscriptionRequest(BaseModel):
    owner_id: str = Field(..., description="Tenant / user identifier")
    url: HttpUrl = Field(..., description="Target URL — must be HTTPS in production")
    events: list[str] = Field(..., min_length=1, description="Event names to subscribe to")
    secret: str | None = Field(default=None, description="Custom HMAC secret; generated if omitted")


class SubscriptionIdRequest(BaseModel):
    id: str = Field(..., description="Subscription id")


class SubscriptionResponse(BaseModel):
    id: str
    owner_id: str
    url: str
    events: list[str]
    status: str
    created_at: str
    secret: str | None = None


def _to_resp(sub: Subscription, include_secret: bool = False) -> SubscriptionResponse:
    return SubscriptionResponse(
        id=sub.id,
        owner_id=sub.owner_id,
        url=sub.url,
        events=list(sub.events),
        status=sub.status.value,
        created_at=sub.created_at.isoformat(),
        secret=sub.secret if include_secret else None,
    )


def _auth(key: str, deps: ServiceDeps) -> None:
    if key != deps.settings.source_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid source key")


async def _require(deps: ServiceDeps, sub_id: str) -> Subscription:
    sub = await deps.sub_store.get(sub_id)
    if not sub:
        raise HTTPException(status_code=404, detail="subscription not found")
    return sub


@router.post(
    "/create",
    response_model=SubscriptionResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a subscription",
)
async def create_subscription(
    body: CreateSubscriptionRequest,
    x_source_key: str = Header(..., alias="X-Source-Key"),
    deps: ServiceDeps = Depends(get_deps),
) -> SubscriptionResponse:
    _auth(x_source_key, deps)
    try:
        sub = await deps.sub_store.create(
            owner_id=body.owner_id,
            url=str(body.url),
            events=body.events,
            secret=body.secret,
        )
    except (ValueError, OSError) as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)
        ) from exc
    return _to_resp(sub, include_secret=True)


@router.get(
    "/list",
    response_model=Page[SubscriptionResponse],
    summary="List subscriptions for an owner",
)
async def list_subscriptions(
    owner_id: str,
    limit: int = 50,
    offset: int = 0,
    x_source_key: str = Header(..., alias="X-Source-Key"),
    deps: ServiceDeps = Depends(get_deps),
) -> Page[SubscriptionResponse]:
    _auth(x_source_key, deps)
    subs = await deps.sub_store.list_for_owner(owner_id)
    window = subs[offset : offset + limit]
    return Page(
        items=[_to_resp(s) for s in window],
        total=len(subs),
        limit=limit,
        offset=offset,
    )


@router.get(
    "/get",
    response_model=SubscriptionResponse,
    summary="Get a subscription by id",
)
async def get_subscription(
    id: str,
    x_source_key: str = Header(..., alias="X-Source-Key"),
    deps: ServiceDeps = Depends(get_deps),
) -> SubscriptionResponse:
    _auth(x_source_key, deps)
    return _to_resp(await _require(deps, id))


@router.post(
    "/delete",
    response_model=SubscriptionResponse,
    summary="Delete a subscription",
)
async def delete_subscription(
    body: SubscriptionIdRequest,
    x_source_key: str = Header(..., alias="X-Source-Key"),
    deps: ServiceDeps = Depends(get_deps),
) -> SubscriptionResponse:
    _auth(x_source_key, deps)
    sub = await _require(deps, body.id)
    await deps.sub_store.delete(body.id)
    return _to_resp(sub)


@router.post(
    "/pause",
    response_model=SubscriptionResponse,
    summary="Pause delivery for a subscription",
)
async def pause_subscription(
    body: SubscriptionIdRequest,
    x_source_key: str = Header(..., alias="X-Source-Key"),
    deps: ServiceDeps = Depends(get_deps),
) -> SubscriptionResponse:
    _auth(x_source_key, deps)
    await deps.sub_store.pause(body.id)
    return _to_resp(await _require(deps, body.id))


@router.post(
    "/resume",
    response_model=SubscriptionResponse,
    summary="Resume delivery for a paused subscription",
)
async def resume_subscription(
    body: SubscriptionIdRequest,
    x_source_key: str = Header(..., alias="X-Source-Key"),
    deps: ServiceDeps = Depends(get_deps),
) -> SubscriptionResponse:
    _auth(x_source_key, deps)
    await deps.sub_store.resume(body.id)
    return _to_resp(await _require(deps, body.id))
