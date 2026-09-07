"""Typed async client for the HTTP surface of this service.

**It lives here, in the service's own repository, and that is the whole point.**
A client kept by a consumer drifts the moment a route changes: the change and
the client are two commits in two repositories, and nothing fails in between.
Here they are one commit, and :mod:`webhook_engine.service.routes` is a file
away — so a route that gains a field and a client that does not is a diff a
reviewer can see.

The measured reason this module exists: ``docs/usage/microservice.md`` told
receivers to sign the bare body, while :mod:`webhook_engine.signer` signs
``f"{ts}.{event_id}.{sha256_hex(body)}"``, and its endpoint table omitted
``POST /v1/emit/batch`` entirely. Two consumers had already read the doc.

**Errors are the service's own exceptions wherever the meaning is the same** —
:class:`~webhook_engine.exceptions.SubscriptionNotFound`,
:class:`~webhook_engine.exceptions.DeliveryNotFound`,
:class:`~webhook_engine.exceptions.InvalidWebhookTarget` — so a caller that
already handles them in-process handles them over HTTP unchanged. Only failures
that cannot happen in-process are new (:class:`WebhookAuthError`,
:class:`WebhookServiceError`, :class:`WebhookTransportError`,
:class:`WebhookProtocolError`), and they are separate types rather than one
carrying a status because they demand opposite handling: a bad key is a
misconfiguration that retrying cannot fix, a 5xx is worth retrying, and a body
that will not parse means the peer is not this service at all.

No extra is needed: ``httpx`` is a base dependency of this package.

    async with WebhookEngineClient(base_url=..., source_key=...) as client:
        await client.emit(EmitEvent(event="order.paid", data={"order_id": 42}))
"""

from __future__ import annotations

from types import TracebackType
from typing import Any, Final, Self

import httpx
from pydantic import BaseModel, Field

from webhook_engine.enums import DeliveryStatus, InvalidTargetReason, SubscriptionStatus
from webhook_engine.exceptions import (
    DeliveryNotFound,
    InvalidWebhookTarget,
    SubscriptionNotFound,
    WebhookError,
)
from webhook_engine.service.schemas import Page

__all__ = [
    "DEFAULT_TIMEOUT_S",
    "MAX_BATCH",
    "DeliveryView",
    "EmitAccepted",
    "EmitBatchAccepted",
    "EmitEvent",
    "SubscriptionSpec",
    "SubscriptionView",
    "WebhookAuthError",
    "WebhookClientError",
    "WebhookEngineClient",
    "WebhookProtocolError",
    "WebhookServiceError",
    "WebhookTransportError",
]

DEFAULT_TIMEOUT_S: Final = 5.0
"""An emit is an append to the event store, not a delivery attempt — the fan-out
happens on the tick loop afterwards. It has to be shorter than the request that
triggers it, or a slow engine becomes a slow caller."""

MAX_BATCH: Final = 50
"""``routes/emit.py``. Over this the service answers 422, so the batch is
refused here instead, where the caller can still see which batch was too big."""

_TARGET_BLOCKED_PREFIX: Final = "target blocked: "


class WebhookClientError(WebhookError):
    """Base of the failures that exist only when the service is reached over
    HTTP. Subclasses :class:`~webhook_engine.exceptions.WebhookError`, so a
    caller may catch the whole package with one clause."""


class WebhookAuthError(WebhookClientError):
    """401 — ``X-Source-Key`` does not match ``WHE_SOURCE_KEY``.

    A configuration fault, never a transient one: retrying sends the same wrong
    key. Kept apart from :class:`WebhookServiceError` for exactly that reason.
    """


class WebhookServiceError(WebhookClientError):
    """The service answered, with a status this client does not translate.

    ``status`` is carried because 5xx is worth retrying and an untranslated 4xx
    is not; ``detail`` is the response body, truncated. A bare status hides the
    difference between a rejected target and a broken engine.
    """

    def __init__(self, action: str, status: int, detail: str) -> None:
        super().__init__(
            f"{action} failed with {status}: {detail}", action=action, status=status
        )
        self.action = action
        self.status = status
        self.detail = detail


class WebhookTransportError(WebhookClientError):
    """The request never completed — DNS, connect, TLS, timeout.

    Distinct from a 5xx on purpose: nothing was delivered to the service, so a
    non-idempotent call is safe to repeat only if it carried an ``event_id``.
    """

    def __init__(self, action: str, detail: str) -> None:
        super().__init__(f"{action} did not complete: {detail}", action=action)
        self.action = action
        self.detail = detail


class WebhookProtocolError(WebhookClientError):
    """A 2xx whose body is not the shape this version expects.

    Usually means the URL points at something that is not this service — a proxy
    error page, a login redirect — and treating that as an empty result would
    read as "no subscriptions" rather than "wrong host".
    """


class EmitEvent(BaseModel):
    """One event to append.

    ``event_id`` is the idempotency key: appending the same one twice is a
    no-op, which is what makes a retry after :class:`WebhookTransportError`
    safe. Omitted, the service mints a random one — and a retry then fans out
    twice. Pass the id of the fact that caused the event whenever you have it.
    """

    event: str = Field(
        ..., min_length=1, description="Registered event name, e.g. 'order.paid'"
    )
    data: dict[str, Any] = Field(default_factory=dict)
    event_id: str | None = Field(default=None)
    tenant_id: str | None = Field(default=None)
    schema_version: str = Field(default="1.0")

    def wire(self) -> dict[str, Any]:
        """The body the route parses. ``event_id`` is dropped when unset so the
        service's own default applies — sending ``null`` would fail validation
        rather than fall back."""
        body: dict[str, Any] = {
            "event": self.event,
            "data": self.data,
            "schema_version": self.schema_version,
            "tenant_id": self.tenant_id,
        }
        if self.event_id is not None:
            body["event_id"] = self.event_id
        return body


class SubscriptionSpec(BaseModel):
    """A subscription to create. ``secret`` is generated by the service when
    omitted, and either way it comes back exactly once — see
    :attr:`SubscriptionView.secret`."""

    owner_id: str = Field(..., min_length=1, description="Tenant / user identifier")
    url: str = Field(..., min_length=1, description="Target URL — HTTPS in production")
    events: list[str] = Field(..., min_length=1)
    secret: str | None = Field(
        default=None, description="Custom HMAC secret; generated if omitted"
    )


class EmitAccepted(BaseModel):
    accepted: bool
    event_id: str


class EmitBatchAccepted(BaseModel):
    accepted: bool
    count: int
    event_ids: list[str]


class SubscriptionView(BaseModel):
    """``SubscriptionResponse`` as the routes return it."""

    id: str
    owner_id: str
    url: str
    events: list[str]
    status: SubscriptionStatus
    created_at: str
    secret: str | None = None
    """Present ONLY in the answer to :meth:`WebhookEngineClient.create_subscription`.
    ``get`` and ``list`` blank it, so a caller that does not persist it there and
    then cannot recover it — the service stores no readable copy."""


class DeliveryView(BaseModel):
    """One delivery attempt of one subscription.

    Not an event log: this is the attempt history of a single subscriber. What
    the emitting application did is the emitter's own journal, and that — not
    the webhook — is its source of truth.
    """

    delivery_id: str
    subscription_id: str
    event_name: str
    event_id: str
    status: DeliveryStatus
    attempts: int
    target_url: str
    redelivery_of: str | None = None


class WebhookEngineClient:
    """One instance per process; it holds a connection pool.

    Pass an ``httpx.AsyncClient`` to share a pool with the caller's own; leave
    it out and the client owns one, which :meth:`aclose` (or ``async with``)
    releases. A client that owns its pool and is never closed leaks sockets
    silently.
    """

    def __init__(
        self,
        *,
        base_url: str,
        source_key: str,
        client: httpx.AsyncClient | None = None,
        timeout_s: float = DEFAULT_TIMEOUT_S,
    ) -> None:
        if not base_url:
            raise ValueError("base_url is required")
        if not source_key:
            # Not a warning and not a default: every route answers 401 without
            # it, so an empty key turns every call into an auth failure that
            # reads like a server problem.
            raise ValueError(
                "source_key is required — every route rejects a missing X-Source-Key"
            )
        self._base_url = base_url.rstrip("/")
        self._source_key = source_key
        self._timeout_s = timeout_s
        self._client = client or httpx.AsyncClient(timeout=timeout_s)
        self._owns_client = client is None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Closes the pool only when this client created it."""
        if self._owns_client:
            await self._client.aclose()

    async def emit(self, event: EmitEvent) -> EmitAccepted:
        """Append one event. The service resolves subscriptions itself, so this
        call never names a URL — which is why no SSRF guard belongs on the
        caller's side of it."""
        return EmitAccepted.model_validate(
            await self._post("/v1/emit", event.wire()),
        )

    async def emit_batch(self, events: list[EmitEvent]) -> EmitBatchAccepted:
        """Up to :data:`MAX_BATCH` events in one write, each idempotent on its
        own ``event_id``. An empty list is a no-op rather than a request."""
        if not events:
            return EmitBatchAccepted(accepted=True, count=0, event_ids=[])
        if len(events) > MAX_BATCH:
            raise ValueError(
                f"{len(events)} events, the service caps a batch at {MAX_BATCH}"
            )
        return EmitBatchAccepted.model_validate(
            await self._post("/v1/emit/batch", {"events": [e.wire() for e in events]}),
        )

    async def create_subscription(self, spec: SubscriptionSpec) -> SubscriptionView:
        """The returned ``secret`` is readable once. Persist it in the same
        transaction that records the subscription, or it is gone."""
        return SubscriptionView.model_validate(
            await self._post("/v1/subscriptions/create", spec.model_dump()),
        )

    async def list_subscriptions(
        self, *, owner_id: str, limit: int = 50, offset: int = 0
    ) -> Page[SubscriptionView]:
        body = await self._get(
            "/v1/subscriptions/list",
            {"owner_id": owner_id, "limit": limit, "offset": offset},
        )
        return Page[SubscriptionView].model_validate(body)

    async def get_subscription(self, subscription_id: str) -> SubscriptionView:
        body = await self._get("/v1/subscriptions/get", {"id": subscription_id})
        return SubscriptionView.model_validate(body)

    async def delete_subscription(self, subscription_id: str) -> None:
        await self._post("/v1/subscriptions/delete", {"id": subscription_id})

    async def pause_subscription(self, subscription_id: str) -> None:
        """Stops delivery without losing the subscription or its secret."""
        await self._post("/v1/subscriptions/pause", {"id": subscription_id})

    async def resume_subscription(self, subscription_id: str) -> None:
        await self._post("/v1/subscriptions/resume", {"id": subscription_id})

    async def ping_subscription(self, subscription_id: str) -> DeliveryView:
        """Sends a synthetic delivery — the target is re-checked, so this raises
        :class:`~webhook_engine.exceptions.InvalidWebhookTarget` when the URL has
        since resolved somewhere it may not reach."""
        body = await self._post(
            "/v1/subscriptions/ping", {"subscription_id": subscription_id}
        )
        return DeliveryView.model_validate(body)

    async def list_deliveries(
        self, *, subscription_id: str, limit: int = 50, offset: int = 0
    ) -> Page[DeliveryView]:
        body = await self._get(
            "/v1/deliveries/list",
            {"subscription_id": subscription_id, "limit": limit, "offset": offset},
        )
        return Page[DeliveryView].model_validate(body)

    async def get_delivery(self, delivery_id: str) -> DeliveryView:
        body = await self._get("/v1/deliveries/get", {"id": delivery_id})
        return DeliveryView.model_validate(body)

    async def redeliver(self, delivery_id: str) -> DeliveryView:
        """Replays a past delivery as a new one. The answer is the CLONE — its
        ``delivery_id`` differs from the argument, and ``redelivery_of`` names
        the original."""
        body = await self._post("/v1/deliveries/redeliver", {"id": delivery_id})
        return DeliveryView.model_validate(body)

    async def ready(self) -> bool:
        """200 only when the service's own backend is reachable.

        The one method that answers instead of raising: a readiness probe that
        raises cannot report "not ready".
        """
        try:
            response = await self._request("GET", "/ready", params=None, json=None)
        except WebhookClientError:
            return False
        return response.status_code == httpx.codes.OK

    async def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        return _json_object(
            path, await self._request("GET", path, params=params, json=None)
        )

    async def _post(self, path: str, json: dict[str, Any]) -> dict[str, Any]:
        return _json_object(
            path, await self._request("POST", path, params=None, json=json)
        )

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None,
        json: dict[str, Any] | None,
    ) -> httpx.Response:
        try:
            response = await self._client.request(
                method,
                f"{self._base_url}{path}",
                params=params,
                json=json,
                headers={"X-Source-Key": self._source_key},
                timeout=self._timeout_s,
            )
        except httpx.HTTPError as exc:
            raise WebhookTransportError(path, str(exc)) from exc
        if response.status_code >= httpx.codes.BAD_REQUEST:
            raise _translate(path, response)
        return response


def _translate(path: str, response: httpx.Response) -> WebhookError:
    """The service's HTTP answer as the exception it was raised from.

    Matching on ``detail`` and not on the status alone is deliberate: 404 covers
    two different missing things and 422 covers a blocked target, and a caller
    that has to re-parse a string to tell them apart will eventually not.
    """
    detail = _detail(response)
    status = response.status_code
    if status == httpx.codes.UNAUTHORIZED:
        return WebhookAuthError(f"{path}: {detail}")
    if status == httpx.codes.NOT_FOUND:
        if "subscription" in detail:
            return SubscriptionNotFound(detail)
        if "delivery" in detail:
            return DeliveryNotFound(detail)
    if status == httpx.codes.UNPROCESSABLE_ENTITY and detail.startswith(
        _TARGET_BLOCKED_PREFIX
    ):
        raw = detail.removeprefix(_TARGET_BLOCKED_PREFIX).strip()
        try:
            reason = InvalidTargetReason(raw)
        except ValueError:
            # A reason this version does not know is still a rejected target —
            # reporting it as a generic 422 would send the caller looking at its
            # own request body instead of at the URL.
            return WebhookServiceError(path, status, detail)
        return InvalidWebhookTarget(reason)
    return WebhookServiceError(path, status, detail)


def _detail(response: httpx.Response) -> str:
    """FastAPI puts the message under ``detail``; anything else (a proxy's error
    page) falls back to the raw text, truncated."""
    try:
        parsed: Any = response.json()
    except ValueError:
        return response.text[:500]
    if isinstance(parsed, dict):
        detail = parsed.get("detail")
        if isinstance(detail, str):
            return detail
    return response.text[:500]


def _json_object(path: str, response: httpx.Response) -> dict[str, Any]:
    if not response.content:
        return {}
    try:
        parsed: Any = response.json()
    except ValueError as exc:
        raise WebhookProtocolError(f"{path}: response is not JSON") from exc
    if not isinstance(parsed, dict):
        raise WebhookProtocolError(
            f"{path}: expected a JSON object, got {type(parsed).__name__}"
        )
    return parsed
