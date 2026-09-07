"""The client against a stubbed transport.

These run without a server: `httpx.MockTransport` answers, so what is under
test is the wire format and the error translation, not the network.

The cases were chosen by asking what a WRONG client would still pass. A test
that only checks the happy path passes with every status collapsed into one
exception, and with `event_id` silently dropped — the two defects that made this
module worth writing.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from webhook_engine.client import (
    MAX_BATCH,
    EmitEvent,
    SubscriptionSpec,
    WebhookAuthError,
    WebhookEngineClient,
    WebhookProtocolError,
    WebhookServiceError,
    WebhookTransportError,
)
from webhook_engine.enums import DeliveryStatus, InvalidTargetReason, SubscriptionStatus
from webhook_engine.exceptions import (
    DeliveryNotFound,
    InvalidWebhookTarget,
    SubscriptionNotFound,
)

SOURCE_KEY = "k-123"

_SUBSCRIPTION: dict[str, Any] = {
    "id": "sub-1",
    "owner_id": "acme",
    "url": "https://example.test/hook",
    "events": ["order.paid"],
    "status": "active",
    "created_at": "2026-09-07T12:00:00+00:00",
    "secret": "whsec_abc",
}

_DELIVERY: dict[str, Any] = {
    "delivery_id": "d-1",
    "subscription_id": "sub-1",
    "event_name": "order.paid",
    "event_id": "e-1",
    "status": "dead_lettered",
    "attempts": 6,
    "target_url": "https://example.test/hook",
    "redelivery_of": None,
}


_Handler = Callable[[httpx.Request], httpx.Response]


def _client(handler: _Handler) -> WebhookEngineClient:
    return WebhookEngineClient(
        base_url="https://engine.test",
        source_key=SOURCE_KEY,
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )


def _json(payload: dict[str, Any], status: int = 200) -> httpx.Response:
    return httpx.Response(status, json=payload)


def _error(status: int, detail: str) -> httpx.Response:
    return httpx.Response(status, json={"detail": detail})


@pytest.mark.parametrize(("base_url", "source_key"), [("", "k"), ("https://x", "")])
def test_refuses_to_build_without_both_halves(base_url: str, source_key: str) -> None:
    """A missing key is answered by every route with 401, which reads like a
    server fault. Refusing at construction names the real cause once."""
    with pytest.raises(ValueError):
        WebhookEngineClient(base_url=base_url, source_key=source_key)


@pytest.mark.asyncio
async def test_emit_sends_the_source_key_and_the_route_body() -> None:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["key"] = request.headers.get("X-Source-Key")
        seen["body"] = json.loads(request.content)
        return _json({"accepted": True, "event_id": "e-1"}, status=202)

    async with _client(handler) as client:
        ack = await client.emit(
            EmitEvent(event="order.paid", data={"order_id": 42}, tenant_id="acme")
        )

    assert ack.accepted is True
    assert seen["path"] == "/v1/emit"
    assert seen["key"] == SOURCE_KEY
    assert seen["body"] == {
        "event": "order.paid",
        "data": {"order_id": 42},
        "schema_version": "1.0",
        "tenant_id": "acme",
    }


@pytest.mark.asyncio
async def test_emit_omits_event_id_when_unset_and_sends_it_when_given() -> None:
    """Two halves of one contract. `null` would fail the route's validation, and
    a dropped id turns a retry into a second fan-out."""
    bodies: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(json.loads(request.content))
        return _json({"accepted": True, "event_id": "e"}, status=202)

    async with _client(handler) as client:
        await client.emit(EmitEvent(event="a"))
        await client.emit(EmitEvent(event="a", event_id="fact-7"))

    assert "event_id" not in bodies[0]
    assert bodies[1]["event_id"] == "fact-7"


@pytest.mark.asyncio
async def test_empty_batch_makes_no_request() -> None:
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - must not run
        raise AssertionError("an empty batch must not reach the network")

    async with _client(handler) as client:
        ack = await client.emit_batch([])

    assert ack.count == 0


@pytest.mark.asyncio
async def test_oversized_batch_is_refused_before_the_network() -> None:
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - must not run
        raise AssertionError("an oversized batch must not reach the network")

    async with _client(handler) as client:
        with pytest.raises(ValueError, match=str(MAX_BATCH)):
            await client.emit_batch([EmitEvent(event="a")] * (MAX_BATCH + 1))


@pytest.mark.asyncio
async def test_create_subscription_returns_the_one_readable_secret() -> None:
    async with _client(lambda _: _json(_SUBSCRIPTION, status=201)) as client:
        sub = await client.create_subscription(
            SubscriptionSpec(
                owner_id="acme", url="https://example.test/hook", events=["order.paid"]
            )
        )

    assert sub.secret == "whsec_abc"
    assert sub.status is SubscriptionStatus.ACTIVE


@pytest.mark.asyncio
async def test_list_parses_the_page_envelope_not_a_bare_list() -> None:
    page = {"items": [_SUBSCRIPTION], "total": 1, "limit": 50, "offset": 0}

    async with _client(lambda _: _json(page)) as client:
        result = await client.list_subscriptions(owner_id="acme")

    assert result.total == 1
    assert result.items[0].id == "sub-1"


@pytest.mark.asyncio
async def test_delivery_status_arrives_as_the_enum() -> None:
    async with _client(lambda _: _json(_DELIVERY)) as client:
        delivery = await client.get_delivery("d-1")

    assert delivery.status is DeliveryStatus.DEAD_LETTERED
    assert delivery.attempts == 6


@pytest.mark.asyncio
async def test_bad_key_is_its_own_error() -> None:
    async with _client(lambda _: _error(401, "invalid source key")) as client:
        with pytest.raises(WebhookAuthError):
            await client.get_delivery("d-1")


@pytest.mark.asyncio
async def test_the_two_kinds_of_404_are_told_apart() -> None:
    """One status, two missing things. A caller closing the owning row on a
    missing subscription must not close it on a missing delivery."""
    async with _client(lambda _: _error(404, "subscription not found")) as client:
        with pytest.raises(SubscriptionNotFound):
            await client.get_subscription("sub-1")

    async with _client(lambda _: _error(404, "delivery not found")) as client:
        with pytest.raises(DeliveryNotFound):
            await client.get_delivery("d-1")


@pytest.mark.asyncio
async def test_blocked_target_carries_the_reason() -> None:
    async with _client(lambda _: _error(422, "target blocked: private_ip")) as client:
        with pytest.raises(InvalidWebhookTarget) as caught:
            await client.ping_subscription("sub-1")

    assert caught.value.reason is InvalidTargetReason.PRIVATE_IP


@pytest.mark.asyncio
async def test_unknown_block_reason_stays_a_service_error() -> None:
    """A reason this version does not know must not be forced into the enum —
    the caller would then act on the wrong reason rather than on none."""
    async with _client(lambda _: _error(422, "target blocked: from_the_future")) as client:
        with pytest.raises(WebhookServiceError):
            await client.ping_subscription("sub-1")


@pytest.mark.asyncio
async def test_server_error_keeps_its_status_for_the_retry_decision() -> None:
    async with _client(lambda _: _error(503, "backend down")) as client:
        with pytest.raises(WebhookServiceError) as caught:
            await client.emit(EmitEvent(event="a"))

    assert caught.value.status == 503


@pytest.mark.asyncio
async def test_a_request_that_never_completed_is_not_a_server_error() -> None:
    """Nothing reached the service, so the caller's retry rules differ."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route to host")

    async with _client(handler) as client:
        with pytest.raises(WebhookTransportError):
            await client.emit(EmitEvent(event="a"))


@pytest.mark.asyncio
async def test_a_200_that_is_not_this_service_is_not_an_empty_result() -> None:
    """A proxy's HTML error page answers 200. Parsed leniently it would read as
    "no subscriptions" instead of "wrong host"."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<html>gateway</html>")

    async with _client(handler) as client:
        with pytest.raises(WebhookProtocolError):
            await client.list_subscriptions(owner_id="acme")


@pytest.mark.asyncio
async def test_ready_reports_false_instead_of_raising() -> None:
    async with _client(lambda _: _error(503, "backend down")) as client:
        assert await client.ready() is False
