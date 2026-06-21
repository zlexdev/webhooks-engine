"""End-to-end smoke tests over the live HTTP surface.

Black-box: every assertion goes through a real HTTP request to a running
service. Covers the happy-path lifecycle, auth, the Page envelope, and the
POST/GET verb-in-path discipline the backend skill mandates.
"""

from __future__ import annotations

from uuid import uuid4

import httpx


def test_health_ok(client: httpx.Client) -> None:
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_ready_reports(client: httpx.Client) -> None:
    # readiness is 200 when the backend is reachable, 503 otherwise — both are a
    # valid *report*, so the contract is "answers with a status field".
    r = client.get("/ready")
    assert r.status_code in (200, 503)
    assert "status" in r.json()


def test_auth_rejected_on_wrong_key(client: httpx.Client) -> None:
    r = client.get(
        "/v1/subscriptions/list",
        params={"owner_id": "nobody"},
        headers={"X-Source-Key": "wrong-key"},
    )
    assert r.status_code == 401


def test_list_returns_page_envelope(client: httpx.Client, auth: dict[str, str]) -> None:
    r = client.get("/v1/subscriptions/list", params={"owner_id": "nobody"}, headers=auth)
    assert r.status_code == 200
    body = r.json()
    assert set(body) >= {"items", "total", "limit", "offset"}
    assert isinstance(body["items"], list)


def test_full_lifecycle(client: httpx.Client, auth: dict[str, str]) -> None:
    owner = f"smoke-{uuid4().hex[:8]}"

    # create — secret is returned exactly once
    created = client.post(
        "/v1/subscriptions/create",
        json={
            "owner_id": owner,
            "url": "https://example.com/webhooks",
            "events": ["order.paid"],
        },
        headers=auth,
    )
    assert created.status_code == 201, created.text
    sub = created.json()
    sub_id = sub["id"]
    assert sub["secret"], "secret must be returned on create"

    try:
        # list — the new sub shows up inside a Page
        listed = client.get(
            "/v1/subscriptions/list", params={"owner_id": owner}, headers=auth
        ).json()
        assert listed["total"] >= 1
        assert any(s["id"] == sub_id for s in listed["items"])
        # secret is never echoed on list
        assert all(s["secret"] is None for s in listed["items"])

        # get — id in the query string
        got = client.get("/v1/subscriptions/get", params={"id": sub_id}, headers=auth)
        assert got.status_code == 200
        assert got.json()["id"] == sub_id

        # ping — synchronous synthetic delivery, then it appears in the page
        pinged = client.post(
            "/v1/subscriptions/ping", json={"subscription_id": sub_id}, headers=auth
        )
        assert pinged.status_code == 202, pinged.text
        delivery_id = pinged.json()["delivery_id"]

        deliveries = client.get(
            "/v1/deliveries/list", params={"subscription_id": sub_id}, headers=auth
        ).json()
        assert any(d["delivery_id"] == delivery_id for d in deliveries["items"])

        # pause / resume flip the status
        paused = client.post(
            "/v1/subscriptions/pause", json={"id": sub_id}, headers=auth
        )
        assert paused.json()["status"] == "paused"
        resumed = client.post(
            "/v1/subscriptions/resume", json={"id": sub_id}, headers=auth
        )
        assert resumed.json()["status"] == "active"
    finally:
        deleted = client.post(
            "/v1/subscriptions/delete", json={"id": sub_id}, headers=auth
        )
        assert deleted.status_code == 200

    # gone after delete
    after = client.get("/v1/subscriptions/get", params={"id": sub_id}, headers=auth)
    assert after.status_code == 404


def test_emit_accepted(client: httpx.Client, auth: dict[str, str]) -> None:
    r = client.post(
        "/v1/emit",
        json={"event": "order.paid", "tenant_id": "smoke", "data": {"order_id": 1}},
        headers=auth,
    )
    assert r.status_code == 202
    assert r.json()["accepted"] is True


def test_http_discipline_no_delete_verb(client: httpx.Client, auth: dict[str, str]) -> None:
    # DELETE is not a registered method anywhere — the verb lives in the path.
    r = client.request("DELETE", "/v1/subscriptions/create", headers=auth)
    assert r.status_code == 405


def test_http_discipline_no_verbless_resource(client: httpx.Client, auth: dict[str, str]) -> None:
    # the old REST-style verbless route must not exist
    r = client.get("/v1/subscriptions", params={"owner_id": "x"}, headers=auth)
    assert r.status_code == 404
