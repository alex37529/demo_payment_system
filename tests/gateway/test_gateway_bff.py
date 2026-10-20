"""gateway BFF tests (phase 6).

A single in-process ASGI "stub fleet" hosts the downstream endpoints the BFF
calls (intake / profile / notification / orchestrator — their paths don't
collide), and the gateway's `*_BASE_URL`s all point at it. The BFF routes are
exercised end-to-end over the httpx ASGI transport, no live fleet needed.
"""
from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path

import pytest_asyncio
from fastapi import FastAPI
from fastapi.responses import JSONResponse, StreamingResponse
from httpx import ASGITransport, AsyncClient

_REPO_ROOT = Path(__file__).resolve().parents[2]
_GW_SERVICE = _REPO_ROOT / "services" / "gateway" / "gateway.py"
STUB = "http://stub:8080"


def _load_gw():
    spec = importlib.util.spec_from_file_location("gateway_stub", _GW_SERVICE)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


gw = _load_gw()


def _stub_fleet() -> FastAPI:
    app = FastAPI()

    # -- intake --
    @app.post("/v1/payments")
    async def create(body: dict) -> JSONResponse:
        return JSONResponse(status_code=202, content={
            "ok": True, "payment_id": "P-12345",
            "amount": body.get("amount"), "account_id": body.get("account_id")})

    @app.get("/v1/payments/{payment_id}")
    async def get_payment(payment_id: str) -> JSONResponse:
        return JSONResponse({"ok": True, "payment_id": payment_id, "state": "Success"})

    # -- profile --
    users = [
        {"id": "U001", "name": "Alice Alvarez", "email": "alice@example.com",
         "currency": "USD", "balance": 620000},
        {"id": "U002", "name": "Benjamin Baker", "email": "benjamin@example.com",
         "currency": "EUR", "balance": 480000},
    ]

    @app.get("/v1/users")
    async def list_users() -> JSONResponse:
        return JSONResponse(users)

    @app.get("/v1/users/{account_id}")
    async def get_user(account_id: str) -> JSONResponse:
        for u in users:
            if u["id"] == account_id:
                return JSONResponse(u)
        return JSONResponse(status_code=404, content={"ok": False, "error": "UserNotFound"})

    # -- notification --
    payments = [{
        "payment_id": "P-12345", "account_id": "U001", "event": "FundsDebited",
        "amount": {"value": 100, "currency": "USD"},
        "new_balance": {"value": 619900, "currency": "USD"},
        "ts": "2026-10-19T08:00:00Z", "event_id": "e1"}]

    @app.get("/v1/payments")
    async def feed(limit: int = 100) -> JSONResponse:
        # the notification read-feed (same path as intake create but GET vs POST)
        return JSONResponse({"count": len(payments), "payments": payments[:limit]})

    @app.get("/v1/stream")
    async def stream():
        async def gen():
            yield ": keepalive\n\n"
            yield 'data: {"payment_id":"P-12345","account_id":"U001","event":"FundsDebited"}\n\n'
            await asyncio.sleep(0.01)
        return StreamingResponse(gen(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache"})

    # -- orchestrator --
    @app.get("/v1/circuit-breakers")
    async def cb() -> JSONResponse:
        return JSONResponse({"fraud": {"state": "CLOSED", "is_open": False}})

    # -- ledger (balance journal) --
    @app.get("/v1/accounts/{account_id}/transactions")
    async def txs(account_id: str, limit: int = 50) -> JSONResponse:
        if account_id == "U001":
            return JSONResponse({
                "account_id": "U001", "current_balance": 400000, "total": 2,
                "transactions": [
                    {"event_type": "RefundPosted", "amount": 100, "balance_after": 400000},
                    {"event_type": "FundsDebited", "amount": 100, "balance_after": 399900},
                ]})
        return JSONResponse(status_code=404, content={"ok": False, "error": "AccountNotFound"})

    return app


@pytest_asyncio.fixture
async def gateway():
    stub = _stub_fleet()
    gw_app = FastAPI()
    gw.attach_gateway(gw_app)
    # the BFF uses relative base names, so point every base at the stub host
    svc: gw.GatewayService = gw_app.state.gateway
    stub_transport = ASGITransport(app=stub)
    # the BFF's internal calls go to the stub fleet
    svc.client = AsyncClient(transport=ASGITransport(app=stub), base_url=STUB)
    svc.stream_client = AsyncClient(transport=stub_transport, base_url=STUB)
    for attr, base in (("gateway_intake", STUB), ("gateway_profile", STUB),
                       ("gateway_notification", STUB), ("gateway_orchestrator", STUB)):
        setattr(gw_app.state, attr, base)
    svc.intake_base = STUB
    svc.profile_base = STUB
    svc.notification_base = STUB
    svc.orchestrator_base = STUB
    svc.ledger_base = STUB
    gw_app.state.gateway_ledger = STUB

    cm = gw_app.router.lifespan_context(gw_app)
    await cm.__aenter__()
    # the test client talks to the GATEWAY app (which then fans out to the stub)
    client = AsyncClient(transport=ASGITransport(app=gw_app), base_url="http://gateway")
    try:
        yield client, gw_app
    finally:
        await client.aclose()
        await svc.client.aclose()
        await svc.stream_client.aclose()
        await cm.__aexit__(None, None, None)


async def test_users_lists_stub_users(gateway):
    client, _ = gateway
    r = await client.get("/v1/users")
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) == 2
    assert rows[0]["id"] == "U001"
    assert rows[0]["balance"] == 620000


async def test_user_200_when_known(gateway):
    client, _ = gateway
    r = await client.get("/v1/users/U002")
    assert r.status_code == 200
    assert r.json()["currency"] == "EUR"


async def test_user_404_when_unknown(gateway):
    client, _ = gateway
    r = await client.get("/v1/users/U999")
    assert r.status_code == 404


async def test_transactions_forwarded_to_ledger(gateway):
    client, _ = gateway
    r = await client.get("/v1/users/U001/transactions")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total"] == 2
    assert [t["event_type"] for t in body["transactions"]] == ["RefundPosted", "FundsDebited"]


async def test_transactions_404_for_unknown_account(gateway):
    client, _ = gateway
    r = await client.get("/v1/users/NOPE/transactions")
    assert r.status_code == 404


async def test_create_payment_forwarded(gateway):
    client, _ = gateway
    r = await client.post("/v1/payments",
                          json={"amount": 100, "currency": "USD", "account_id": "U001"},
                          headers={"Idempotency-Key": "K1"})
    assert r.status_code == 202
    assert r.json()["ok"] is True
    assert r.json()["payment_id"] == "P-12345"


async def test_get_payment_forwarded(gateway):
    client, _ = gateway
    r = await client.get("/v1/payments/P-12345")
    assert r.status_code == 200
    assert r.json()["payment_id"] == "P-12345"


async def test_status_aggregates_fraud_cb(gateway):
    client, _ = gateway
    r = await client.get("/v1/_status")
    assert r.status_code == 200
    body = r.json()
    assert body["up"] is True
    assert body["circuit_breakers"]["fraud"]["state"] == "CLOSED"
    assert body["circuit_breakers"]["fraud"]["is_open"] is False


async def test_sse_proxy_streams_events(gateway):
    client, _ = gateway
    async with client.stream("GET", "/v1/stream") as resp:
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/event-stream")
        frames = [chunk.decode("utf-8", errors="replace") async for chunk in resp.aiter_bytes()]
        body = "".join(frames)
        assert "keepalive" in body
        assert "FundsDebited" in body
