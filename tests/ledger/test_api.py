"""API integration tests (SQLite-backed): wiring, idempotency, error mapping.

Exercises the real FastAPI routes -> use-cases -> SQLAlchemy path. The ASGI
lifespan (startup handlers) is invoked manually; each test gets a fresh app +
in-memory DB (function scope).
"""
from __future__ import annotations

import pytest_asyncio
from httpx import ASGITransport, AsyncClient


@pytest_asyncio.fixture
async def client(monkeypatch):
    monkeypatch.setenv("LEDGER_DSN", "sqlite+aiosqlite:///:memory:")
    monkeypatch.setenv("LEDGER_ENABLE_CONSUMER", "0")
    from services.ledger.api import attach_ledger, start, stop
    from services.shared_app import build_app

    app = build_app("ledger-test")
    attach_ledger(app)
    await start(app)

    from services.ledger.infrastructure.models import AccountRow
    sf = app.state.ledger_runner.session_factory
    async with sf() as s:
        s.add(AccountRow(id="A1", owner_name="Bob", currency="USD", balance=500, reserved=0, version=1))
        await s.commit()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield app, c
    await stop(app)


async def test_debit_then_get_balance(client):
    app, c = client
    r = await c.post("/v1/accounts/A1/debit", json={"amount": 100, "currency": "USD", "payment_id": "P1"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["applied"] is True
    assert body["event_type"] == "FundsDebited"
    g = await c.get("/v1/accounts/A1")
    assert g.status_code == 200
    assert g.json()["balance"] == {"value": 400, "currency": "USD"}


async def test_debit_replay_is_idempotent(client):
    app, c = client
    for _ in range(2):
        r = await c.post("/v1/accounts/A1/debit", json={"amount": 100, "currency": "USD", "payment_id": "R"})
        assert r.status_code == 200, r.text
    assert (await c.get("/v1/accounts/A1")).json()["balance"]["value"] == 400


async def test_debit_insufficient_is_409(client):
    app, c = client
    r = await c.post("/v1/accounts/A1/debit", json={"amount": 600, "currency": "USD", "payment_id": "P2"})
    assert r.status_code == 409
    assert r.json()["error"] == "InsufficientFundsError"
    assert (await c.get("/v1/accounts/A1")).json()["balance"]["value"] == 500


async def test_debit_unknown_account_is_404(client):
    app, c = client
    r = await c.post("/v1/accounts/NOPE/debit", json={"amount": 1, "currency": "USD", "payment_id": "P3"})
    assert r.status_code == 404


async def test_reserve_holds_funds(client):
    app, c = client
    r = await c.post("/v1/accounts/A1/reserve", json={"amount": 200, "currency": "USD", "payment_id": "R1"})
    assert r.status_code == 200, r.text
    a = (await c.get("/v1/accounts/A1")).json()
    assert a["balance"]["value"] == 500      # reserve holds, balance unchanged
    assert a["reserved"]["value"] == 200
    assert a["available"]["value"] == 300


async def test_reserve_then_debit_overdraw_is_409(client):
    app, c = client
    await c.post("/v1/accounts/A1/reserve", json={"amount": 200, "currency": "USD", "payment_id": "R1"})
    r = await c.post("/v1/accounts/A1/debit", json={"amount": 400, "currency": "USD", "payment_id": "P4"})
    assert r.status_code == 409


async def test_refund_and_cancel_reservation(client):
    app, c = client
    await c.post("/v1/accounts/A1/debit", json={"amount": 100, "currency": "USD", "payment_id": "D1"})
    await c.post("/v1/accounts/A1/refund", json={"amount": 100, "currency": "USD", "payment_id": "F1"})
    assert (await c.get("/v1/accounts/A1")).json()["balance"]["value"] == 500

    await c.post("/v1/accounts/A1/reserve", json={"amount": 150, "currency": "USD", "payment_id": "R2"})
    assert (await c.get("/v1/accounts/A1")).json()["reserved"]["value"] == 150
    await c.post("/v1/accounts/A1/cancel-reservation", json={"amount": 150, "currency": "USD", "payment_id": "CR1"})
    assert (await c.get("/v1/accounts/A1")).json()["reserved"]["value"] == 0


async def test_get_missing_account_is_404(client):
    app, c = client
    assert (await c.get("/v1/accounts/NOPE")).status_code == 404
