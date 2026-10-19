"""Orchestrator API tests — FastAPI wiring, lifespan, routes (hermetic: no Kafka)."""
from __future__ import annotations

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from services.orchestrator.api import attach_orchestrator, start, stop
from services.orchestrator.infrastructure.models import SagaRow
from services.shared_app import build_app


@pytest_asyncio.fixture
async def client(monkeypatch):
    monkeypatch.setenv("ORCHESTRATOR_DSN", "sqlite+aiosqlite:///:memory:")
    monkeypatch.setenv("ORCH_ENABLE_CONSUMER", "0")   # no Kafka needed
    monkeypatch.setenv("ORCH_WATCHDOG_S", "0")       # disable the sweep task
    monkeypatch.setenv("FRAUD_URL", "http://127.0.0.1:9")  # unroutable; not called
    monkeypatch.setenv("KAFKA_BROKERS", "127.0.0.1:9")

    app = build_app("orchestrator-test")
    attach_orchestrator(app)
    await start(app)

    sf = app.state.orchestrator_repo.session_factory
    async with sf() as s:
        for pid in (10, 20):
            s.add(SagaRow(
                saga_id=f"S{pid}", payment_id=f"P{pid}", state="Success",
                history=[{"step": "check", "cmd": "CheckLimits", "event": "LimitsChecked",
                          "ts": "x", "ok": True, "err": ""}],
                payload={"account_id": "A1", "amount": {"value": 10, "currency": "USD"}},
                compensation=[]))
            s.add(SagaRow(saga_id=f"S19{pid}", payment_id=f"PAY19{pid}",
                          state="Failed", history=[], payload={}, compensation=[]))
        await s.commit()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield app, c
    await stop(app)


async def test_get_saga_by_id(client):
    app, c = client
    r = await c.get("/v1/sagas/S10")
    assert r.status_code == 200
    body = r.json()
    assert body["payment_id"] == "P10"
    assert body["state"] == "Success"
    assert body["history"][0]["step"] == "check"
    assert body["compensation"] == []
    assert "reservation_confirmed" in body


async def test_get_saga_by_payment(client):
    app, c = client
    r = await c.get("/v1/sagas/by-payment/P10")
    assert r.status_code == 200
    assert r.json()["id"] == "S10"


async def test_get_saga_404(client):
    app, c = client
    assert (await c.get("/v1/sagas/NONE")).status_code == 404
    assert (await c.get("/v1/sagas/by-payment/P999")).status_code == 404


async def test_circuit_breakers_endpoint(client):
    app, c = client
    r = await c.get("/v1/circuit-breakers")
    assert r.status_code == 200
    fraud = r.json()["fraud"]
    assert "state" in fraud and "is_open" in fraud
