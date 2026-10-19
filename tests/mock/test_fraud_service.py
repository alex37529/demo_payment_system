"""mock-fraud service tests — the phase-3 failure-injection target.

The service lives under `services/mock-fraud/` (folder has a hyphen — not a
valid Python module name), so we import its `fraud_service` directly by path.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from services.shared_app import build_app


def _attach_fraud(app):
    spec = importlib.util.spec_from_file_location(
        "mock_fraud_service",
        Path(__file__).resolve().parents[2] / "services" / "mock-fraud" / "fraud_service.py",
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    mod.attach_fraud(app)
    return mod


@pytest_asyncio.fixture
async def client():
    app = build_app("mock-fraud-test")
    mod = _attach_fraud(app)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield app, c, mod


async def test_default_mode_is_off_and_low_amount_approved(client):
    app, c, mod = client
    r = await c.get("/fraud/admin/mode")
    assert r.json()["mode"] == "off"
    r = await c.post("/fraud/check", json={"payment_id": "P1",
                                            "amount": {"value": 10, "currency": "USD"}})
    assert r.status_code == 200
    body = r.json()
    assert body["decision"] == "approve"
    assert 0.0 <= body["score"] < 1.0


async def test_high_amount_is_declined(client):
    app, c, mod = client
    r = await c.post("/fraud/check", json={"payment_id": "P2",
                                            "amount": {"value": 1500, "currency": "USD"}})
    assert r.status_code == 200
    assert r.json()["decision"] == "decline"
    assert r.json()["reason"] == "high_amount"


async def test_mode_switch_errors_returns_500(client):
    app, c, mod = client
    r = await c.post("/fraud/admin/fail", json={"mode": "errors"})
    assert r.json() == {"ok": True, "mode": "errors"}
    assert (await c.get("/fraud/admin/mode")).json()["mode"] == "errors"
    # every check is now a 500 — the CB will count this (ADR-004)
    for _ in range(3):
        rr = await c.post("/fraud/check", json={"payment_id": "P3", "amount": 10})
        assert rr.status_code == 500
        assert rr.json() == {"error": "injected fraud failure"}


async def test_mode_switch_timeout_sets_mode(client):
    app, c, mod = client
    r = await c.post("/fraud/admin/fail", json={"mode": "timeout"})
    assert r.json()["mode"] == "timeout"
    assert (await c.get("/fraud/admin/mode")).json()["mode"] == "timeout"


async def test_invalid_mode_is_rejected(client):
    app, c, mod = client
    r = await c.post("/fraud/admin/fail", json={"mode": "nope"})
    assert r.status_code == 422
