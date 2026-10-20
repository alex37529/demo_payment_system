"""mock-acquirer tests — generator + burst (phase 6).

The service lives under `services/mock-acquirer/` (folder has a hyphen — not a
valid Python module name), so we import its `acquirer_service` directly by path
(same pattern as `tests/mock/test_fraud_service.py`).

A fake intake is an ASGI app on the *same* httpx transport: we point
`AcquirerService.client` at an in-process intake stub so the generator's
`client.post()` is exercised end-to-end without a live Intake service.
"""
from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path

import pytest_asyncio
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from httpx import ASGITransport, AsyncClient

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ACQ_SERVICE = _REPO_ROOT / "services" / "mock-acquirer" / "acquirer_service.py"


def _load_acq():
    spec = importlib.util.spec_from_file_location("mock_acquirer", _ACQ_SERVICE)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


acq = _load_acq()


def _intake_stub_app() -> FastAPI:
    """Tiny intake: POST /v1/payments -> 202 if amount>0 else 422."""
    app = FastAPI()
    seen: list[dict] = []

    @app.post("/v1/payments")
    async def create(body: dict) -> JSONResponse:
        amount = int(body.get("amount", 0))
        account = body.get("account_id", "")
        seen.append({"amount": amount, "account_id": account})
        if amount <= 0:
            return JSONResponse(status_code=422, content={"ok": False, "error": "invalid amount"})
        return JSONResponse(status_code=202, content={"ok": True,
                                                       "payment_id": f"P-{account}",
                                                       "account_id": account})

    app.state.seen = seen
    return app


@pytest_asyncio.fixture
async def acq_service():
    intake_app = _intake_stub_app()
    client = AsyncClient(transport=ASGITransport(app=intake_app), base_url="http://intake")
    svc = acq.AcquirerService(base_url="http://intake:8080")
    svc.client = client  # inject the stub intake
    yield svc, intake_app
    await svc.aclose()
    await client.aclose()


async def test_currency_follows_seeded_parity(acq_service):
    svc, _ = acq_service
    # odd index -> USD, even -> EUR (matches init/01_databases.sql)
    assert svc._currency_for("U001") == "USD"
    assert svc._currency_for("U002") == "EUR"
    assert svc._currency_for("U020") == "EUR"


async def test_generate_valid_is_accepted(acq_service):
    svc, intake_app = acq_service
    r = await svc._generate_one(include_invalids=False)
    assert r["status"] == 202
    assert r["accepted"] is True
    assert r["body"]["amount"] > 0
    assert intake_app.state.seen[-1]["account_id"] == r["body"]["account_id"]


async def test_generate_includes_invalids_sometimes_rejected(acq_service):
    svc, _ = acq_service
    statuses: list[int | None] = []
    for _ in range(40):
        r = await svc._generate_one(include_invalids=True, invalid_ratio=1.0)
        statuses.append(r["status"])
    # invalid_ratio=1.0 -> every one is invalid (amount 0 or -1000) -> every 422
    assert all(s == 422 for s in statuses)


async def test_accepted_counter_increments(acq_service):
    svc, _ = acq_service
    before = svc.counters["accepted"]
    for _ in range(3):
        await svc._generate_one(include_invalids=False)
    assert svc.counters["accepted"] == before + 3
    assert svc.counters["requested"] >= 3


async def test_burst_runs_then_completes(acq_service):
    svc, _ = acq_service
    # a very short burst at a low tps
    r = await svc.start_burst(tps=20, duration=1, include_invalids=False, invalid_ratio=0.0)
    assert r == {"status": "running", "tps": 20, "duration": 1}
    # wait for the task to finish (it should be done within ~1.5s)
    await asyncio.sleep(1.5)
    assert svc._burst_task is not None and svc._burst_task.done()
    assert svc.counters["requested"] > 0
    # starting a new burst now returns `running` (previous is done)
    r2 = await svc.start_burst(tps=10, duration=1, include_invalids=False, invalid_ratio=0.0)
    assert r2["status"] == "running"


async def test_burst_stop_cancels(acq_service):
    svc, _ = acq_service
    await svc.start_burst(tps=50, duration=30, include_invalids=False, invalid_ratio=0.0)
    assert svc._burst_task is not None and not svc._burst_task.done()
    svc.stop_burst()
    await asyncio.sleep(0.05)
    assert svc._burst_task is not None and svc._burst_task.done()


async def test_status_shape(acq_service):
    svc, _ = acq_service
    s = svc.status()
    for k in ("burst_running", "tps_requested", "duration_s", "in_flight",
              "requested", "accepted", "rejected", "errors"):
        assert k in s
    assert s["burst_running"] is False
    assert s["requested"] >= 0
