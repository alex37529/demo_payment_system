"""mock-fraud — fraud-check stub + failure injection (tz_v2 §2.3, phase 3).

Endpoints (port 8007):
    POST /fraud/check          {"decision": "approve"|"decline", "score", ...}
    POST /fraud/admin/fail     {"mode": "off"|"errors"|"timeout"}
    GET  /fraud/admin/mode     current mode (for tests / status)

The three modes drive the Orchestrator's circuit breaker (ADR-004):
  * off     — 200 + a decision (amount >= 1000 → decline, else approve);
  * errors  — 500 on every request  => each counts as a CB failure;
  * timeout — sleeps ~5s  => exceeds the CB's 2.5s timeout => CB failure.

The decision is deliberately simple (no ML); the point is the *failure
injection* for the acceptance test `POST /fraud/admin/fail {mode: errors}` →
CB open → fraud checks land in the DLQ while valid traffic continues.
"""
from __future__ import annotations

import asyncio
import logging

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

log = logging.getLogger(__name__)

#: timeout-injection delay — MUST exceed the orchestrator CB timeout (2.5s, ADR-004)
TIMEOUT_DELAY_S = 5.0
#: amounts >= this are declined in "off" mode (a realistic-ish risk rule)
DECLINE_THRESHOLD = 1000

_MODES = ("off", "errors", "timeout")


class FraudCheckIn(BaseModel):
    payment_id: str = ""
    account_id: str = ""
    amount: dict | int | None = None


class AdminFailIn(BaseModel):
    mode: str


class _InjectedServerError(Exception):
    """Internal sentinel for the `errors` injection -> mapped to a 500."""


class FraudService:
    def __init__(self) -> None:
        self.mode: str = "off"
        self.hits: int = 0  # observability: how many checks received

    def set_mode(self, mode: str) -> str:
        if mode not in _MODES:
            raise ValueError(f"invalid mode {mode!r}; expected one of {_MODES}")
        self.mode = mode
        return self.mode

    async def check(self, payment_id: str = "", amount: dict | int | None = None) -> JSONResponse:
        self.hits += 1
        if self.mode == "errors":
            log.info("fraud check %s: injected 500", payment_id)
            raise _InjectedServerError()
        if self.mode == "timeout":
            await asyncio.sleep(TIMEOUT_DELAY_S)
            log.info("fraud check %s: timeout-mode response (after %.1fs)",
                     payment_id, TIMEOUT_DELAY_S)

        value = self._amount_to_value(amount)
        decision = "decline" if value >= DECLINE_THRESHOLD else "approve"
        score = 0.9 if decision == "decline" else 0.3
        reason = "high_amount" if decision == "decline" else ""
        log.info("fraud check %s amount=%s -> %s (score=%.2f)", payment_id, value, decision, score)
        return JSONResponse({"decision": decision, "score": score,
                             "reason": reason, "payment_id": payment_id})

    @staticmethod
    def _amount_to_value(amount: dict | int | None) -> int:
        if isinstance(amount, int):
            return amount
        if isinstance(amount, dict):
            return int(amount.get("value", amount.get("amount", 0)) or 0)
        return 0


def attach_fraud(app: FastAPI) -> None:
    svc = FraudService()
    app.state.fraud = svc

    @app.exception_handler(_InjectedServerError)
    async def _server_err(_: object, exc: _InjectedServerError) -> JSONResponse:
        return JSONResponse(status_code=500, content={"error": "injected fraud failure"})

    @app.post("/fraud/check")
    async def check(body: FraudCheckIn) -> JSONResponse:
        return await svc.check(body.payment_id, body.amount)

    @app.post("/fraud/admin/fail")
    async def admin_fail(body: AdminFailIn) -> JSONResponse:
        try:
            mode = svc.set_mode(body.mode.lower())
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e)) from e
        return JSONResponse({"ok": True, "mode": mode})

    @app.get("/fraud/admin/mode")
    async def get_mode() -> JSONResponse:
        return JSONResponse({"mode": svc.mode, "hits": svc.hits})
