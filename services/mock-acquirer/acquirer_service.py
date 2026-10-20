"""mock-acquirer — payment generator + 50 TPS load burst (phase 6, tz_v2 §2.2).

External stimulus generator (NOT business logic). It POSTs create-payment
requests to Intake over HTTP. Two surfaces:

* ``POST /acquirer/generate`` — one payment (valid, or ``include_invalids`` →
  a ~10% share of invalid ones: amount ``0``/negative, which Intake 422-rejects
  so they never reach Kafka — phase-6 spec pitfall #3).
* ``POST /acquirer/burst`` — a fire-and-forget load worker that issues one
  payment every ``1/tps`` seconds for ``duration`` seconds (spec §2.2). The
  *actual* throughput is what the platform sustains; if latency > 1/tps the real
  TPS dips below the target (spec pitfall #2). The worker does not block the
  HTTP response.

In-flight tasks are tracked in a ``set`` and dropped on completion (spec
pitfall #4: fire-and-forget tasks need a reference so the GC does not reap
them).

Currency is derived from the seeded account (``init/01_databases.sql``): odd
suffix → USD, even → EUR, so every generated payment satisfies the
"own-currency only" invariant (v2 #5) and the happy path (fraud approves the
low amounts) works end-to-end.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time
import uuid

import httpx
from fastapi import FastAPI
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)

#: valid-amount ceiling, kept BELOW the phase-3 fraud decline threshold (1000
#: cents = $10) so generated payments take the *happy* path (fraud approve →
#: debit → balance move). See the phase-6 done report for the deviation from
#: the spec's illustrative "$10..$1000" (which conflicts with the merged
#: phase-3 fraud rule).
VALID_MAX_CENTS = 950
VALID_MIN_CENTS = 50


class GenerateRequest(BaseModel):
    include_invalids: bool = False
    invalid_ratio: float = Field(default=0.10, ge=0.0, le=1.0)


class BurstRequest(BaseModel):
    tps: int = Field(default=50, ge=1, le=1000)
    duration: int = Field(default=60, ge=1, le=600)
    include_invalids: bool = True
    invalid_ratio: float = Field(default=0.10, ge=0.0, le=1.0)


_ACCOUNT_DIGITS = re.compile(r"(\d+)$")


class AcquirerService:
    def __init__(self, base_url: str = "http://intake:8080", timeout: float = 5.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.accounts = [f"U{i:03d}" for i in range(1, 21)]  # seeded U001..U020
        self.client = httpx.AsyncClient(timeout=timeout)
        self._in_flight: set[asyncio.Task] = set()
        self._burst_task: asyncio.Task | None = None
        self.burst_tps = 0
        self.burst_duration = 0
        self.counters = {
            "started_at": None,   # monotonic epoch of the current/last burst
            "requested": 0,      # every payment request issued
            "accepted": 0,        # Intake 2xx (202 fresh / 200 replay)
            "rejected": 0,        # Intake 4xx (invalid payload → 422)
            "errors": 0,          # 5xx / network failure
        }

    # -- helpers ------------------------------------------------------------- #
    @staticmethod
    def _currency_for(account_id: str) -> str:
        m = _ACCOUNT_DIGITS.search(account_id)
        if m and int(m.group(1)) % 2 == 0:
            return "EUR"
        return "USD"

    def spawn(self, coro) -> asyncio.Task:
        t = asyncio.create_task(coro)
        self._in_flight.add(t)
        t.add_done_callback(self._in_flight.discard)
        return t

    # -- generation ---------------------------------------------------------- #
    async def _generate_one(self, *, include_invalids: bool = False,
                            invalid_ratio: float = 0.10, r=None) -> dict:
        import random as _r
        rnd = r or _r
        account_id = rnd.choice(self.accounts)
        currency = self._currency_for(account_id)
        invalid = include_invalids and rnd.random() < invalid_ratio
        if invalid:
            amount = 0 if rnd.random() < 0.5 else -1000  # Intake must 422 these
        else:
            amount = rnd.randint(VALID_MIN_CENTS, VALID_MAX_CENTS)
        body = {"amount": amount, "currency": currency, "account_id": account_id}
        idem = f"acq-{uuid.uuid4()}"

        status: int | None = None
        payment_id = ""
        try:
            resp = await self.client.post(f"{self.base_url}/v1/payments", json=body,
                                          headers={"Idempotency-Key": idem})
            status = resp.status_code
            if status in (200, 202):
                self.counters["accepted"] += 1
                try:
                    payment_id = resp.json().get("payment_id") or resp.json().get("id") or ""
                except ValueError:
                    pass
            elif status < 500:
                self.counters["rejected"] += 1
            else:
                self.counters["errors"] += 1
        except httpx.HTTPError as e:
            self.counters["errors"] += 1
            log.warning("acquirer: intake unreachable (%s)", e.__class__.__name__)
        self.counters["requested"] += 1
        log.debug("acquirer: %s %s amt=%s -> %s %s", account_id, currency, amount,
                  status, payment_id)
        return {"status": status, "payment_id": payment_id, "body": body,
                "accepted": status in (200, 202)}

    # -- burst --------------------------------------------------------------- #
    async def start_burst(self, *, tps: int, duration: int,
                          include_invalids: bool, invalid_ratio: float) -> dict:
        if self._burst_task is not None and not self._burst_task.done():
            return {"status": "already_running"}
        self.burst_tps = tps
        self.burst_duration = duration
        self.counters["started_at"] = time.monotonic()
        self.counters.update({"requested": 0, "accepted": 0, "rejected": 0, "errors": 0})
        self._burst_task = asyncio.create_task(
            self._burst_worker(tps, duration, include_invalids, invalid_ratio),
            name="acquirer-burst")
        log.info("acquirer burst START tps=%d duration=%ds", tps, duration)
        return {"status": "running", "tps": tps, "duration": duration}

    async def _burst_worker(self, tps: int, duration: int,
                            include_invalids: bool, invalid_ratio: float) -> None:
        interval = 1.0 / max(1, tps)
        end = time.monotonic() + duration
        try:
            while time.monotonic() < end:
                self.spawn(self._generate_one(include_invalids=include_invalids,
                                              invalid_ratio=invalid_ratio))
                await asyncio.sleep(interval)
            log.info("acquirer burst DONE tps=%d requested=%d", tps,
                     self.counters["requested"])
        except asyncio.CancelledError:
            log.info("acquirer burst CANCELLED after %d requests", self.counters["requested"])
            raise

    def stop_burst(self) -> None:
        if self._burst_task is not None and not self._burst_task.done():
            self._burst_task.cancel()

    def status(self) -> dict:
        c = self.counters
        out: dict = {
            "burst_running": self._burst_task is not None and not self._burst_task.done(),
            "tps_requested": self.burst_tps,
            "duration_s": self.burst_duration,
            "in_flight": len(self._in_flight),
            "requested": c["requested"],
            "accepted": c["accepted"],
            "rejected": c["rejected"],
            "errors": c["errors"],
        }
        if c.get("started_at") is not None:
            out["elapsed_s"] = round(time.monotonic() - c["started_at"], 2)
        return out

    async def aclose(self) -> None:
        self.stop_burst()
        await self.client.aclose()


def attach_acquirer(app: FastAPI) -> None:
    from contextlib import asynccontextmanager

    svc = AcquirerService(base_url=os.environ.get("INTAKE_URL", "http://intake:8080"))
    app.state.acquirer = svc

    @app.post("/acquirer/generate")
    async def generate(req: GenerateRequest) -> dict:
        return await svc._generate_one(include_invalids=req.include_invalids,
                                       invalid_ratio=req.invalid_ratio)

    @app.post("/acquirer/burst")
    async def burst(req: BurstRequest) -> dict:
        return await svc.start_burst(tps=req.tps, duration=req.duration,
                                     include_invalids=req.include_invalids,
                                     invalid_ratio=req.invalid_ratio)

    @app.post("/acquirer/burst/stop")
    async def burst_stop() -> dict:
        svc.stop_burst()
        return {"ok": True}

    @app.get("/acquirer/burst/status")
    async def burst_status() -> dict:
        return svc.status()

    @app.get("/acquirer/status")
    async def status() -> dict:
        return svc.status()

    @asynccontextmanager
    async def _acq_lifespan(_app: FastAPI):
        log.info("mock-acquirer ready (intake=%s)", svc.base_url)
        try:
            yield
        finally:
            await svc.aclose()
            log.info("mock-acquirer shutdown complete")

    app.router.lifespan_context = _acq_lifespan
