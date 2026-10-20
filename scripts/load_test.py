#!/usr/bin/env python
"""Load harness: N TPS against the Intake for D seconds, with percentiles.

The phase-7 spec §8 harness, with the numbers the SLO is measured in:

    throughput  — payments/second ACTUALLY achieved (not the requested one)
    p50/p95/p99 — client-side latency percentiles
    max, errors — the tail and the 5xx count

Why the achieved throughput and not the requested one (spec pitfall #14):
50 TPS means a request every 20 ms. If one request takes longer than that,
the achieved rate DROPS below 50 — that is a fact about the system, not a
bug in the harness, so the harness reports what actually happened.

Usage:
    python scripts/load_test.py                       # 50 TPS / 60 s -> Intake
    python scripts/load_test.py --tps 100 --duration 30
    python scripts/load_test.py --url http://localhost:8000/v1/payments
    python scripts/load_test.py --slo 300              # fail if p99 > 300 ms

Exit code is 1 when the SLO is violated (p99 > --slo) or a 5xx was seen —
so it can be used as a gate in CI / in `make acceptance`.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import random
import statistics
import sys
import time
import uuid

import httpx

#: The seeded accounts (init/01_databases.sql) — 20 users, USD/EUR.
ACCOUNTS = [f"U{i:03d}" for i in range(1, 21)]
CURRENCIES = ["USD", "EUR"]

#: Amount range in MINOR units (cents). It is deliberately kept BELOW the
#: mock-fraud decline threshold (1000 cents = $10): an amount of $10 or more
#: is declined by the fraud rule, so the payment would die at the fraud step
#: and NEVER reach the debit — the harness would then measure the intake's
#: HTTP latency while nothing was actually settled. The mock-acquirer uses the
#: same range for the same reason.
MIN_AMOUNT_CENTS = 50
MAX_AMOUNT_CENTS = 950


def account_currency(account: str) -> str:
    """The currency of a seeded account.

    The seed alternates: U001 USD, U002 EUR, U003 USD, … (odd = USD,
    even = EUR). Sending the WRONG currency is refused by the ledger at the
    reserve step (own-currency invariant, ADR-003) — the payment then dies
    before the debit, and the harness would "pass" while nothing was
    actually settled. Same rule as the mock-acquirer.
    """
    digits = "".join(ch for ch in account if ch.isdigit())
    if digits and int(digits) % 2 == 0:
        return "EUR"
    return "USD"


async def one(client: httpx.AsyncClient, url: str, account: str,
              currency: str, amount: int) -> tuple[float, int]:
    """POST one payment. Returns (latency_ms, status_code)."""
    payload = {"amount": amount, "currency": currency, "account_id": account}
    headers = {"Idempotency-Key": f"load-{uuid.uuid4()}"}
    start = time.perf_counter()
    try:
        r = await client.post(url, json=payload, headers=headers)
        status = r.status_code
    except Exception:  # noqa: BLE001 — a connection error is a failed request
        status = 0
    return (time.perf_counter() - start) * 1000.0, status


async def run(tps: int, duration: int, url: str, timeout: float) -> dict:
    """Run the burst, collecting the results as they arrive."""
    interval = 1.0 / tps
    results: list[tuple[float, int]] = []
    timeout_cfg = httpx.Timeout(timeout, connect=timeout)

    async with httpx.AsyncClient(timeout=timeout_cfg) as client:
        inflight: set[asyncio.Task] = set()

        def _collect(task: asyncio.Task) -> None:
            """Record the result as soon as the request finishes.

            The task must be removed from `inflight` AND its result kept —
            dropping it from the set before the final gather would lose it.
            """
            inflight.discard(task)
            if not task.cancelled() and task.exception() is None:
                results.append(task.result())

        # Pace on an ABSOLUTE schedule (slot N starts at start + N*interval),
        # not on `sleep(interval)` in a loop: the loop body itself costs a few
        # ms (task creation + wake-up), and that overhead would accumulate
        # into the ACHIEVED rate — the harness would then measure itself
        # instead of the system (spec pitfall #14).
        start = time.monotonic()
        issued = 0
        while True:
            now = time.monotonic()
            if now - start >= duration:
                break
            slot = issued * interval
            if slot > now - start:
                await asyncio.sleep(min(slot - (now - start), interval))
            account = random.choice(ACCOUNTS)
            currency = account_currency(account)
            amount = random.randint(MIN_AMOUNT_CENTS, MAX_AMOUNT_CENTS)
            task = asyncio.create_task(
                one(client, url, account, currency, amount)
            )
            inflight.add(task)
            task.add_done_callback(_collect)
            issued += 1

        if inflight:
            await asyncio.gather(*inflight, return_exceptions=True)

    latencies = sorted(r[0] for r in results)
    statuses = [r[1] for r in results]
    server_errors = sum(1 for s in statuses if s >= 500)
    conn_errors = sum(1 for s in statuses if s == 0)
    accepted = sum(1 for s in statuses if 200 <= s < 300)

    n = len(latencies)

    def pct(p: float) -> float:
        if not latencies:
            return 0.0
        idx = min(n - 1, max(0, round(p * (n - 1))))
        return latencies[idx]

    return {
        "requested_tps": tps,
        "duration_s": duration,
        "total": n,
        "throughput": round(n / duration, 2) if duration else 0.0,
        "accepted": accepted,
        "server_errors": server_errors,
        "connection_errors": conn_errors,
        "p50": round(pct(0.50), 1),
        "p95": round(pct(0.95), 1),
        "p99": round(pct(0.99), 1),
        "max": round(latencies[-1], 1) if latencies else 0.0,
        "mean": round(statistics.fmean(latencies), 1) if latencies else 0.0,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Payments load harness (TPS + percentiles)")
    ap.add_argument("--url", default="http://localhost:8001/v1/payments",
                    help="the create-payment endpoint")
    ap.add_argument("--tps", type=int, default=50, help="requests per second")
    ap.add_argument("--duration", type=int, default=60, help="seconds")
    ap.add_argument("--timeout", type=float, default=5.0, help="per-request timeout (s)")
    ap.add_argument("--slo", type=float, default=300.0,
                    help="p99 SLO in ms; the run fails if exceeded (0 = off)")
    args = ap.parse_args()

    result = asyncio.run(run(args.tps, args.duration, args.url, args.timeout))
    print(json.dumps(result, indent=2))

    slo_ok = args.slo <= 0 or result["p99"] <= args.slo
    errors_ok = result["server_errors"] == 0 and result["connection_errors"] == 0
    if not (slo_ok and errors_ok):
        print(f"FAIL: p99={result['p99']}ms (slo {args.slo}ms), "
              f"5xx={result['server_errors']}, conn_errors={result['connection_errors']}",
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
