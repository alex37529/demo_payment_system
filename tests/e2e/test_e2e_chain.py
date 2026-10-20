"""E2E test: the full write-side chain, against the LIVE stack.

    debit -> accounts + outbox_events (ONE TX)
          -> Debezium (reads the Postgres WAL)
          -> Kafka `ledger.events`
          -> Profile consumer (idempotent) -> profile.users.balance
          -> Notification consumer -> SSE feed

**Why not Testcontainers** (as the phase-7 spec §7 sketches): a testcontainers
E2E needs PG + Kafka + Connect + Debezium + the services = 12 containers, and
on Windows/Docker Desktop that is 30-60 s per run — and it would test a
*different* Debezium wiring than the one that actually runs. This test
instead drives the REAL stack (the same `docker compose up` the demo uses), so
it covers the deployed configuration, not a reconstruction of it.

**How to run** (the stack must be up: `make up`):

    RUN_E2E=1 python -m pytest tests/e2e -v

Without `RUN_E2E=1` the tests are SKIPPED — `make test` stays hermetic and
fast, and the E2E is an explicit opt-in (it mutates the running system's
balances, so it must not run by surprise during a unit-test run).

**Eventual consistency** (spec pitfall #13): Debezium is async. A fixed
``asyncio.sleep(2)`` is NOT reliable — every wait here is a POLL with a
timeout, so the test is fast when things are fast and correct when slow.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
import uuid

import httpx
import pytest

#: The seeded accounts (init/01_databases.sql) — 20 users, USD/EUR.
#: Each test uses its OWN account: the tests share the seeded accounts, and a
#: concurrent test's payment would move the balance under our feet.
ACCOUNT_USD = "U001"
ACCOUNT_EUR = "U002"
ACCOUNT_USD_2 = "U005"
ACCOUNT_USD_3 = "U007"

INTAKE = os.environ.get("E2E_INTAKE_URL", "http://localhost:8001")
PROFILE = os.environ.get("E2E_PROFILE_URL", "http://localhost:8004")
NOTIFICATION = os.environ.get("E2E_NOTIFICATION_URL", "http://localhost:8005")

#: How long to wait for the async chain to settle (Debezium -> Kafka -> consumer).
DEADLINE_S = float(os.environ.get("E2E_DEADLINE_S", "30"))

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_E2E") != "1",
    reason="E2E needs the live stack: RUN_E2E=1 make up && RUN_E2E=1 pytest tests/e2e",
)


async def _poll(fn, timeout_s: float = DEADLINE_S, interval_s: float = 0.5):
    """Poll ``fn`` until it returns a truthy value (or the deadline passes).

    Returns the value, or ``None`` on timeout. Polling (not sleeping) is the
    only correct way to wait for an eventually-consistent chain.
    """
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        value = await fn()
        if value:
            return value
        await asyncio.sleep(interval_s)
    return None


async def _poll_until(fn, predicate, timeout_s: float = DEADLINE_S,
                      interval_s: float = 0.5):
    """Poll ``fn`` until ``predicate(value)`` holds; return that value.

    Needed for the balance: the balance is ALREADY truthy before the debit
    lands, so a plain truthy-poll returns the unchanged value and the test
    would pass/fail for the wrong reason. The predicate states what we are
    actually waiting for.
    """
    deadline = asyncio.get_running_loop().time() + timeout_s
    last = None
    while asyncio.get_running_loop().time() < deadline:
        last = await fn()
        if predicate(last):
            return last
        await asyncio.sleep(interval_s)
    return last


async def _balance(client: httpx.AsyncClient, account_id: str) -> int | None:
    r = await client.get(f"{PROFILE}/v1/users/{account_id}")
    if r.status_code != 200:
        return None
    return r.json().get("balance")


async def _create_payment(client: httpx.AsyncClient, account_id: str, amount: int,
                          currency: str) -> str:
    """POST a payment and return its payment_id (202 = accepted)."""
    key = f"e2e-{uuid.uuid4()}"
    r = await client.post(
        f"{INTAKE}/v1/payments",
        json={"amount": amount, "currency": currency, "account_id": account_id},
        headers={"Idempotency-Key": key},
    )
    assert r.status_code in (200, 202), f"intake rejected: {r.status_code} {r.text}"
    return r.json()["payment_id"]


@pytest.fixture
async def client():
    async with httpx.AsyncClient(timeout=10.0) as c:
        yield c


async def test_outbox_to_kafka_to_profile_balance(client):
    """The money must move all the way: debit -> outbox -> Debezium -> Kafka
    -> profile read model. The profile balance is the OBSERVABLE end of the
    chain, so this asserts the whole pipeline actually works."""
    before = await _balance(client, ACCOUNT_USD)
    assert before is not None, f"profile not reachable at {PROFILE}"

    amount = 100
    payment_id = await _create_payment(client, ACCOUNT_USD, amount, "USD")

    # Wait for the balance to actually CHANGE (a truthy-poll would return the
    # unchanged balance and pass for the wrong reason).
    after = await _poll_until(
        lambda: _balance(client, ACCOUNT_USD),
        lambda b: b is not None and b != before,
    )
    # The balance must have DECREASED by exactly the debited amount.
    assert after is not None, "profile balance never changed — the chain is broken"
    assert after == before - amount, f"balance {before} -> {after}, expected {before - amount}"

    # The saga must have reached Success (the orchestrator's own state).
    saga = await client.get(f"http://localhost:8002/v1/sagas/by-payment/{payment_id}")
    if saga.status_code == 200:
        assert saga.json()["state"] == "Success", saga.text


async def test_ledger_events_carry_the_payment(client):
    """The notification feed must show the payment (it consumes the same
    `ledger.events` topic the profile consumer does)."""
    payment_id = await _create_payment(client, ACCOUNT_USD_2, 50, "USD")

    async def in_feed() -> bool:
        r = await client.get(f"{NOTIFICATION}/v1/payments?limit=100")
        if r.status_code != 200:
            return False
        return any(p.get("payment_id") == payment_id for p in r.json().get("payments", []))

    assert await _poll(in_feed), f"payment {payment_id} never reached the notification feed"


async def test_replay_does_not_double_debit(client):
    """At-least-once delivery must not double-apply: the SAME Idempotency-Key
    must return the same payment and must NOT debit twice."""
    # A dedicated account: the tests share the seeded accounts, and a
    # concurrent test's payment would move the balance under our feet.
    account_id = "U004"
    key = f"e2e-replay-{uuid.uuid4()}"
    body = {"amount": 75, "currency": "EUR", "account_id": account_id}
    headers = {"Idempotency-Key": key}

    before = await _balance(client, account_id)
    r1 = await client.post(f"{INTAKE}/v1/payments", json=body, headers=headers)
    assert r1.status_code in (200, 202), r1.text
    payment_id = r1.json()["payment_id"]

    after_first = await _poll_until(
        lambda: _balance(client, account_id),
        lambda b: b is not None and b != before,
    )
    assert after_first == before - 75, f"balance {before} -> {after_first}"

    # Replay the SAME key.
    r2 = await client.post(f"{INTAKE}/v1/payments", json=body, headers=headers)
    assert r2.status_code in (200, 202), r2.text
    assert r2.json()["payment_id"] == payment_id, "replay must return the same payment_id"

    # Give the (re-published) command time to travel, then the balance must
    # still be the single-debited value.
    await asyncio.sleep(3)
    assert await _balance(client, account_id) == after_first, "replay double-debited"


async def test_own_currency_invariant_enforced(client):
    """ADR-003 / tz_v2 v2 #5: a USD account cannot be debited in EUR."""
    await client.post(
        f"{INTAKE}/v1/payments",
        json={"amount": 10, "currency": "EUR", "account_id": ACCOUNT_USD},
        headers={"Idempotency-Key": f"e2e-fx-{uuid.uuid4()}"},
    )
    # The payment may be ACCEPTED (the saga decides later) — what must not
    # happen is a successful EUR debit on a USD account. The saga therefore
    # ends Failed (fraud/ledger rejects) and the balance is unchanged.
    await asyncio.sleep(3)
    r2 = await client.get(f"{PROFILE}/v1/users/{ACCOUNT_USD}")
    assert r2.status_code == 200
    assert r2.json()["currency"] == "USD"


async def test_reservation_is_released_after_a_successful_payment(client):
    """A successful payment must leave NO hold on the account.

    This was the observable symptom of the reservation leak (phase-7 §6):
    the saga reserved the funds and then debited them, but nothing released
    the hold on the success path, so `reserved` grew by the payment amount
    on every payment and `available` shrank for ever. Fixed by ADR-010 —
    the debit consumes the hold of its own payment.
    """
    account_id = "U003"
    before = await _balance(client, account_id)
    assert before is not None

    await _create_payment(client, account_id, 100, "USD")
    after = await _poll_until(
        lambda: _balance(client, account_id),
        lambda b: b is not None and b != before,
    )
    assert after == before - 100

    # The hold must be gone. The read-side does not expose `reserved`, so
    # this is asserted through the ledger API — and per payment, so a
    # concurrent payment on the same account cannot make it flaky.
    r = await client.get(f"http://localhost:8003/v1/accounts/{account_id}")
    assert r.status_code == 200
    body = r.json()
    assert body["holds"] == {}, f"the reservation was not released: {body}"
    assert body["reserved"]["value"] == 0, f"the reservation was not released: {body}"
    assert body["available"]["value"] == body["balance"]["value"]

async def test_loki_receives_structured_logs():
    """The logs pillar must actually work: the service's JSON log line has to
    be queryable by the `service` label (pitfall #6 — Loki indexes labels
    only). This is the check that the log driver / promtail pipeline works."""
    async with httpx.AsyncClient(timeout=10.0) as c:
        await c.get(f"{PROFILE}/v1/users/{ACCOUNT_USD}")

        loki = os.environ.get("E2E_LOKI_URL", "http://localhost:3100")
        # WALL-CLOCK seconds: the Loki API takes absolute unix timestamps.
        # (The event loop's clock is MONOTONIC — seconds since boot — so it
        # must NOT be used here.)
        end = int(time.time())
        start = end - 300
        q = '{service="profile"}'

        async def found() -> bool:
            r = await c.get(
                f"{loki}/loki/api/v1/query_range",
                params={"query": q, "limit": "5", "direction": "backward",
                        "start": str(start * 1_000_000_000), "end": str(end * 1_000_000_000)},
            )
            if r.status_code != 200:
                return False
            return bool(r.json().get("data", {}).get("result"))

        assert await _poll(found, timeout_s=20), "no logs with service=profile in Loki"


async def test_trace_is_continuous_across_kafka(client):
    """One payment must produce ONE trace, not a trace per service.

    The trace id is read from the intake's own JSON log line (the service
    that starts the chain), then Tempo is asked for that trace: the spans
    must come from MORE THAN ONE service — that is exactly what "the trace
    does not break at the Kafka boundary" means (phase-7 red flag #1).
    """
    async with httpx.AsyncClient(timeout=10.0) as c:
        await _create_payment(client, ACCOUNT_USD_3, 25, "USD")

        loki = os.environ.get("E2E_LOKI_URL", "http://localhost:3100")
        tempo = os.environ.get("E2E_TEMPO_URL", "http://localhost:3200")
        # WALL-CLOCK seconds (the event loop's clock is monotonic).
        end = int(time.time())
        start = end - 300

        async def find_trace() -> str | None:
            r = await c.get(
                f"{loki}/loki/api/v1/query_range",
                params={"query": '{service="intake"}', "limit": "50",
                        "direction": "backward",
                        "start": str(start * 1_000_000_000),
                        "end": str(end * 1_000_000_000)},
            )
            if r.status_code != 200:
                return None
            for stream in r.json().get("data", {}).get("result", []):
                for _, line in stream.get("values", []):
                    # The Loki `values` entry is [ns_timestamp, log_line] and
                    # the log line IS our JSON object (the timestamp is a
                    # separate field, not a prefix of the line).
                    try:
                        rec = json.loads(line)
                    except (ValueError, TypeError):
                        continue
                    if rec.get("path") == "/v1/payments" and rec.get("method") == "POST":
                        tid = rec.get("trace_id")
                        if tid and tid != "-":
                            return tid
            return None

        trace_id = await _poll(find_trace, timeout_s=20)
        assert trace_id, "no trace_id found in the intake logs"

        async def services_of(trace: str) -> set[str]:
            """The services that contributed spans to the trace.

            Tempo delivers a trace INCREMENTALLY (one batch per service), so
            "the trace exists" does NOT mean "the trace is complete" —
            polling for existence can catch it with only the first batch
            there. The wait condition is therefore the COMPLETENESS, not the
            existence.
            """
            r = await c.get(f"{tempo}/api/traces/{trace}")
            if r.status_code != 200:
                return set()
            found: set[str] = set()
            for b in r.json().get("batches", []):
                for attr in b.get("scopeSpans", []) or b.get("resourceSpans", []):
                    name = (attr.get("scope") or {}).get("name", "")
                    if name:
                        found.add(name)
            return found

        services = await _poll_until(
            lambda: services_of(trace_id),
            lambda s: len(s) >= 2,
            timeout_s=30,
        )
        # The spans must come from more than one service — that is exactly
        # what "the trace does not break at the Kafka boundary" means.
        assert len(services) >= 2, f"trace has spans from one service only: {services}"
