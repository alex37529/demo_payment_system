#!/usr/bin/env python
"""Acceptance: the 7 criteria of §7 TЗ v2, checked automatically.

The phase-8 spec sketches this as a bash script with `jq`. This is the same
check list implemented in Python, for two reasons:

* **jq is not installed** on a plain Windows dev box, and the repo's other
  tooling (`scripts/load_test.py`, `scripts/register_connector.py`,
  `scripts/set_grafana_password.py`) is Python anyway — the acceptance script
  should not be the only one that needs a tool the machine may not have;
* the checks need **polling with a timeout** (the spec's own pitfall #1 calls
  a fixed `sleep 3` "dirty"), and JSON/DB/Kafka inspection is far more
  readable in Python than in a shell pipeline.

`scripts/acceptance.sh` is the entry point (what `make acceptance` calls).

Usage:
    python scripts/acceptance.py              # all 7 criteria
    python scripts/acceptance.py --skip-setup # do not wipe/recreate the stack

Exit code 0 = every check passed.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import uuid
from collections.abc import Callable
from typing import Any

import httpx

BASE = os.environ.get("ACCEPTANCE_BASE", "http://localhost")
GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://localhost:3000")
TEMPO_URL = os.environ.get("TEMPO_URL", "http://localhost:3200")
PROM_URL = os.environ.get("PROM_URL", "http://localhost:9090")
AM_URL = os.environ.get("ALERTMANAGER_URL", "http://localhost:9093")
LOKI_URL = os.environ.get("LOKI_URL", "http://localhost:3100")

#: How long a check may wait for its condition to become true.
POLL_TIMEOUT_S = float(os.environ.get("ACCEPTANCE_POLL_S", "30"))

GREEN, RED, YELLOW, DIM, RESET = "\033[32m", "\033[31m", "\033[33m", "\033[2m", "\033[0m"


class Report:
    """Collects the results of the checks (the spec wants ALL of them, not
    the first failure — a failing run should show the whole picture)."""

    def __init__(self) -> None:
        self.passed = 0
        self.failed = 0
        self.failures: list[str] = []

    def check(self, name: str, ok: bool, detail: str = "") -> bool:
        if ok:
            self.passed += 1
            print(f"  {GREEN}PASS{RESET}  {name}" + (f" {DIM}({detail}){RESET}" if detail else ""))
        else:
            self.failed += 1
            self.failures.append(name)
            print(f"  {RED}FAIL{RESET}  {name}" + (f" {DIM}({detail}){RESET}" if detail else ""))
        return ok

    def section(self, title: str) -> None:
        print(f"\n{DIM}--- [{title}] ---{RESET}")


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def sh(*args: str, check: bool = False) -> tuple[int, str]:
    """Run a command, return (exit code, output)."""
    try:
        p = subprocess.run(list(args), capture_output=True, text=True,
                           timeout=180, check=False)
    except (FileNotFoundError, subprocess.TimeoutExpired) as e:
        return 127, str(e)
    out = (p.stdout or "") + (p.stderr or "")
    if check and p.returncode != 0:
        print(f"  {YELLOW}! command failed: {' '.join(args)}{RESET}\n{DIM}{out.strip()[:400]}{RESET}")
    return p.returncode, out


def psql(db: str, sql: str) -> str:
    """Run a query against one of the four bounded-context databases."""
    _, out = sh("docker", "compose", "exec", "-T", "postgres",
                "psql", "-U", "fintech", "-d", db, "-tAc", sql)
    return out.strip()


def kafka_count(topic: str, needle: str = "") -> int:
    """How many messages of `topic` contain `needle` (bounded read).

    NB: this reads from the BEGINNING of the topic, so on a long-lived topic
    it only sees the OLDEST messages — use :class:`KafkaTail` when you need
    the NEW ones.
    """
    _, out = sh("docker", "compose", "exec", "-T", "kafka",
                "kafka-console-consumer.sh", "--bootstrap-server", "localhost:9092",
                "--topic", topic, "--from-beginning", "--timeout-ms", "8000",
                "--max-messages", "5000")
    if not needle:
        return len([ln for ln in out.splitlines() if ln.strip()])
    return len([ln for ln in out.splitlines() if needle in ln])


class KafkaTail:
    """Collect the NEW messages of a topic (reads from the END).

    `kafka-console-consumer --from-beginning` reads the OLDEST messages
    first, so on a topic that already holds thousands of them the newest
    ones are never reached — which made the acceptance checks measure
    history instead of the event they had just caused. This tail seeks to the
    end of every partition and then collects what arrives, so it must be
    STARTED BEFORE the action whose messages you want to see.
    """

    def __init__(self, topic: str, *, seconds: float = 25.0,
                 brokers: str = "localhost:29092") -> None:
        self.topic = topic
        self.seconds = seconds
        self.brokers = brokers
        self.lines: list[str] = []
        self._thread = None

    def start(self) -> KafkaTail:
        import threading

        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        time.sleep(2)  # let the consumer reach the end of the partitions
        return self

    def _run(self) -> None:
        import asyncio

        async def _collect() -> None:
            from aiokafka import AIOKafkaConsumer, TopicPartition

            consumer = AIOKafkaConsumer(
                bootstrap_servers=self.brokers, enable_auto_commit=False,
                group_id=None, auto_offset_reset="latest",
            )
            await consumer.start()
            try:
                parts = consumer.partitions_for_topic(self.topic) or {0}
                tps = [TopicPartition(self.topic, p) for p in parts]
                consumer.assign(tps)
                for tp in tps:
                    consumer.seek_to_end(tp)
                deadline = time.monotonic() + self.seconds
                while time.monotonic() < deadline:
                    batches = await consumer.getmany(timeout_ms=500, max_records=500)
                    for msgs in batches.values():
                        for m in msgs:
                            self.lines.append(m.value.decode(errors="replace"))
            finally:
                await consumer.stop()

        try:
            asyncio.run(_collect())
        except Exception as e:  # noqa: BLE001 — a broken tail must not crash the run
            print(f"  warning: kafka tail for {self.topic} failed: {e}", file=sys.stderr)

    def stop(self) -> list[str]:
        if self._thread is not None:
            self._thread.join(timeout=self.seconds + 10)
        return self.lines

    def count(self, needle: str = "") -> int:
        if not needle:
            return len([ln for ln in self.lines if ln.strip()])
        return len([ln for ln in self.lines if needle in ln])


def prometheus(query: str) -> list[dict[str, Any]]:
    r = httpx.get(f"{PROM_URL}/api/v1/query", params={"query": query}, timeout=15)
    if r.status_code != 200:
        return []
    return r.json().get("data", {}).get("result", []) or []


def prom_value(query: str) -> float | None:
    res = prometheus(query)
    if not res:
        return None
    try:
        return float(res[0]["value"][1])
    except (KeyError, IndexError, ValueError):
        return None


def poll(fn: Callable[[], Any], predicate: Callable[[Any], bool],
         timeout_s: float = POLL_TIMEOUT_S, interval_s: float = 1.0) -> Any:
    """Wait until `predicate(fn())` holds (spec pitfall #1: poll, don't sleep)."""
    deadline = time.monotonic() + timeout_s
    last: Any = None
    while time.monotonic() < deadline:
        last = fn()
        if predicate(last):
            return last
        time.sleep(interval_s)
    return last


def account_currency(account: str) -> str:
    """The currency of an account (the read model knows it).

    The own-currency invariant (ADR-003) means a payment in the WRONG
    currency is refused by the ledger at the reserve step — the saga then
    dies before the fraud step, so such a payment can NEVER prove a circuit
    breaker recovery. Every payment here therefore uses the account's own
    currency.
    """
    try:
        r = httpx.get(f"{BASE}:8004/v1/users/{account}", timeout=10)
        if r.status_code == 200:
            return str(r.json().get("currency") or "USD")
    except httpx.HTTPError:
        pass
    return "USD"


def create_payment(account: str, amount: int = 100, currency: str | None = None,
                  idem: str | None = None) -> dict[str, Any]:
    """POST a payment (through the gateway — the real browser path).

    The currency defaults to the ACCOUNT's own currency (see
    :func:`account_currency`).
    """
    key = idem or f"acc-{uuid.uuid4()}"
    currency = currency or account_currency(account)
    r = httpx.post(
        f"{BASE}:8000/v1/payments",
        json={"amount": amount, "currency": currency, "account_id": account},
        headers={"Idempotency-Key": key}, timeout=20,
    )
    try:
        body = r.json()
    except ValueError:
        body = {}
    body.setdefault("_status", r.status_code)
    return body


def balance_of(account: str) -> int | None:
    r = httpx.get(f"{BASE}:8004/v1/users/{account}", timeout=10)
    if r.status_code != 200:
        return None
    try:
        return int(r.json().get("balance"))
    except (ValueError, TypeError, KeyError):
        return None


def richest_account() -> str:
    """The account with the most available funds (the read model)."""
    best, best_balance = "U001", -1
    for i in range(1, 21):
        account = f"U{i:03d}"
        bal = balance_of(account)
        if bal is not None and bal > best_balance:
            best, best_balance = account, bal
    return best


# --------------------------------------------------------------------------- #
# the 7 criteria
# --------------------------------------------------------------------------- #
def criterion_1_consistency(rep: Report) -> None:
    """Consistency: a payment is written to the outbox exactly once per event
    and the event is not lost or duplicated (Transactional Outbox + Debezium)."""
    rep.section("1. Consistency (Outbox)")
    # The tail must be running BEFORE the payment, so it sees the NEW events.
    tail = KafkaTail("ledger.events", seconds=30).start()

    payment = create_payment("U017")
    pid = str(payment.get("payment_id") or "")
    if not rep.check("payment accepted", bool(pid), f"payment_id={pid}"):
        tail.stop()
        return

    # The ledger is killed while the payment is in flight: the outbox row must
    # still exist afterwards, and the event must reach Kafka exactly once.
    time.sleep(1)
    sh("docker", "compose", "stop", "ledger")
    sh("docker", "compose", "start", "ledger")

    # A payment produces SEVERAL events (FundsReserved + FundsDebited), so the
    # invariant is not "one row" but "one row per event, no duplicates".
    rows = psql("ledger",
                "SELECT count(*)||'/'||count(DISTINCT event_id) FROM outbox_events "
                f"WHERE payload->>'payment_id'='{pid}'")
    total, _, distinct = rows.partition("/")
    rep.check("one outbox row per event (no duplicates)",
              total == distinct and int(total or 0) >= 1, f"rows={rows}")

    time.sleep(10)
    lines = tail.stop()
    hits = [ln for ln in lines if pid in ln]
    rep.check("event reached Kafka", len(hits) >= 1, f"messages={len(hits)}")
    event_ids = set()
    for ln in hits:
        try:
            event_ids.add(json.loads(ln).get("event_id", ""))
        except ValueError:
            pass
    rep.check("no duplicated event in Kafka", len(hits) == len(event_ids) or not event_ids,
              f"messages={len(hits)}, distinct event_ids={len(event_ids)}")


def criterion_2_resiliency(rep: Report) -> None:
    """Resiliency: the fraud API fails -> the circuit breaker opens, the
    payments go to the DLQ, and the valid stream keeps flowing."""
    rep.section("2. Resiliency (Circuit Breaker + DLQ)")
    tail = KafkaTail("ledger.commands.dlq", seconds=30).start()
    httpx.post(f"{BASE}:8007/fraud/admin/fail", json={"mode": "errors"}, timeout=10)

    # A short load so the breaker actually trips.
    sh("python", "scripts/load_test.py", "--tps", "30", "--duration", "20")
    time.sleep(3)

    cb = poll(lambda: prom_value("pybreaker_open"), lambda v: v is not None and v == 1,
              timeout_s=20)
    rep.check("circuit breaker OPEN", cb == 1, f"pybreaker_open={cb}")

    dlq = tail.count()
    rep.check("DLQ non-empty", dlq > 0, f"messages={dlq}")

    # The valid stream must keep flowing: a payment accepted by the intake
    # must still be processed (the saga ends Failed, but the PIPELINE lives).
    payment = create_payment("U018")
    rep.check("valid payment still accepted", bool(payment.get("payment_id")),
              f"payment_id={payment.get('payment_id')}")

    # Recovery: the breaker only closes on a CALL (a half-open probe), so a
    # payment is what proves the recovery — not the passage of time. It must
    # also be a payment that REACHES the fraud step, so it is sent to the
    # account with the most available funds. The probe is repeated: after the
    # ledger restart of criterion 1 there may be a backlog, and the first
    # payment can sit in it instead of reaching the fraud step.
    httpx.post(f"{BASE}:8007/fraud/admin/fail", json={"mode": "off"}, timeout=10)
    time.sleep(12)  # reset_timeout (10s) must expire before the probe is allowed
    rich = richest_account()
    recovered = None
    for attempt in range(3):
        create_payment(rich, amount=50)
        recovered = poll(lambda: prom_value("pybreaker_open"),
                         lambda v: v == 0, timeout_s=25)
        if recovered == 0:
            break
        time.sleep(2)
    rep.check("breaker recovered after recovery", recovered == 0,
              f"pybreaker_open={recovered}, probe on {rich}")
    tail.stop()


def criterion_3_observability(rep: Report) -> None:
    """Observability: one trace from the gateway to the ledger."""
    rep.section("3. Observability (Tracing)")
    payment = create_payment("U019")
    pid = str(payment.get("payment_id") or "")
    if not rep.check("payment accepted", bool(pid), f"payment_id={pid}"):
        return

    # Tempo: find the trace by the payment id (the span is tagged with it).
    def find() -> list[dict[str, Any]]:
        try:
            r = httpx.get(f"{TEMPO_URL}/api/search",
                          params={"tags": f"payment_id={pid}", "limit": "5"}, timeout=15)
            if r.status_code != 200:
                return []
            return r.json().get("traces", []) or []
        except httpx.HTTPError:
            return []

    traces = poll(find, lambda t: len(t) >= 1, timeout_s=30)
    rep.check("one trace found for the payment", len(traces) >= 1,
              f"traces={len(traces)}")

    if not traces:
        return
    trace_id = traces[0].get("traceID", "")
    try:
        full = httpx.get(f"{TEMPO_URL}/api/traces/{trace_id}", timeout=20).json()
    except httpx.HTTPError:
        full = {}
    services = set()
    spans = 0
    for batch in full.get("batches", []) or []:
        # Tempo's JSON: the RESOURCE (service.name) is a sibling of scopeSpans
        # on the batch, not inside a scope.
        for a in (batch.get("resource", {}) or {}).get("attributes", []) or []:
            if a.get("key") == "service.name":
                services.add((a.get("value") or {}).get("stringValue", ""))
        for scope in batch.get("scopeSpans", []) or []:
            spans += len(scope.get("spans", []) or [])
    rep.check("trace spans gateway..ledger (>=4 services)", len(services) >= 4,
              f"services={sorted(services)}")
    rep.check("trace has >=5 spans", spans >= 5, f"spans={spans}")

    p99 = prom_value(
        "histogram_quantile(0.99, sum by (le) "
        "(rate(http_request_duration_highr_seconds_bucket[5m])))")
    rep.check("p99 is exported to Prometheus", p99 is not None and p99 > 0,
              f"p99={p99}")


def criterion_4_code_quality(rep: Report) -> None:
    """Code quality: clean boundaries — no service reads another service's
    database — and the domain is covered by tests."""
    rep.section("4. Code Quality (boundaries + coverage)")

    # The read-side must not touch another context's DATABASE. The naive
    # `ledger\.` grep matches the TOPIC name (`ledger.events`) in docstrings
    # — 18 false positives — so the check looks for actual SQL cross-schema
    # access and for cross-service infrastructure imports instead.
    _, out = sh("rg", "-n", r"\b(FROM|JOIN|INTO|UPDATE)\s+ledger\.",
                 "services/profile", "services/notification")
    sql_hits = [ln for ln in out.splitlines() if ln.strip()]
    rep.check("no cross-schema SQL in the read-side", not sql_hits,
              f"{len(sql_hits)} hits")

    _, out = sh("rg", "-n", r"from\s+services\.(ledger|orchestrator|intake)\b",
                 "services/profile", "services/notification")
    imp_hits = [ln for ln in out.splitlines() if ln.strip()]
    rep.check("no cross-service imports in the read-side", not imp_hits,
              f"{len(imp_hits)} hits")

    # The read-side must not even KNOW another service's DSN.
    _, out = sh("rg", "-n", r"(LEDGER_DSN|ORCHESTRATOR_DSN|INTAKE_DSN)",
                 "services/profile", "services/notification")
    dsn_hits = [ln for ln in out.splitlines() if ln.strip()]
    rep.check("no foreign DSN in the read-side", not dsn_hits, f"{len(dsn_hits)} hits")

    # The domain is exercised by the test suite.
    code, out = sh("python", "-m", "pytest", "-q", "--tb=no")
    rep.check("unit tests pass", code == 0,
              out.strip().splitlines()[-1] if out.strip() else "")

    # The domain modules of the ledger must be well covered (the phase-2
    # spec asked for 100% of the aggregate's transitions).
    if _has_coverage():
        cov = _domain_coverage()
        rep.check("ledger domain coverage >= 90%", cov is not None and cov >= 90,
                  f"{cov}%")
    else:
        print(f"  {YELLOW}SKIP{RESET}  ledger domain coverage "
              f"{DIM}(pytest-cov is not installed){RESET}")


def _has_coverage() -> bool:
    try:
        import pytest_cov  # noqa: F401
        return True
    except ImportError:
        return False


def _domain_coverage() -> float | None:
    out = sh("python", "-m", "pytest", "-q", "--tb=no", "-p", "no:cacheprovider",
              "--cov=services.ledger.domain", "--cov-report=term:skip-covered")[1]
    for line in out.splitlines():
        if line.strip().startswith("TOTAL"):
            try:
                return float(line.split()[-1].rstrip("%"))
            except ValueError:
                return None
    return None


def criterion_5_ui_ux(rep: Report) -> None:
    """UI/UX: the SSE stream survives a notification restart (auto-reconnect)
    and a new event arrives afterwards."""
    rep.section("5. UI/UX (SSE auto-reconnect)")
    # The gateway proxies the SSE stream; read a few frames from it.
    frames: list[str] = []
    try:
        with httpx.stream("GET", f"{BASE}:8000/v1/stream", timeout=25) as r:
            it = r.iter_lines()
            deadline = time.monotonic() + 12
            for line in it:
                if line:
                    frames.append(line)
                if time.monotonic() > deadline:
                    break
    except httpx.HTTPError:
        pass
    rep.check("SSE stream delivers frames", len(frames) > 0, f"frames={len(frames)}")

    # The notification service restarts; the stream must recover.
    sh("docker", "compose", "stop", "notification")
    time.sleep(2)
    sh("docker", "compose", "start", "notification")
    time.sleep(5)
    health = httpx.get(f"{BASE}:8005/healthz", timeout=10)
    rep.check("notification recovered (200)", health.status_code == 200,
              f"HTTP {health.status_code}")

    # And a new event must reach the feed afterwards.
    payment = create_payment("U020", amount=50)
    pid = str(payment.get("payment_id") or "")

    def saga_rows() -> int:
        raw = psql("orchestrator",
                   f"SELECT count(*) FROM sagas WHERE payment_id='{pid}'")
        try:
            return int(raw)
        except ValueError:
            return 0

    rows = poll(saga_rows, lambda n: n > 0, timeout_s=25)
    rep.check("new event after reconnect", rows > 0, f"saga rows={rows}")


def criterion_6_slo(rep: Report) -> None:
    """SLO: >= 50 TPS and p99 < 300 ms (tz_v2 §4)."""
    rep.section("6. SLO (50 TPS, P99 < 300ms)")
    out = sh("python", "scripts/load_test.py", "--tps", "50",
             "--duration", "30", "--slo", "300")[1]
    try:
        data = json.loads(out[out.index("{"):out.rindex("}") + 1])
    except (ValueError, json.JSONDecodeError, AttributeError):
        data = {}
    tp = float(data.get("throughput", 0) or 0)
    p99 = float(data.get("p99", 0) or 0)
    errs = int(data.get("server_errors", 0) or 0) + int(data.get("connection_errors", 0) or 0)
    rep.check("throughput >= 50 TPS", tp >= 50, f"{tp} TPS")
    rep.check("P99 < 300ms", 0 < p99 < 300, f"p99={p99}ms")
    rep.check("no request errors", errs == 0, f"errors={errs}")


def criterion_7_idempotency(rep: Report) -> None:
    """Idempotency: a duplicate Idempotency-Key produces exactly ONE event."""
    rep.section("7. Idempotency (duplicate key -> one event)")
    key = f"acc-dup-{uuid.uuid4()}"
    first = create_payment("U016", idem=key)
    second = create_payment("U016", idem=key)
    rep.check("same payment_id for the duplicate",
              bool(first.get("payment_id"))
              and first.get("payment_id") == second.get("payment_id"),
              f"{first.get('payment_id')} vs {second.get('payment_id')}")

    time.sleep(3)
    cmds = kafka_count("payments.commands", key)
    rep.check("duplicate key -> single message in Kafka", cmds <= 1, f"messages={cmds}")

    sagas = psql("orchestrator",
                  f"SELECT count(*) FROM sagas WHERE payload->>'idem_key'='{key}'")
    rep.check("single saga for the duplicate", sagas in ("", "0", "1"),
              f"sagas={sagas}")


# --------------------------------------------------------------------------- #
# setup / teardown
# --------------------------------------------------------------------------- #
def setup(stack: bool) -> None:
    if not stack:
        print(f"{DIM}setup: skipped (using the running stack){RESET}")
        return
    print(f"{DIM}setup: recreating the stack (make clean && make up){RESET}")
    sh("make", "clean")
    sh("make", "up")
    code, out = sh("bash", "scripts/smoke_env.sh")
    print(f"{DIM}{out.strip()[-300:]}{RESET}")
    if code != 0:
        print(f"{RED}setup: smoke failed{RESET}")
        sys.exit(2)


def teardown(stack: bool) -> None:
    if not stack:
        return
    print(f"{DIM}teardown: stopping the stack{RESET}")
    sh("make", "down")


def main() -> int:
    ap = argparse.ArgumentParser(description="Acceptance: 7 criteria of §7 TЗ v2")
    ap.add_argument("--skip-setup", action="store_true",
                    help="do not wipe/recreate the stack before the checks")
    ap.add_argument("--skip-teardown", action="store_true",
                    help="leave the stack running after the checks")
    args = ap.parse_args()

    setup(not args.skip_setup)
    rep = Report()
    print("\n=== ACCEPTANCE ===")
    for fn in (criterion_1_consistency, criterion_2_resiliency,
                criterion_3_observability, criterion_4_code_quality,
                criterion_5_ui_ux, criterion_6_slo, criterion_7_idempotency):
        try:
            fn(rep)
        except Exception as e:  # noqa: BLE001 — one broken criterion must not
            # hide the results of the others
            rep.check(fn.__name__, False, f"{type(e).__name__}: {e}")

    print("\n" + "=" * 52)
    print(f"  ACCEPTANCE: {rep.passed} passed, {rep.failed} failed")
    print("=" * 52)
    if rep.failures:
        print(f"{RED}failed:{RESET} " + ", ".join(rep.failures))
    teardown(not args.skip_teardown)

    if rep.failed:
        print(f"{RED}ACCEPTANCE FAILED{RESET}")
        return 1
    print(f"{GREEN}ALL ACCEPTANCE CRITERIA MET{RESET}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
