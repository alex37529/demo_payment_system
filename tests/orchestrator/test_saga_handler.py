"""SagaHandler flow tests — the phase-3 exit criteria, without real Kafka/HTTP.

Uses an in-memory async SQLite DB (real repository + saga + state machine) and
fakes for the two outward dependencies:
  * ``publisher`` — an in-memory recorder (no Kafka),
  * ``fraud``      — a stub with a controllable decision (approve / decline /
                     circuit-open), standing in for the CB-protected client.

Covers:
  * happy path  Create -> FundsReserved -> FundsDebited == Success;
  * fraud decline after a confirmed reservation -> RolledBack + CancelReservation + DLQ;
  * fraud CB-open (route_to_dlq)  -> RolledBack + compensation + DLQ (valid ones keep flowing);
  * replay `Create` for the same payment_id does NOT re-run the saga;
  * a bad amount is routed straight to the DLQ.
"""
from __future__ import annotations

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from services.orchestrator.application import (
    SagaHandler,
    TOPIC_DLQ,
    TOPIC_LEDGER_COMMANDS,
    TOPIC_PAYMENTS_EVENTS,
)
from services.orchestrator.infrastructure import SagaRepository
from shared.db.base import Base


class FakePublisher:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, dict]] = []

    async def publish(self, topic: str, key: str, value: dict) -> None:
        self.sent.append((topic, key, value))

    def for_topic(self, t: str) -> list[dict]:
        return [v for (tt, _, v) in self.sent if tt == t]


class FakeFraud:
    def __init__(self, decision: str = "approve", score: float = 0.2, reason: str = "") -> None:
        self.decision, self.score, self.reason = decision, score, reason

    async def post(self, url: str, json: dict) -> dict:
        return {"decision": self.decision, "score": self.score, "reason": self.reason}

    def state(self) -> str:
        return "CLOSED"

    def is_open(self) -> bool:
        return False

    async def close(self) -> None:
        return None


@pytest_asyncio.fixture
async def env():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as c:
        await c.run_sync(Base.metadata.create_all)
    sf = async_sessionmaker(engine, expire_on_commit=False)
    repo = SagaRepository(sf)
    yield repo, sf
    await engine.dispose()


def make_handler(repo, *, decision="approve", score=0.2, reason="") -> tuple[SagaHandler, FakePublisher]:
    pub = FakePublisher()
    return (SagaHandler(repo=repo, publisher=pub, fraud=FakeFraud(decision, score, reason),
                        fraud_url="http://fraud:8080/fraud/check"), pub)


async def _state(repo, payment_id: str) -> str:
    return (await repo.get_by_payment(payment_id)).state


async def test_happy_path_create_reserved_debited(env):
    repo, _ = env
    h, pub = make_handler(repo)
    await h.on_create("P100", {"cmd": "Create", "payment_id": "P100",
                               "account_id": "A1", "amount": {"value": 100, "currency": "USD"}})
    row = await repo.get_by_payment("P100")
    assert row.state == "Reserve"
    assert [x["cmd"] for x in pub.for_topic(TOPIC_LEDGER_COMMANDS)] == ["Reserve"]
    assert [x["event_type"] for x in pub.for_topic(TOPIC_PAYMENTS_EVENTS)] == ["LimitsChecked"]

    # ledger confirms the reservation
    await h.on_funds_reserved("P100", {"event_type": "FundsReserved", "payment_id": "P100",
                                       "saga_id": row.saga_id, "account_id": "A1",
                                       "amount": {"value": 100, "currency": "USD"}})
    row = await repo.get_by_payment("P100")
    assert row.state == "Debit"
    assert any(x["cmd"] == "Debit" for x in pub.for_topic(TOPIC_LEDGER_COMMANDS))

    # ledger debits -> success
    await h.on_funds_debited("P100", {"event_type": "FundsDebited", "payment_id": "P100",
                                      "saga_id": row.saga_id, "new_balance": {"value": 400, "currency": "USD"}})
    row = await repo.get_by_payment("P100")
    assert row.state == "Success"
    events = [x["event_type"] for x in pub.for_topic(TOPIC_PAYMENTS_EVENTS)]
    assert events == ["LimitsChecked", "FraudVerdict", "SagaSucceeded"]


async def test_fraud_decline_after_reserve_is_rolledback_with_compensation(env):
    repo, _ = env
    h, pub = make_handler(repo, decision="decline", score=0.95, reason="high_amount")
    await h.on_create("P200", {"cmd": "Create", "payment_id": "P200", "account_id": "A1",
                               "amount": {"value": 50, "currency": "USD"}})
    row = await repo.get_by_payment("P200")
    await h.on_funds_reserved("P200", {"event_type": "FundsReserved", "payment_id": "P200",
                                       "saga_id": row.saga_id})
    row = await repo.get_by_payment("P200")
    assert row.state == "RolledBack"
    assert row.compensation == ["CancelReservation"]
    # compensation command was issued + DLQ got the reason
    assert any(x["cmd"] == "CancelReservation" for x in pub.for_topic(TOPIC_LEDGER_COMMANDS))
    dlq = pub.for_topic(TOPIC_DLQ)
    assert dlq and dlq[-1]["failed_step"] == "fraud"


async def test_fraud_circuit_open_goes_to_dlq_not_blocking(env):
    repo, _ = env
    # CB-open: the fallback returns route_to_dlq (see fraud_client._fraud_fallback)
    h, pub = make_handler(repo, decision="route_to_dlq", score=100.0, reason="circuit_open_or_error")
    await h.on_create("P300", {"cmd": "Create", "payment_id": "P300", "account_id": "A1",
                               "amount": {"value": 20, "currency": "USD"}})
    row = await repo.get_by_payment("P300")
    await h.on_funds_reserved("P300", {"event_type": "FundsReserved", "payment_id": "P300",
                                       "saga_id": row.saga_id})
    row = await repo.get_by_payment("P300")
    assert row.state == "RolledBack"
    assert pub.for_topic(TOPIC_DLQ)
    # and a subsequent *valid* payment can still proceed (CB only guards fraud)
    h_ok, pub_ok = make_handler(repo, decision="approve")
    await h_ok.on_create("P301", {"cmd": "Create", "payment_id": "P301", "account_id": "A1",
                                  "amount": {"value": 10, "currency": "USD"}})
    r = await repo.get_by_payment("P301")
    assert r.state == "Reserve"  # not blocked


async def test_create_replay_does_not_rerun(env):
    repo, _ = env
    h, pub = make_handler(repo)
    await h.on_create("P400", {"cmd": "Create", "payment_id": "P400", "account_id": "A1",
                               "amount": {"value": 10, "currency": "USD"}})
    count_before = len(pub.for_topic(TOPIC_LEDGER_COMMANDS))
    await h.on_create("P400", {"cmd": "Create", "payment_id": "P400", "account_id": "A1",
                               "amount": {"value": 10, "currency": "USD"}})
    assert len(pub.for_topic(TOPIC_LEDGER_COMMANDS)) == count_before  # no 2nd Reserve
    assert (await _state(repo, "P400")) == "Reserve"


async def test_bad_amount_routes_to_dlq(env):
    repo, _ = env
    h, pub = make_handler(repo)
    await h.on_create("P500", {"cmd": "Create", "payment_id": "P500", "account_id": "A1",
                               "amount": {"value": -5, "currency": "USD"}})
    assert pub.for_topic(TOPIC_DLQ)
    assert await repo.get_by_payment("P500") is None  # no saga created for a bad command
