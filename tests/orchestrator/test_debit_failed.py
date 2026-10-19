"""Handler paths for `DebitFailed` and compensation-applied events (phase 3).

The ledger consumer in phase 2 does NOT emit `DebitFailed` (it stays up and
logs the business error), so the actual compensation trigger in this phase is
the watchdog. But the handlers are part of the public API:
  * if a future phase starts publishing `DebitFailed` on `ledger.events`,
    `on_debit_failed` will take the saga to `RolledBack` with the reservation
    cancelled — this test proves the handler contract.
  * `on_compensation_applied` records the reverse-command arrival so the
    audit trail is complete.
"""
from __future__ import annotations

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from services.orchestrator.application import SagaHandler, TOPIC_DLQ, TOPIC_LEDGER_COMMANDS, TOPIC_PAYMENTS_EVENTS
from services.orchestrator.infrastructure import SagaRow, SagaRepository
from shared.db.base import Base


class FakePublisher:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, dict]] = []

    async def publish(self, topic: str, key: str, value: dict) -> None:
        self.sent.append((topic, key, value))

    def for_topic(self, t: str) -> list[dict]:
        return [v for (tt, _, v) in self.sent if tt == t]


class FakeFraud:
    async def post(self, url, json): return {"decision": "approve", "score": 0.1}
    def state(self): return "CLOSED"
    def is_open(self): return False
    async def close(self): return None


async def test_on_debit_failed_routes_to_rolledback_with_compensation():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as c:
        await c.run_sync(Base.metadata.create_all)
    sf = async_sessionmaker(engine, expire_on_commit=False)
    repo = SagaRepository(sf)
    pub = FakePublisher()
    h = SagaHandler(repo=repo, publisher=pub, fraud=FakeFraud(), fraud_url="u")

    # Simulate: saga advanced to `Debit` (a reservation was confirmed).
    async with sf() as s:
        s.add(SagaRow(saga_id="S1", payment_id="P1", state="Debit",
                      history=[{"step": "check", "cmd": "CheckLimits", "event": "LimitsChecked", "ts": "x", "ok": True, "err": ""},
                               {"step": "reserve", "cmd": "Reserve", "event": "FundsReserved", "ts": "x", "ok": True, "err": ""}],
                      payload={"account_id": "A1", "amount": {"value": 100, "currency": "USD"}},
                      compensation=[]))
        await s.commit()

    await h.on_debit_failed("P1", {"event_type": "DebitFailed", "payment_id": "P1",
                                    "saga_id": "S1", "reason": "insufficient funds"})
    row = await repo.get_by_payment("P1")
    assert row.state == "RolledBack"
    assert row.compensation == ["CancelReservation"]
    # and both the compensation command AND the DLQ record went out
    assert any(x["cmd"] == "CancelReservation" for x in pub.for_topic(TOPIC_LEDGER_COMMANDS))
    assert pub.for_topic(TOPIC_DLQ)
    events = [x["event_type"] for x in pub.for_topic(TOPIC_PAYMENTS_EVENTS)]
    assert "SagaFailed" in events
    assert "DebitFailed" in events

    await engine.dispose()


async def test_on_compensation_applied_appends_to_history():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as c:
        await c.run_sync(Base.metadata.create_all)
    sf = async_sessionmaker(engine, expire_on_commit=False)
    repo = SagaRepository(sf)
    pub = FakePublisher()
    h = SagaHandler(repo=repo, publisher=pub, fraud=FakeFraud(), fraud_url="u")

    async with sf() as s:
        s.add(SagaRow(saga_id="S2", payment_id="P2", state="RolledBack",
                      history=[{"step": "fraud", "cmd": "FraudCheck", "event": "failure",
                                "ts": "x", "ok": False, "err": "declined"}],
                      payload={"account_id": "A1", "amount": {"value": 50, "currency": "USD"}},
                      compensation=["CancelReservation"]))
        await s.commit()

    await h.on_compensation_applied("P2", {"payment_id": "P2", "event_type": "CancelReservationPosted"},
                                    event_type="CancelReservationPosted")
    row = await repo.get_by_payment("P2")
    assert row.state == "RolledBack"   # state is preserved (only history grows)
    # the compensation event is now in the history trail
    assert any(h.get("event") == "CancelReservationPosted" for h in row.history)

    await engine.dispose()
