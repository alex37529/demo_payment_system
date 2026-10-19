"""Repository idempotency + consumer dispatch (phase-5 spec §3.1, pitfall #5):
at-least-once Kafka redelivery must not double-apply the read-side balance.
"""
from __future__ import annotations

import json

from services.profile.kafka_consumer import process_record

LIVE_DEBITED = {
    "id": 42, "topic": "ledger.events", "status": "pending",
    "payload": json.dumps({
        "event_type": "FundsDebited",
        "event_id": "E-9",
        "account_id": "U003",
        "payment_id": "P1", "saga_id": "S1",
        "amount": {"value": 200, "currency": "USD"},
        "new_balance": {"value": 349800, "currency": "USD"},
        "ts": "2026-10-19T12:00:00+00:00",
    }),
}
LIVE_RESERVED = {
    "id": 43, "topic": "ledger.events", "status": "pending",
    "payload": json.dumps({
        "event_type": "FundsReserved",
        "event_id": "E-10",
        "account_id": "U001",
        "payment_id": "P2",
        "amount": {"value": 50, "currency": "USD"},
    }),
}


async def test_consumer_applies_funds_debited(harness):
    repo = harness["repo"]
    before = (await repo.get("U003"))["balance"]
    await process_record(LIVE_DEBITED, "key-1", repo)
    after = (await repo.get("U003"))["balance"]
    assert after == 349800
    assert before == 350000


async def test_consumer_reserved_does_not_change_balance(harness):
    repo = harness["repo"]
    await process_record(LIVE_RESERVED, "key-2", repo)
    # U001 seeded as 620000 — unchanged by the reserved event.
    assert (await repo.get("U001"))["balance"] == 620000
    # And the event id is recorded (so a replay is a no-op).
    assert await repo.is_processed("E-10") is True


async def test_consumer_redelivery_is_noop(harness):
    repo = harness["repo"]
    await process_record(LIVE_DEBITED, "key-3", repo)
    bal_first = (await repo.get("U003"))["balance"]
    # Deliver the SAME raw record again — must NOT double-apply.
    await process_record(LIVE_DEBITED, "key-3", repo)
    bal_second = (await repo.get("U003"))["balance"]
    assert bal_first == bal_second == 349800


async def test_apply_event_updates_balance(harness):
    repo = harness["repo"]
    before = (await repo.get("U001"))["balance"]
    assert before == 620000

    applied = await repo.apply_event(event_id="E-1", account_id="U001",
                                     new_balance=619900, event_type="FundsDebited")
    assert applied is True
    assert (await repo.get("U001"))["balance"] == 619900


async def test_apply_event_is_idempotent_on_redelivery(harness):
    """The same ``event_id`` delivered twice -> only ONE balance change."""
    repo = harness["repo"]
    first = await repo.apply_event(event_id="E-DUP", account_id="U002",
                                   new_balance=400000, event_type="FundsDebited")
    second = await repo.apply_event(event_id="E-DUP", account_id="U002",
                                    new_balance=300000, event_type="FundsDebited")
    assert first is True
    assert second is False                       # EXIT: idempotent no-op
    assert (await repo.get("U002"))["balance"] == 400000  # 2nd value did NOT stick


async def test_is_processed(harness):
    repo = harness["repo"]
    assert await repo.is_processed("E-NO") is False
    await repo.mark_processed("E-NO", event_type="FundsReserved", account_id="U003")
    assert await repo.is_processed("E-NO") is True


async def test_apply_unknown_account_is_noop_not_error(harness):
    repo = harness["repo"]
    applied = await repo.apply_event(event_id="E-UNK", account_id="U999",
                                     new_balance=1, event_type="FundsDebited")
    assert applied is False                      # nothing to update, not raised
    assert (await repo.get("U999")) is None
