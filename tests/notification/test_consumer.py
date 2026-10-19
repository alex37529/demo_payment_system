"""Notification consumer -> ring + relay fan-out (phase-5 spec 4.1, commit #6).

Drives the *consumer-side* pipeline end-to-end WITHOUT a broker: normalize a
Debezium ``ledger.events`` record to a feed payment (``to_payment``), push it to
the ring buffer, and broadcast it to N subscribers — then assert ``/v1/payments``
serves it and a live subscriber received it.
"""
from __future__ import annotations

import json

from services.notification.kafka_consumer import to_payment

# A live Debezium + `ExtractNewRecordState` record (payload is a JSON string).
LIVE_DEBITED = {
    "id": 7, "topic": "ledger.events", "status": "pending",
    "payload": json.dumps({
        "event_type": "FundsDebited",
        "event_id": "EE-1",
        "account_id": "U001",
        "payment_id": "P1", "saga_id": "S1",
        "amount": {"value": 100, "currency": "USD"},
        "new_balance": {"value": 619900, "currency": "USD"},
        "ts": "2026-10-19T12:00:00+00:00",
    }),
}


def test_to_payment_from_debezium_record():
    p = to_payment(LIVE_DEBITED)
    assert p["account_id"] == "U001"
    assert p["payment_id"] == "P1"
    assert p["event_id"] == "EE-1"
    # `event` is the canonical name from the record (event_type preferred).
    assert p["event"] == "FundsDebited"
    assert p["new_balance"] == {"value": 619900, "currency": "USD"}


def test_to_payment_old_row_infers_event_by_shape():
    # A pre-discriminator record (no event_type) still classifies (phase-3
    # heuristic, mirrored here for the feed).
    rec = {"account_id": "U001", "amount": {"value": 5, "currency": "USD"},
           "new_balance": {"value": 1, "currency": "USD"}}
    assert to_payment(rec)["event"] == "FundsDebited"
    rec2 = {"account_id": "U001", "amount": {"value": 5, "currency": "USD"}}
    assert to_payment(rec2)["event"] == "FundsReserved"


async def test_consumer_pipeline_pushes_ring_and_fans_out(notif):
    """Simulate one consumed event: push to the ring + broadcast to subscribers."""
    plog = notif["plog"]
    relay = notif["relay"]

    q = relay.subscribe()                       # a live SSE client
    try:
        payment = to_payment(LIVE_DEBITED)
        plog.push(payment)                      # ring buffer (last N)
        relay.broadcast(payment)                # fan-out to N subscribers

        assert len(plog) == 1
        assert plog.latest(5)[0]["payment_id"] == "P1"
        assert q.qsize() == 1                   # the subscriber got it
        assert q.get_nowait() == payment

        # and GET /v1/payments serves the ring
        r = await notif["http"].get("/v1/payments", params={"limit": 10})
        assert r.status_code == 200
        body = r.json()
        assert body["count"] == 1
        assert body["payments"][0]["payment_id"] == "P1"
    finally:
        relay.unsubscribe(q)


async def test_fanout_to_multiple_subscribers(notif):
    """1 event -> N subscribers (commit #6 'fan-out broadcast')."""
    plog = notif["plog"]
    relay = notif["relay"]
    subs = [relay.subscribe() for _ in range(3)]
    try:
        payment = to_payment(LIVE_DEBITED)
        plog.push(payment)
        relay.broadcast(payment)
        for q in subs:
            assert q.qsize() == 1
    finally:
        for q in subs:
            relay.unsubscribe(q)
