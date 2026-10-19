"""kafka_consumer unwrap/dispatch tests — the Debezium `ledger.events` shape.

Phase 3 live finding: Debezium + `ExtractNewRecordState` hands the orchestrator
the **outbox row** with the event nested inside a `payload` **JSON string**:
    {"id":1, "topic":"ledger.events", "payload":"{...}", "status":"pending"}
(plus older pre-phase-3 rows without the `event_type` field). These tests lock
in `_unwrap` and `_event_type` against that exact shape.
"""
from __future__ import annotations

import json

from services.orchestrator.kafka_consumer import _event_type, _unwrap

# A realistic live Debezium message (event_type present, payload a JSON string)
LIVE_RESERVED = {
    "id": 1, "topic": "ledger.events", "status": "pending",
    "payload": json.dumps({"event_type": "FundsReserved", "account_id": "A1",
                           "payment_id": "P1", "saga_id": "S1",
                           "amount": {"value": 100, "currency": "USD"}}),
}
LIVE_DEBITED = {
    "id": 2, "topic": "ledger.events", "status": "pending",
    "payload": json.dumps({"event_type": "FundsDebited", "account_id": "A1",
                           "payment_id": "P1", "saga_id": "S1",
                           "amount": {"value": 100, "currency": "USD"},
                           "new_balance": {"value": 400, "currency": "USD"}}),
}
# A pre-phase-3 row (no event_type) — must still be classified by shape.
OLD_DEBITED = {
    "id": 3, "topic": "ledger.events", "status": "pending",
    "payload": json.dumps({"account_id": "A2", "payment_id": "P42",
                           "amount": {"value": 5000, "currency": "USD"},
                           "new_balance": {"value": 995000, "currency": "USD"}}),
}


def test_unwrap_parses_json_string_payload():
    inner = _unwrap(LIVE_RESERVED)
    assert inner["event_type"] == "FundsReserved"
    assert inner["payment_id"] == "P1"


def test_unwrap_passthrough_for_inline_dict():
    # payments.commands payloads are inline dicts — must pass through.
    rec = {"cmd": "Create", "payment_id": "P9", "amount": {"value": 5, "currency": "USD"}}
    assert _unwrap(rec) == rec
    # and a plain payload-less record degrades to itself
    plain = {"foo": "bar"}
    assert _unwrap(plain) == plain


def test_event_type_prefers_explicit_discriminant():
    assert _event_type(_unwrap(LIVE_RESERVED)) == "FundsReserved"
    assert _event_type(_unwrap(LIVE_DEBITED)) == "FundsDebited"


def test_event_type_infers_old_rows_by_shape():
    assert _event_type(_unwrap(OLD_DEBITED)) == "FundsDebited"


def test_event_type_none_when_unclassifiable():
    assert _event_type({"id": 99, "topic": "ledger.events"}) is None
