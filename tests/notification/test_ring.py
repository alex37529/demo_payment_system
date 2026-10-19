"""Ring buffer (PaymentLog) tests (phase-5 spec §4.1)."""
from __future__ import annotations

import pytest

from services.notification.ring import PaymentLog


def _ev(pid: str) -> dict:
    return {"payment_id": pid, "account_id": "U001", "event": "FundsDebited",
            "amount": {"value": 100, "currency": "USD"},
            "new_balance": {"value": 619900, "currency": "USD"},
            "ts": "2026-10-19T12:00:00+00:00", "event_id": f"E-{pid}"}


def test_push_and_latest():
    plog = PaymentLog(maxlen=1000)
    plog.push(_ev("P1"))
    plog.push(_ev("P2"))
    plog.push(_ev("P3"))
    assert len(plog) == 3
    got = plog.latest(10)
    assert [p["payment_id"] for p in got] == ["P3", "P2", "P1"]   # newest first


def test_limit_returns_first_n():
    plog = PaymentLog(maxlen=1000)
    for i in range(1, 6):
        plog.push(_ev(f"P{i}"))
    got = plog.latest(3)
    assert [p["payment_id"] for p in got] == ["P5", "P4", "P3"]


def test_maxlen_evicts_oldest():
    plog = PaymentLog(maxlen=3)
    for i in range(1, 6):
        plog.push(_ev(f"P{i}"))
    assert len(plog) == 3
    got = plog.latest(10)
    assert [p["payment_id"] for p in got] == ["P5", "P4", "P3"]


def test_contains():
    plog = PaymentLog(maxlen=1000)
    plog.push(_ev("P1"))
    plog.push(_ev("P2"))
    assert "P1" in plog
    assert "P3" not in plog


def test_invalid_maxlen():
    with pytest.raises(ValueError):
        PaymentLog(maxlen=0)
