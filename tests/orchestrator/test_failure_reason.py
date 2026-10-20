"""Failure-reason bucketing for the `payments_failed_total` metric.

The label a failure lands in is what an operator reads off the dashboard, so
it must be both STABLE (bounded cardinality) and ACCURATE. It was neither:
the watchdog's reason ("watchdog_timeout: saga stalled in in-flight state")
contains the word "timeout" and was bucketed as `timeout` — which reads as
a network/DB timeout, while the real cause is a command that failed a
BUSINESS validation and therefore produced no event at all.
"""
from __future__ import annotations

import pytest

from services.orchestrator.application.saga_handler import _failure_reason


@pytest.mark.parametrize(
    ("step", "reason", "expected"),
    [
        # The watchdog: the saga got no answer and was compensated.
        ("reserve", "watchdog_timeout: saga stalled in in-flight state", "watchdog_stalled"),
        ("debit", "watchdog_timeout: saga stalled in in-flight state", "watchdog_stalled"),
        # The fraud breaker was open.
        ("fraud", "circuit_open_or_error", "cb_open"),
        # The fraud API declined the payment.
        ("fraud", "high_amount", "fraud_decline"),
        ("fraud", "decline", "fraud_decline"),
        # The ledger refused the money.
        ("debit", "insufficient funds", "insufficient_funds"),
        # The ledger's actual rejection text (it never says "insufficient").
        ("reserve", "reserve 60183 exceeds available 40939 (reserved 0)", "insufficient_funds"),
        ("debit", "debit 900 exceeds spendable 100 (balance 100, reserved 0)", "insufficient_funds"),
        # A real timeout.
        ("fraud", "timeout", "timeout"),
    ],
)
def test_failure_reason_buckets(step: str, reason: str, expected: str):
    assert _failure_reason(step, reason) == expected


def test_failure_reason_falls_back_to_the_step():
    """An unknown reason still yields a bounded, meaningful bucket."""
    assert _failure_reason("fraud", "something unexpected") == "fraud_decline"
    assert _failure_reason("debit", "something unexpected") == "insufficient_funds"
    assert _failure_reason("reserve", "something unexpected") == "other"


def test_failure_reason_is_low_cardinality():
    """Every distinct free-text reason must collapse into a small set —
    otherwise the metric creates one series per message."""
    reasons = [
        "watchdog_timeout: saga stalled in in-flight state",
        "watchdog_timeout: saga stalled in in-flight state (payment P-1)",
        "circuit_open_or_error",
        "circuit_open_or_error (retry 2)",
        "high_amount",
        "insufficient funds: need 500, have 10",
        "insufficient funds: need 900, have 3",
    ]
    buckets = {_failure_reason("reserve", r) for r in reasons}
    assert len(buckets) <= 4, buckets
