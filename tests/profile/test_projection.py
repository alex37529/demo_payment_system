"""Pure balance-projection tests: the event → should-we-update decision.

The decision is pure (no DB, no Kafka) so we can test it directly against
canonical event dicts in the shape that the ledger's `to_json()` produces
(see `shared.domain.events.DomainEvent.to_json`).
"""
from __future__ import annotations

from services.profile.projection import resolve_new_balance


def test_final_balance_event_sets_new_balance():
    """FundsDebited (new_balance = current) must set the read-model balance."""
    assert resolve_new_balance({
        "event_type": "FundsDebited",
        "account_id": "U001",
        "new_balance": {"value": 619900, "currency": "USD"},
    }) == 619900


def test_reserved_is_delta_only_skipped():
    """FundsReserved has no new_balance — the final balance is unchanged."""
    assert resolve_new_balance({
        "event_type": "FundsReserved",
        "account_id": "U001",
        "amount": {"value": 50, "currency": "USD"},
    }) is None


def test_cancel_reservation_is_delta_only_skipped():
    assert resolve_new_balance({
        "event_type": "CancelReservationPosted",
        "account_id": "U001",
        "amount": {"value": 50, "currency": "USD"},
        "reason": "fraud decline",
    }) is None


def test_refund_no_new_balance_skipped():
    """RefundPosted canonical event has no new_balance — skipped (per spec §3.1
    'if new_balance is None, continue')."""
    assert resolve_new_balance({
        "event_type": "RefundPosted",
        "account_id": "U001",
        "amount": {"value": 10, "currency": "USD"},
    }) is None


def test_missing_account_id_skipped():
    assert resolve_new_balance({
        "event_type": "FundsDebited",
        "new_balance": {"value": 100, "currency": "USD"},
    }) is None
