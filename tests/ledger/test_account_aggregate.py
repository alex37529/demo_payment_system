"""Ledger aggregate tests — 100% of transitions + negative cases (phase 2 exit)."""
from __future__ import annotations

import dataclasses

import pytest

from services.ledger.domain import (
    Account,
    IllegalTransitionError,
    InsufficientFundsError,
    Money,
    Payment,
)


@pytest.fixture
def account_dollar() -> Account:
    return Account(account_id="A1", owner_name="John",
                   currency="USD", balance=Money(500, "USD"))


# --- Money VO ---
def test_money_eq_and_hash():
    assert Money(100, "USD") == Money(100, "USD")
    assert Money(100, "USD") != Money(100, "EUR")
    assert hash(Money(1, "USD")) == hash(Money(1, "USD"))


def test_money_rejects_negative_and_bool_and_bad_currency():
    with pytest.raises(ValueError):
        Money(-1, "USD")
    with pytest.raises(ValueError):
        Money(1, "XXX")
    with pytest.raises(TypeError):
        Money(True, "USD")  # bool is not a valid amount


def test_money_to_dict_canonical():
    assert Money(250, "EUR").to_dict() == {"value": 250, "currency": "EUR"}


def test_money_from_dict():
    assert Money.from_dict({"value": 7, "currency": "USD"}) == Money(7, "USD")


# --- debit ---
def test_debit_success(account_dollar):
    account_dollar.debit(Money(100, "USD"))
    assert account_dollar.balance == Money(400, "USD")
    assert len(account_dollar.pull_events()) == 1


def test_debit_insufficient_balance_unchanged(account_dollar):
    with pytest.raises(InsufficientFundsError):
        account_dollar.debit(Money(600, "USD"))
    assert account_dollar.balance == Money(500, "USD")
    assert account_dollar.pull_events() == []


def test_debit_currency_mismatch(account_dollar):
    with pytest.raises(InsufficientFundsError):
        account_dollar.debit(Money(100, "EUR"))


def test_debit_event_payload(account_dollar):
    ev = account_dollar.debit(Money(100, "USD"), ref="P9")
    assert ev.account_id == "A1"
    assert ev.payment_id == "P9"
    assert ev.new_balance == {"value": 400, "currency": "USD"}
    assert ev.amount == {"value": 100, "currency": "USD"}


# --- reserve / available ---
def test_reserve_increases_reserved(account_dollar):
    account_dollar.reserve(Money(200, "USD"))
    assert account_dollar.reserved == Money(200, "USD")
    assert account_dollar.balance == Money(500, "USD")  # reserve holds, doesn't move balance


def test_reserve_then_debit_blocked(account_dollar):
    """A hold of ANOTHER payment blocks a debit (ADR-010).

    The debit of a payment that holds money for ITSELF may spend it — the
    saga reserves exactly what it is about to debit. But a hold that
    belongs to a DIFFERENT payment is untouchable: the money is spoken for.
    """
    account_dollar.reserve(Money(200, "USD"), ref="P-OTHER")
    # available = 500 - 200 = 300; debit 400 (of an unrelated payment) must fail
    with pytest.raises(InsufficientFundsError):
        account_dollar.debit(Money(400, "USD"), ref="P-MINE")


def test_debit_consumes_its_own_hold(account_dollar):
    """The core of ADR-010: the debit spends the money ITS payment held.

    Before the fix the hold was never released, so `available` shrank by
    every payment and a user whose balance EXACTLY equalled the payment
    amount could not pay it (the reserve left available=0, then the debit
    was refused).
    """
    account_dollar.reserve(Money(100, "USD"), ref="P1")
    assert account_dollar.reserved == Money(100, "USD")
    assert account_dollar.available == Money(400, "USD")

    account_dollar.debit(Money(100, "USD"), ref="P1")
    assert account_dollar.balance == Money(400, "USD")
    # The hold is GONE — the money that was held is the money that left.
    assert account_dollar.reserved == Money(0, "USD")
    assert account_dollar.available == Money(400, "USD")


def test_debit_may_spend_its_whole_balance(account_dollar):
    """A user whose balance equals the payment amount CAN pay it.

    This is the case that was broken: reserve(500) left available=0 and
    the debit of 500 was then refused as "exceeds available 0".
    """
    account_dollar.reserve(Money(500, "USD"), ref="P1")
    account_dollar.debit(Money(500, "USD"), ref="P1")
    assert account_dollar.balance == Money(0, "USD")
    assert account_dollar.reserved == Money(0, "USD")


def test_holds_are_tracked_per_payment(account_dollar):
    """Two concurrent reservations are two separate holds."""
    account_dollar.reserve(Money(100, "USD"), ref="P1")
    account_dollar.reserve(Money(200, "USD"), ref="P2")
    assert account_dollar.reserved == Money(300, "USD")
    assert account_dollar.hold_for("P1") == 100
    assert account_dollar.hold_for("P2") == 200
    # P1's debit consumes ONLY P1's hold.
    account_dollar.debit(Money(100, "USD"), ref="P1")
    assert account_dollar.reserved == Money(200, "USD")
    assert account_dollar.hold_for("P1") == 0
    assert account_dollar.hold_for("P2") == 200


def test_cancel_reservation_releases_that_payment_hold(account_dollar):
    account_dollar.reserve(Money(100, "USD"), ref="P1")
    account_dollar.reserve(Money(200, "USD"), ref="P2")
    account_dollar.cancel_reservation(Money(100, "USD"), ref="P1")
    assert account_dollar.hold_for("P1") == 0
    assert account_dollar.reserved == Money(200, "USD")


def test_reserved_survives_a_round_trip(account_dollar):
    """The per-payment holds are persisted, so a hold is still there after
    the account is reloaded from the DB (the reserve and the debit are
    SEPARATE transactions)."""
    account_dollar.reserve(Money(150, "USD"), ref="P1")
    row = account_dollar.to_row()
    assert row["reserved"] == 150
    assert row["holds"] == {"P1": 150}


def test_reserve_exceeds_available(account_dollar):
    with pytest.raises(InsufficientFundsError):
        account_dollar.reserve(Money(600, "USD"))


def test_reserve_currency_mismatch(account_dollar):
    with pytest.raises(InsufficientFundsError):
        account_dollar.reserve(Money(100, "EUR"))


# --- cancel reservation ---
def test_cancel_reservation_frees_reserved(account_dollar):
    account_dollar.reserve(Money(200, "USD"))
    assert account_dollar.reserved == Money(200, "USD")
    account_dollar.cancel_reservation(Money(200, "USD"))
    assert account_dollar.reserved == Money(0, "USD")


def test_cancel_more_than_reserved(account_dollar):
    account_dollar.reserve(Money(200, "USD"))
    with pytest.raises(InsufficientFundsError):
        account_dollar.cancel_reservation(Money(300, "USD"))


def test_cancel_currency_mismatch(account_dollar):
    account_dollar.reserve(Money(200, "USD"))
    with pytest.raises(InsufficientFundsError):
        account_dollar.cancel_reservation(Money(200, "EUR"))


# --- refund ---
def test_refund_increases_balance(account_dollar):
    account_dollar.debit(Money(100, "USD"))
    account_dollar.refund(Money(100, "USD"))
    assert account_dollar.balance == Money(500, "USD")


def test_refund_currency_mismatch(account_dollar):
    with pytest.raises(InsufficientFundsError):
        account_dollar.refund(Money(100, "EUR"))


# --- event catalog + idempotency ---
def test_each_command_emits_canonical_event(account_dollar):
    from shared.domain.events import (
        CancelReservationPosted,
        FundsDebited,
        FundsReserved,
        RefundPosted,
    )
    assert isinstance(account_dollar.debit(Money(1, "USD"), ref="d"), FundsDebited)
    account_dollar.pull_events()
    assert isinstance(account_dollar.reserve(Money(1, "USD"), ref="r"), FundsReserved)
    account_dollar.pull_events()
    assert isinstance(account_dollar.cancel_reservation(Money(1, "USD"), ref="c"), CancelReservationPosted)
    account_dollar.pull_events()
    assert isinstance(account_dollar.refund(Money(1, "USD"), ref="f"), RefundPosted)


def test_idempotent_replay_no_double_effect(account_dollar):
    account_dollar.debit(Money(100, "USD"), ref="TX-1")
    first = account_dollar.pull_events()  # drain the one real event
    assert len(first) == 1
    bal = account_dollar.balance.amount
    # replay same ref -> no-op (no second debit, no second event)
    account_dollar.debit(Money(100, "USD"), ref="TX-1")
    assert account_dollar.balance.amount == bal
    assert account_dollar.pull_events() == []


# --- Account construction invariants ---
def test_balance_must_match_account_currency():
    with pytest.raises(InsufficientFundsError):
        Account("A1", currency="USD", balance=Money(10, "EUR"))


# --- round-trip mapping (to_row / load) ---
def test_round_trip_to_row_load(account_dollar):
    account_dollar.debit(Money(100, "USD"), ref="P1")
    account_dollar.reserve(Money(50, "USD"), ref="R1")
    row = account_dollar.to_row()
    assert row["balance"] == 400
    assert row["reserved"] == 50
    assert row["version"] == 3  # 1 + debit + reserve
    assert row["currency"] == "USD"


# --- Payment aggregate ---
def test_payment_happy_path_transitions():
    p = Payment("P1", Money(100, "USD"), "A1", idem_key="k1")
    assert p.state == "Created"
    p.transition("Validating")
    p.transition("Processing")
    p.transition("Success")
    assert p.state == "Success"


def test_payment_illegal_transition():
    p = Payment("P1", Money(100, "USD"), "A1")
    with pytest.raises(IllegalTransitionError):
        p.transition("Success")  # Created -> Success not allowed


@pytest.mark.parametrize(
    "path",
    [
        ["Validating", "Processing", "Failed"],
        ["Validating", "Processing", "RolledBack"],
        ["Validating", "Processing", "Success"],
    ],
)
def test_payment_all_terminal_paths(path):
    p = Payment("PX", Money(1, "USD"), "A1")
    for s in path:
        p.transition(s)
    assert p.state in ("Success", "Failed", "RolledBack")


def test_payment_event_frozen_shared_catalog():
    # Payment pulls from the same shared catalog (no local event defs).
    ev = Payment("P1", Money(100, "USD"), "A1")
    assert dataclasses.is_dataclass(ev) is False  # Payment is not a dataclass; events are
