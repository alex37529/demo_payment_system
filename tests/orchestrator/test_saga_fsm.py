"""Orchestrator domain tests: SagaStateMachine transitions, guards, restore, FSM
authority on which states are terminal, compensation map, and saga aggregate.
"""
from __future__ import annotations

import pytest

from statemachine.exceptions import TransitionNotAllowed

from services.orchestrator.domain import (
    COMPENSATIONS,
    Saga,
    SagaStateMachine,
    compensation_for,
    normalize_amount,
)

# --------------------------------------------------------------------------- #
# state machine
# --------------------------------------------------------------------------- #
def test_fsm_happy_path_all_states():
    """Created -> Checking -> Reserve -> Fraud -> Debit -> Success."""
    sm = SagaStateMachine()
    assert sm.current_state_value == "created"
    sm.start_checking()
    sm.limits_ok = True
    sm.enter_reserve()
    sm.enter_fraud()
    sm.enter_debit()
    sm.complete()
    assert sm.current_state_value == "success"
    assert sm.reached_terminal()


def test_fsm_limits_guard_is_blocked_by_default():
    """Default limits_ok=False — must not be able to enter_reserve (pitfall #1)."""
    sm = SagaStateMachine()
    sm.start_checking()
    with pytest.raises(TransitionNotAllowed):
        sm.enter_reserve()
    assert sm.current_state_value == "checking"  # still checking


def test_fsm_restore_from_persisted_state():
    """Restore from a persisted state name; then continue (pitfall #2)."""
    sm = SagaStateMachine.restore("Fraud", limits_ok=True)
    assert sm.current_state_value == "fraud"
    sm.enter_debit()
    sm.complete()
    assert sm.current_state_value == "success"


def test_fsm_compensation_paths():
    """Failure in different states leads to the right terminal state."""
    # at `checking` (no reservation): fail directly.
    s1 = SagaStateMachine.restore("Checking", limits_ok=True)
    s1.fail()
    assert s1.current_state_value == "failed"
    # at `fraud` (reservation confirmed): must rollback.
    s2 = SagaStateMachine.restore("Fraud")
    s2.rollback()
    assert s2.current_state_value == "rolledback"
    # at `debit`: same.
    s3 = SagaStateMachine.restore("Debit")
    s3.rollback()
    assert s3.current_state_value == "rolledback"


def test_fsm_illegal_transitions_are_blocked():
    """You can't `complete` from `checking` (you must go through the steps)."""
    sm = SagaStateMachine.restore("Checking", limits_ok=True)
    with pytest.raises(TransitionNotAllowed):
        sm.complete()
    # Also can't `enter_reserve` without first `start_checking`.
    sm2 = SagaStateMachine()
    with pytest.raises(TransitionNotAllowed):
        sm2.enter_reserve()


def test_fsm_final_states():
    finals = {s.value for s in SagaStateMachine.final_states}
    assert finals == {"success", "failed", "rolledback"}
