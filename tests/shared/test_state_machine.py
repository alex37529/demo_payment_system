import pytest

from shared.state_machine import PaymentStateMachine

from statemachine.exceptions import TransitionNotAllowed  # type: ignore


def test_happy_path():
    sm = PaymentStateMachine()
    sm.validate()
    sm.start()
    sm.has_debit_ok = True
    sm.succeed()
    assert sm.current_state_value == "success"
    final_values = {s.value for s in sm.final_states}
    assert "success" in final_values


def test_fail_path():
    sm = PaymentStateMachine()
    sm.validate()
    sm.start()
    sm.has_error = True
    sm.fail()
    assert sm.current_state_value == "failed"


def test_rollback_path():
    sm = PaymentStateMachine()
    sm.validate()
    sm.start()
    sm.reservation_made = True
    sm.rollback()
    assert sm.current_state_value == "rolledback"


def test_guard_blocks_unsatisfied_transition():
    sm = PaymentStateMachine()
    sm.validate()
    sm.start()
    # has_debit_ok is False -> succeed must be rejected
    with pytest.raises(TransitionNotAllowed):
        sm.succeed()
    assert sm.current_state_value == "processing"


def test_restore_from_state():
    sm = PaymentStateMachine.restore("Processing", reservation_made=True)
    assert sm.current_state_value == "processing"
    sm.rollback()
    assert sm.current_state_value == "rolledback"
