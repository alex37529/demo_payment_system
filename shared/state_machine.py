"""Payment State Machine (mapping §3.4 TЗ v2).

States (top-level): Created, Validating, Processing (sub: reserved), Success,
Failed, RolledBack. `Reserved` is a sub-state of Processing, not a top state.

Guards are normal methods referenced by name (python-statemachine resolves them
on the instance). They default to False so a transition is blocked until the
orchestrator sets the relevant flag (pitfall #23).
"""
from __future__ import annotations

from statemachine import State as SMState
from statemachine import StateMachine


class PaymentStateMachine(StateMachine):
    created = SMState("Created", initial=True)
    validating = SMState("Validating")
    processing = SMState("Processing")  # sub-state: reserved
    success = SMState("Success", final=True)
    failed = SMState("Failed", final=True)
    rolledback = SMState("RolledBack", final=True)

    # Guard flags (settable on the instance by the orchestrator).
    has_debit_ok: bool = False
    has_error: bool = False
    reservation_made: bool = False

    def _has_debit_ok(self) -> bool:
        return self.has_debit_ok

    def _has_error(self) -> bool:
        return self.has_error

    def _reservation_made(self) -> bool:
        return self.reservation_made

    # Transitions (per mapping §3.4)
    validate = created.to(validating)
    start = validating.to(processing)
    succeed = processing.to(success, cond=_has_debit_ok)
    fail = processing.to(failed, cond=_has_error)
    rollback = processing.to(rolledback, cond=_reservation_made)

    @classmethod
    def restore(cls, state: str, *, has_debit_ok: bool = False,
                has_error: bool = False, reservation_made: bool = False) -> "PaymentStateMachine":
        """Rebuild from a persisted state string (Orchestrator, phase 3)."""
        value = {"Created": "created", "Validating": "validating", "Processing": "processing",
                 "Success": "success", "Failed": "failed", "RolledBack": "rolledback"}.get(state, "created")
        machine = cls(start_value=value)
        machine.has_debit_ok = has_debit_ok
        machine.has_error = has_error
        machine.reservation_made = reservation_made
        return machine
