"""Saga state machine (python-statemachine v3, via the v2-compatible alias).

Top-level states of a payment saga, per the step map tz_v2 §3.4 / §3.3 and the
phase-3 spec §2.1:

    Created -> Checking -> Reserve -> Fraud -> Debit -> Success
                  |           |         |         |
                  +-----------+---------+---------+----> Failed
                              \\---------------------------------> RolledBack
                               (a reservation was confirmed, a later step failed)

`Success`, `Failed` and `RolledBack` are final. Failure routing is split by
whether a reservation was already *booked*, encoded structurally so an
orchestrator bug cannot silently drop a required compensation:

* `checking` / `reserve` failures -> `fail`     (nothing booked  -> Failed);
* `fraud`    / `debit`   failures -> `rollback` (reservation booked -> must be
  cancelled: CancelReservation [+ Refund] -> RolledBack).

`SagaStateMachine` is the single authority on which transitions are legal.
The `Saga` aggregate records history + payload + the compensation plan; the FSM
guards the path. A machine is rebuilt from a persisted state via `start_value`
(pitfall #2: a fresh `SagaStateMachine()` always starts at `created`, so the
orchestrator restores from `Saga.state` stored in the `orchestrator` DB).

Guards default to False; the orchestrator sets `limits_ok` before the
`enter_reserve` gate (pitfall #1: explicit guards are the contract).
"""
from __future__ import annotations

from statemachine import State as SagaSMState
from statemachine import StateMachine

# Guard flags must be class attributes declared before the transitions that
# reference them (python-statemachine resolves `cond=_name` at class-creation
# time, not at runtime).
_LIMITS_DEFAULT = False


class SagaState(SagaSMState):
    """`State` subclass used as the phase-3 spec sketch names the base class."""


class SagaStateMachine(StateMachine):
    limits_ok: bool = _LIMITS_DEFAULT

    def _limits_ok(self) -> bool:
        return self.limits_ok

    created = SagaState("Created", initial=True)
    checking = SagaState("Checking")
    reserve = SagaState("Reserve")
    fraud = SagaState("Fraud")
    debit = SagaState("Debit")
    success = SagaState("Success", final=True)
    failed = SagaState("Failed", final=True)
    rolledback = SagaState("RolledBack", final=True)

    # --- happy path -----------------------------------------------------
    start_checking = created.to(checking)
    enter_reserve = checking.to(reserve, cond=_limits_ok)   # gate: limits passed
    enter_fraud = reserve.to(fraud)
    enter_debit = fraud.to(debit)
    complete = debit.to(success)

    # --- failure --------------------------------------------------------
    fail = failed.from_(checking, reserve)
    rollback = rolledback.from_(fraud, debit)

    # --- helpers --------------------------------------------------------
    @classmethod
    def restore(cls, state: str, **flags: bool) -> SagaStateMachine:
        """Rebuild from a persisted state name + guard flags (pitfall #2)."""
        value = {
            "Created": "created",
            "Checking": "checking",
            "Reserve": "reserve",
            "Fraud": "fraud",
            "Debit": "debit",
            "Success": "success",
            "Failed": "failed",
            "RolledBack": "rolledback",
        }.get(state, "created")
        machine = cls(start_value=value)
        for name, val in flags.items():
            if hasattr(machine, name):
                setattr(machine, name, val)
        return machine

    def reached_terminal(self) -> bool:
        return self.current_state_value in {"success", "failed", "rolledback"}
