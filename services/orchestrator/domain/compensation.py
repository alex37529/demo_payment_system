"""Compensation map for the saga (§ phase-3 spec §3.2, order rules §3.2 pitfall #7/#8).

`Compensations` are the *reverse* commands that undo the work a step did, and
they run in the REVERSE order of the steps that actually succeeded. Which ones
are needed depends on how far the saga advanced before it failed:

    failed at            already booked            reverse (newest -> oldest)
    check   (limits)     (nothing)                 —                       -> Failed
    reserve  (booking)   (nothing confirmed yet)   —                       -> Failed
    fraud    (after Reserve confirmed)             CancelReservation       -> RolledBack
    debit    (after Reserve confirmed)             CancelReservation       -> RolledBack

Notes (phase-3 spec §3.2 pitfall #8):
* `Refund` (putting money back onto the balance) would only be meaningful if a
  `Debit` actually settled. In this orchestration the saga dies *before* a
  `Debit` succeeds (the `FundsDebited` event is the success trigger), so a single
  `CancelReservation` is sufficient and correct. A settled-debit refund path is
  an explicit out-of-scope follow-up, not silently omitted.
* The compensation list is *derived* on failure (not stored as a running
  to-do), keyed by the step that failed — that keeps "what we owe to undo" in
  sync with "how far we advanced" without a stale plan to prune.
"""
from __future__ import annotations

#: command sequences to undo, newest (most recently booked) first.
COMPENSATIONS: dict[str, list[str]] = {
    "check": [],                          # limits: nothing was booked
    "reserve": [],                        # booking not confirmed yet: nothing to undo
    "fraud": ["CancelReservation"],       # reservation confirmed: release it
    "debit": ["CancelReservation"],       # reservation confirmed: release it
}


def compensation_for(step: str) -> list[str]:
    """Reverse commands required after `step` failed (empty if nothing was booked).

    Unknown steps are treated defensively as "release the reservation if one
    exists" so a typo can never *skip* a required compensation.
    """
    return list(COMPENSATIONS.get(step, COMPENSATIONS["debit"]))
