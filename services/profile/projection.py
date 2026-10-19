"""Balance projection (pure) — decide whether a ``ledger.events`` event moves
the Read-Side balance, and if so, what value it should become (spec §3.1/§3.2).

CQRS read-side keeps the **final** balance (TЗ §3.11.1). Only events that carry
``new_balance`` settle it:

  * ``FundsDebited`` — ``new_balance`` IS the current balance after the debit.
    We copy it: ``profile.users.balance := new_balance``. (Final.)
  * ``FundsReserved`` / ``CancelReservationPosted`` — **delta** events: money
    moved into/out of the ``reserved`` hold, not the final balance (see the
    docstring on ``shared.domain.events.FundsReserved``). Per spec §3.1 the
    consumer does "if new_balance is None -> continue", so we skip them.
  * ``RefundPosted`` — money returned, no ``new_balance`` on the canonical
    event (delta). Skipped by the same rule (out of the PoC happy path).

The decision is a pure function so it is unit-testable against the exact
Debezium record shape (no DB, no Kafka, no FastAPI).
"""
from __future__ import annotations

from typing import Any

#: canonical events whose ``new_balance`` is the current balance (final state)
_FINAL_EVENTS = frozenset({"FundsDebited"})


def resolve_new_balance(event: dict[str, Any]) -> int | None:
    """Return the new ``profile.users.balance`` (minor units) for ``event``.

    Returns ``None`` when this event must NOT touch the balance: a delta event
    (FundsReserved / CancelReservationPosted / RefundPosted), an unrecognised
    event, or an event with no ``account_id`` / no ``new_balance`` amount.
    """
    if not event.get("account_id"):
        return None
    if event.get("event_type") not in _FINAL_EVENTS:
        return None

    nb = event.get("new_balance")
    if isinstance(nb, dict):
        amount = nb.get("value", nb.get("amount"))
        return int(amount) if amount is not None else None
    if isinstance(nb, (int, float)) and not isinstance(nb, bool):  # shorthand
        return int(nb)
    return None
