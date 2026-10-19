"""Saga aggregate + domain errors (orchestrator bounded context).

Pure domain — no FastAPI / SQLAlchemy / httpx. The aggregate owns:
  * identity     — `saga_id` (+ the unique `payment_id` it coordinates),
  * state        — the saga state name (mirrors the FSM),
  * history      — append-only audit trail of every step (persisted),
  * payload      — the original command payload (account/amount/…),
  * compensation — the reverse commands owed after a failure (the "debt").

State *transitions* are owned by `SagaStateMachine` (single authority); this
aggregate records what happened and what is owed to undo it.

`Money` is imported from the ledger domain so the own-currency invariant
(ADR-003) has exactly one definition across the two contexts.
"""
from __future__ import annotations

from datetime import UTC, datetime

from services.ledger.domain import Money
from shared.domain import AggregateRoot


# -------------------------------------------------------------------------- #
# errors
# -------------------------------------------------------------------------- #
class SagaError(Exception):
    """Base for orchestrator domain errors."""


class SagaNotFound(SagaError):
    """No such saga / payment in the orchestrator context."""


class UnsupportedCommand(SagaError):
    """A `payments.commands` message the dispatcher does not handle."""


# -------------------------------------------------------------------------- #
# amount shaping
# -------------------------------------------------------------------------- #
def normalize_amount(amount: object, currency: str = "") -> dict:
    """Coerce a command's amount to the canonical ``{"value","currency"}`` shape.

    Accepts the ledger `Money` dict form, a bare int (assumed USD), or a `Money`
    value object. Raises `ValueError` when the amount is missing / invalid, so a
    malformed command fails the saga early (handler routes it to the DLQ).
    """
    if isinstance(amount, Money):
        return amount.to_dict()
    if isinstance(amount, bool):
        raise TypeError(f"amount must be a number, not a boolean: {amount!r}")
    if isinstance(amount, int):
        return Money(int(amount), (currency or "USD").upper()).to_dict()
    if isinstance(amount, dict):
        d = dict(amount)
        d.setdefault("value", d.get("amount", 0))          # canonical or shorthand
        d.setdefault("currency", currency or "USD")
        value = int(d["value"])
        cur = str(d["currency"]).upper()
        return Money(value, cur).to_dict()                 # revalidates (own-currency)
    raise ValueError(f"cannot normalize amount: {amount!r}")


# -------------------------------------------------------------------------- #
# aggregate
# -------------------------------------------------------------------------- #
#: canonical step order (used for history ordering + compensation reasoning)
STEP_ORDER = ("check", "reserve", "fraud", "debit")


class Saga(AggregateRoot):
    def __init__(self, *, saga_id: str, payment_id: str, state: str = "Created",
                 history: list[dict] | None = None, payload: dict | None = None,
                 compensation: list[str] | None = None) -> None:
        super().__init__()
        self.saga_id = saga_id
        self.payment_id = payment_id
        self.state = state
        self.history = list(history or [])
        self.payload = dict(payload or {})
        self.compensation = list(compensation or [])
        self.created_at = datetime.now(UTC)

    # -- hydration --------------------------------------------------------
    @classmethod
    def load(cls, row) -> Saga:
        return cls(
            saga_id=str(row.saga_id),
            payment_id=row.payment_id,
            state=row.state or "Created",
            history=list(row.history or []),
            payload=dict(row.payload or {}),
            compensation=list(row.compensation or []),
        )

    def to_row(self) -> dict:
        return {
            "saga_id": self.saga_id,
            "payment_id": self.payment_id,
            "state": self.state,
            "history": self.history,
            "payload": self.payload,
            "compensation": self.compensation,
        }

    @staticmethod
    def _now() -> str:
        return datetime.now(UTC).isoformat()

    # -- behaviour --------------------------------------------------------
    def mark_step(self, step: str, *, cmd: str = "", event: str = "", ok: bool,
                  err: str = "") -> dict:
        """Append one step to the audit trail. Does NOT change `self.state`
        (the FSM owns transitions)."""
        record: dict = {"step": step, "cmd": cmd, "event": event,
                        "ts": type(self)._now(), "ok": ok, "err": err}
        self.history.append(record)
        return record

    # -- derived (from persisted history — survives restart) -------------
    def has_confirmed_reservation(self) -> bool:
        """True once the ledger has **confirmed** a reservation (a `FundsReserved`
        event was recorded). This is the durable, authoritative signal (persisted
        in `history`), so the compensation decision survives an orchestrator
        restart — pitfall #3 (state must be in the DB, not in-memory)."""
        return any(
            h.get("step") == "reserve" and h.get("ok") is True
            and h.get("event") in ("FundsReserved",)
            for h in self.history
        )

    def compensation_commands(self, failed_step: str) -> list[str]:
        """Reverse commands owed after `failed_step` failed.

        Empty if no reservation was confirmed yet (failure at `check` / before
        the reservation was booked). The step->command map lives in
        `domain/compensation.py: COMPENSATIONS`.
        """
        if not self.has_confirmed_reservation():
            return []
        from services.orchestrator.domain.compensation import compensation_for

        return list(compensation_for(failed_step))

    # -- derived context --------------------------------------------------
    @property
    def account_id(self) -> str:
        return str(self.payload.get("account_id", ""))

    @property
    def amount(self) -> dict:
        return self.payload.get("amount", {"value": 0, "currency": "USD"})
