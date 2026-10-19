"""Ledger domain: Money (VO), Account + Payment (aggregates), domain errors.

Pure domain — no FastAPI, no SQLAlchemy. Invariants live on the aggregate and
can only be violated through a raised error:

  1. balance >= 0                     (no negative / 'debt' in the PoC)
  2. reserve: available(balance - reserved) >= amount
  3. debit:   available(balance - reserved) >= amount
  4. cancel_reservation: reserved >= amount
  5. own-currency only: op currency == account currency  (§3.10 v2, ADR-003)
  6. mutation ONLY through aggregate methods (never ``account.balance = ...``)

Amounts are integers in minor currency units (USD/EUR: cents) — never float.

Money dict shape is the canonical event-contract shape: ``{"value", "currency"}``
(tz_v2 §3.11.1).
"""
from __future__ import annotations

from dataclasses import dataclass

from shared.domain import AggregateRoot, ValueObject
from shared.domain.events import (
    CancelReservationPosted,
    FundsDebited,
    FundsReserved,
    RefundPosted,
)

SUPPORTED_CURRENCIES = ("USD", "EUR")

_VALID_TRANSITIONS = {
    "Created": ("Validating",),
    "Validating": ("Processing",),
    "Processing": ("Success", "Failed", "RolledBack"),
}


class LedgerError(Exception):
    """Base for ledger domain errors (maps to a 4xx at the API boundary)."""


class InsufficientFundsError(LedgerError):
    """Insufficient balance, over-reservation, or currency mismatch (ADR-003)."""


class PaymentNotFound(LedgerError):
    """No such payment in the ledger context."""


class AccountNotFound(LedgerError):
    """No such account in the ledger context."""


class IllegalTransitionError(LedgerError):
    """Payment state transition not allowed (see ``_VALID_TRANSITIONS``)."""


@dataclass(frozen=True)
class Money(ValueObject):
    """Money = integer minor units + ISO-4217 currency. Equality by value."""

    amount: int = 0
    currency: str = "USD"

    def __post_init__(self) -> None:
        if not isinstance(self.amount, int) or isinstance(self.amount, bool):
            raise TypeError(f"amount must be an int (minor units), got {self.amount!r}")
        if self.amount < 0:
            raise ValueError(f"amount must be >= 0, got {self.amount}")
        if self.currency not in SUPPORTED_CURRENCIES:
            raise ValueError(f"unsupported currency {self.currency!r}")

    def to_dict(self) -> dict:
        return {"value": self.amount, "currency": self.currency}

    @classmethod
    def from_dict(cls, d: dict | None) -> Money | None:
        if d is None:
            return None
        if isinstance(d, int):  # shorthand: bare minor units, assume USD
            return cls(int(d), "USD")
        value = d.get("value", d.get("amount", 0))  # accept canonical or shorthand key
        return cls(int(value), d.get("currency", "USD"))


class Account(AggregateRoot):
    """Account aggregate root (CQRS write-side).

    ``balance``  = funds owned.
    ``reserved`` = funds currently held (set aside) for in-flight reservations.
    ``available``= balance - reserved — the money a new reserve/debit may use.
    """

    def __init__(self, account_id: str, *, owner_name: str = "",
                 currency: str, balance: Money) -> None:
        super().__init__()
        if balance.currency != currency:
            raise InsufficientFundsError(f"balance currency {balance.currency} != account {currency}")
        self.account_id = account_id
        self.owner_name = owner_name
        self.currency = currency
        self.balance = balance
        self.reserved = Money(0, currency)
        self._version = 1
        self._applied: dict[str, str] = {}  # ref -> op (idempotency; replay = no-op)

    # --- hydration / persistence (infrastructure <-> domain mapping) ---
    @classmethod
    def load(cls, row) -> Account:
        acct = cls(
            str(row.id),
            owner_name=row.owner_name or "",
            currency=row.currency,
            balance=Money(row.balance, row.currency),
        )
        acct.reserved = Money(getattr(row, "reserved", 0) or 0, row.currency)
        return acct

    def to_row(self) -> dict:
        return {
            "id": self.account_id,
            "owner_name": self.owner_name,
            "currency": self.currency,
            "balance": self.balance.amount,
            "reserved": self.reserved.amount,
            "version": self._version,
        }

    @property
    def available(self) -> Money:
        return Money(self.balance.amount - self.reserved.amount, self.currency)

    def _bump(self) -> None:
        self._version += 1

    def _already_applied(self, ref: str, op: str) -> bool:
        """Idempotency: True if this (ref, op) pair was already applied (replay = no-op)."""
        return bool(ref) and self._applied.get(ref) == op

    def _record(self, ref: str, op: str) -> None:
        if ref:
            self._applied[ref] = op

    # --- commands (the only way to change state) ---
    def debit(self, amount: Money, *, ref: str = "") -> FundsDebited:
        """Move funds out. Guard: ``available`` (balance - reserved) >= amount."""
        if self._already_applied(ref, "debit"):
            return FundsDebited(account_id=self.account_id, payment_id=ref,
                                amount=amount.to_dict(), new_balance=self.balance.to_dict())
        if amount.currency != self.currency:
            raise InsufficientFundsError(f"currency mismatch: {amount.currency} != {self.currency}")
        if self.available.amount < amount.amount:
            raise InsufficientFundsError(
                f"debit {amount.amount} exceeds available {self.available.amount} (reserved {self.reserved.amount})")
        new_balance = Money(self.balance.amount - amount.amount, self.currency)
        self.balance = new_balance
        self._bump()
        event = FundsDebited(account_id=self.account_id, payment_id=ref,
                             amount=amount.to_dict(), new_balance=new_balance.to_dict())
        self._record(ref, "debit")
        self.add_event(event)
        return event

    def reserve(self, amount: Money, *, ref: str = "") -> FundsReserved:
        if self._already_applied(ref, "reserve"):
            return FundsReserved(account_id=self.account_id, payment_id=ref, amount=amount.to_dict())
        if amount.currency != self.currency:
            raise InsufficientFundsError(f"currency mismatch: {amount.currency} != {self.currency}")
        if self.available.amount < amount.amount:
            raise InsufficientFundsError(
                f"reserve {amount.amount} exceeds available {self.available.amount} (reserved {self.reserved.amount})")
        self.reserved = Money(self.reserved.amount + amount.amount, self.currency)
        self._bump()
        event = FundsReserved(account_id=self.account_id, payment_id=ref, amount=amount.to_dict())
        self._record(ref, "reserve")
        self.add_event(event)
        return event

    def cancel_reservation(self, amount: Money, *, ref: str = "") -> CancelReservationPosted:
        if self._already_applied(ref, "cancel_reservation"):
            return CancelReservationPosted(account_id=self.account_id, payment_id=ref, amount=amount.to_dict())
        if amount.currency != self.currency:
            raise InsufficientFundsError(f"currency mismatch: {amount.currency} != {self.currency}")
        if amount.amount > self.reserved.amount:
            raise InsufficientFundsError(f"cancel-reservation {amount.amount} > reserved {self.reserved.amount}")
        self.reserved = Money(self.reserved.amount - amount.amount, self.currency)
        self._bump()
        event = CancelReservationPosted(account_id=self.account_id, payment_id=ref, amount=amount.to_dict())
        self._record(ref, "cancel_reservation")
        self.add_event(event)
        return event

    def refund(self, amount: Money, *, ref: str = "") -> RefundPosted:
        if self._already_applied(ref, "refund"):
            return RefundPosted(account_id=self.account_id, payment_id=ref, amount=amount.to_dict())
        if amount.currency != self.currency:
            raise InsufficientFundsError(f"currency mismatch: {amount.currency} != {self.currency}")
        new_balance = Money(self.balance.amount + amount.amount, self.currency)
        self.balance = new_balance
        self._bump()
        event = RefundPosted(account_id=self.account_id, payment_id=ref, amount=amount.to_dict())
        self._record(ref, "refund")
        self.add_event(event)
        return event


class Payment(AggregateRoot):
    """Payment aggregate: id + amount + state (projection of the state machine)."""

    def __init__(self, payment_id: str, amount: Money, account_id: str,
                 idem_key: str = "", saga_id: str = "") -> None:
        super().__init__()
        self.payment_id = payment_id
        self.amount = amount
        self.account_id = account_id
        self.idem_key = idem_key
        self.saga_id = saga_id
        self.state = "Created"

    @classmethod
    def load(cls, row) -> Payment:
        return cls(
            str(row.id), Money(row.amount, row.currency), row.account_id,
            idem_key=row.idem_key or "", saga_id=row.saga_id or "",
        )

    def to_row(self) -> dict:
        return {
            "id": self.payment_id, "amount": self.amount.amount, "currency": self.amount.currency,
            "account_id": self.account_id, "idem_key": self.idem_key,
            "saga_id": self.saga_id, "state": self.state,
        }

    def transition(self, new_state: str) -> None:
        if new_state not in _VALID_TRANSITIONS.get(self.state, ()):
            raise IllegalTransitionError(f"{self.state} -> {new_state} not allowed")
        self.state = new_state
