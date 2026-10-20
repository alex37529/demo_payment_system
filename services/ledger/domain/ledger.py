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

    ``balance``   = funds owned.
    ``reserved`` = funds currently HELD for in-flight reservations
                   (= the sum of the per-payment holds).
    ``available``= balance - reserved — the money a new reserve/debit may use.

    **Holds are tracked PER PAYMENT** (``holds: {payment_id: amount}``), not
    as one anonymous number. That is what makes the debit able to consume its
    own hold: the saga reserves X for payment P and then debits X for P, and
    the money that is being spent is exactly the money that was held for it
    — counting it against the spendable balance would make the payment
    look like it costs 2X (ADR-010).
    """

    def __init__(self, account_id: str, *, owner_name: str = "",
                 currency: str, balance: Money,
                 holds: dict[str, int] | None = None) -> None:
        super().__init__()
        if balance.currency != currency:
            raise InsufficientFundsError(f"balance currency {balance.currency} != account {currency}")
        self.account_id = account_id
        self.owner_name = owner_name
        self.currency = currency
        self.balance = balance
        #: payment_id -> held amount (minor units). The single source of
        #: truth for `reserved`; the scalar column is a derived projection.
        self._holds: dict[str, int] = {str(k): int(v) for k, v in (holds or {}).items()}
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
            holds=dict(getattr(row, "holds", None) or {}),
        )
        return acct

    def to_row(self) -> dict:
        return {
            "id": self.account_id,
            "owner_name": self.owner_name,
            "currency": self.currency,
            "balance": self.balance.amount,
            "reserved": self.reserved.amount,
            "holds": dict(self._holds),
            "version": self._version,
        }

    @property
    def reserved(self) -> Money:
        """The total held (sum of the per-payment holds)."""
        return Money(sum(self._holds.values()), self.currency)

    @property
    def available(self) -> Money:
        return Money(self.balance.amount - self.reserved.amount, self.currency)

    def hold_for(self, payment_id: str) -> int:
        """The amount currently held for one payment."""
        return self._holds.get(str(payment_id), 0)

    def holds_items(self) -> list[tuple[str, int]]:
        """The (payment_id, amount) holds — for the API/ops view."""
        return sorted(self._holds.items())

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
        """Move funds out.

        Guard: the spendable amount is ``balance - (reserved - own_hold)`` —
        the hold THIS payment created is money it is allowed to spend (it
        was reserved precisely for this debit), so it must not count
        against the balance. Every OTHER payment's hold still does.

        On success the hold is CONSUMED: the money that was held left the
        account, so nothing is held for this payment any more (ADR-010 —
        before this, the hold was never released and `available` shrank by
        every payment until the account refused payments it could afford).
        """
        if self._already_applied(ref, "debit"):
            return FundsDebited(account_id=self.account_id, payment_id=ref,
                                amount=amount.to_dict(), new_balance=self.balance.to_dict())
        if amount.currency != self.currency:
            raise InsufficientFundsError(f"currency mismatch: {amount.currency} != {self.currency}")
        own_hold = self.hold_for(ref)
        spendable = self.balance.amount - (self.reserved.amount - own_hold)
        if spendable < amount.amount:
            raise InsufficientFundsError(
                f"debit {amount.amount} exceeds spendable {spendable} "
                f"(balance {self.balance.amount}, reserved {self.reserved.amount})")
        new_balance = Money(self.balance.amount - amount.amount, self.currency)
        self.balance = new_balance
        # The hold this payment created is consumed by the debit.
        if own_hold:
            left = own_hold - amount.amount
            if left > 0:
                self._holds[str(ref)] = left
            else:
                self._holds.pop(str(ref), None)
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
        # The hold belongs to THIS payment — the debit that settles it will
        # consume exactly this hold (ADR-010).
        self._holds[str(ref)] = self.hold_for(ref) + amount.amount
        self._bump()
        event = FundsReserved(account_id=self.account_id, payment_id=ref, amount=amount.to_dict())
        self._record(ref, "reserve")
        self.add_event(event)
        return event

    def cancel_reservation(self, amount: Money, *, ref: str = "") -> CancelReservationPosted:
        """Release a hold (the compensation path).

        The hold released is the one of THIS payment; if the payment has no
        hold, the oldest-holds-any are released (a legacy/anonymous
        reservation) so the compensation of a saga whose payment id we
        could not map still frees the money.
        """
        if self._already_applied(ref, "cancel_reservation"):
            return CancelReservationPosted(account_id=self.account_id, payment_id=ref, amount=amount.to_dict())
        if amount.currency != self.currency:
            raise InsufficientFundsError(f"currency mismatch: {amount.currency} != {self.currency}")
        if amount.amount > self.reserved.amount:
            raise InsufficientFundsError(f"cancel-reservation {amount.amount} > reserved {self.reserved.amount}")
        left = amount.amount
        own = self.hold_for(ref)
        if own:
            take = min(own, left)
            self._holds[str(ref)] = own - take
            if self._holds[str(ref)] == 0:
                self._holds.pop(str(ref), None)
            left -= take
        # Anything left over comes from the other holds (newest first).
        for pid in sorted(self._holds, reverse=True):
            if left <= 0:
                break
            take = min(self._holds[pid], left)
            self._holds[pid] -= take
            if self._holds[pid] == 0:
                self._holds.pop(pid, None)
            left -= take
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
