"""Canonical Domain Event catalog.

Canonical names (v2 #10). Source of truth: doc/tz_v2.md §3.11.1.
`ReservePosted` / `DebitPosted` (v1 drafts) are REPLACED by FundsReserved / FundsDebited.

Note: fields default to empty so subclasses can omit optional context (saga_id etc.).
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass(frozen=True)
class DomainEvent:
    event_id: uuid.UUID = field(default_factory=uuid.uuid4)
    ts: datetime = field(default_factory=lambda: datetime.now(UTC))
    occurred_by: str = ""

    def to_json(self) -> dict:
        out: dict = {
            # Self-describing discriminator: lets consumers (e.g. the Orchestrator)
            # key off `event_type` in `ledger.events` / `payments.events` without
            # inferring from which fields are present (the event classes share
            # nearly identical field shapes). Backwards-compatible: purely additive.
            "event_type": type(self).__name__,
        }
        for k, v in self.__dict__.items():
            if hasattr(v, "isoformat"):
                out[k] = v.isoformat()
            elif isinstance(v, uuid.UUID):
                out[k] = str(v)
            else:
                out[k] = v
        return out


# === payments (produced: Intake, Orchestrator) ===
@dataclass(frozen=True)
class PaymentCreated(DomainEvent):
    payment_id: str = ""
    amount: dict = field(default_factory=dict)
    account_id: str = ""
    idem_key: str = ""


@dataclass(frozen=True)
class LimitsChecked(DomainEvent):
    payment_id: str = ""
    saga_id: str = ""


@dataclass(frozen=True)
class FraudVerdict(DomainEvent):
    payment_id: str = ""
    saga_id: str = ""
    decision: str = ""  # "approve" | "decline" | "route_to_dlq"
    score: float = 0.0


@dataclass(frozen=True)
class DebitFailed(DomainEvent):
    payment_id: str = ""
    saga_id: str = ""
    step: str = ""
    reason: str = ""


# === ledger (produced: Ledger — outbox -> Debezium -> ledger.events) ===
@dataclass(frozen=True)
class FundsReserved(DomainEvent):
    """Delta-only: reservation, NOT the final balance (see phase 05)."""

    account_id: str = ""
    payment_id: str = ""
    saga_id: str = ""
    amount: dict = field(default_factory=dict)


@dataclass(frozen=True)
class FundsDebited(DomainEvent):
    """Final: funds debited, new_balance is the current balance."""

    account_id: str = ""
    payment_id: str = ""
    saga_id: str = ""
    amount: dict = field(default_factory=dict)
    new_balance: dict = field(default_factory=dict)


@dataclass(frozen=True)
class CancelReservationPosted(DomainEvent):
    account_id: str = ""
    payment_id: str = ""
    saga_id: str = ""
    amount: dict = field(default_factory=dict)
    reason: str = ""


@dataclass(frozen=True)
class RefundPosted(DomainEvent):
    account_id: str = ""
    payment_id: str = ""
    saga_id: str = ""
    amount: dict = field(default_factory=dict)
    reason: str = ""


# === saga (produced: Orchestrator) ===
@dataclass(frozen=True)
class SagaFailed(DomainEvent):
    saga_id: str = ""
    payment_id: str = ""
    failed_step: str = ""
    reason: str = ""


@dataclass(frozen=True)
class SagaSucceeded(DomainEvent):
    saga_id: str = ""
    payment_id: str = ""


__all__ = [
    "CancelReservationPosted",
    "DebitFailed",
    "DomainEvent",
    "FraudVerdict",
    "FundsDebited",
    "FundsReserved",
    "LimitsChecked",
    "PaymentCreated",
    "RefundPosted",
    "SagaFailed",
    "SagaSucceeded",
]
