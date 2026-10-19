"""Ledger ORM rows (DB `ledger`, schema `public`).

Two purposes:
1. The aggregate's state is persisted as a row (`accounts`) via SQLAlchemy async.
2. The **idempotency guard** (replayed command must NOT re-apply) lives here as
   `applied_commands.idem_key UNIQUE` — the use-case's transaction attempts the
   INSERT first, and any `IntegrityError` → `ROLLBACK` = "already applied".

Balance is `BigInteger` (minor units, i.e. cents). No `float` anywhere.
"""
from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import BigInteger, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from shared.db.base import Base


class AccountRow(Base):
    """One row per account — `accounts` table in the `ledger` database."""

    __tablename__ = "accounts"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    owner_name: Mapped[str] = mapped_column(String(255), default="")
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    balance: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    reserved: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)  # optimistic lock (PoC: phase 7)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )


class AppliedCommandRow(Base):
    """Idempotency guard — one row per (account, op, business ref) that has been applied.

    The use-case's transaction does `INSERT ...` on this BEFORE touching the
    account row; `UNIQUE(idem_key)` ensures a replay of the same business ref
    (e.g. same `payment_id`) raises `IntegrityError` → whole TX rolls back.
    """

    __tablename__ = "applied_commands"

    idem_key: Mapped[str] = mapped_column(String(191), primary_key=True)  # SHA or "<payment_id>:<op>"
    op: Mapped[str] = mapped_column(String(32), nullable=False)  # debit / reserve / cancel_reservation / refund
    account_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    payment_id: Mapped[str] = mapped_column(String(64), default="")
    saga_id: Mapped[str] = mapped_column(String(64), default="")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC))


class PaymentRow(Base):
    """Payment (ledger-context view). In phase 2 a thin mirror of the payment
    being acted upon; orchestrator owns the full lifecycle in phase 3.
    """

    __tablename__ = "payments"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    account_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    amount: Mapped[int] = mapped_column(BigInteger, nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    idem_key: Mapped[str] = mapped_column(String(191), unique=True, index=True)
    saga_id: Mapped[str] = mapped_column(String(64), default="")
    state: Mapped[str] = mapped_column(String(32), default="Created")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )
