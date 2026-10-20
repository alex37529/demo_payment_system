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

from sqlalchemy import JSON, BigInteger, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from shared.db.base import Base


class AccountRow(Base):
    """One row per account — `accounts` table in the `ledger` database."""

    __tablename__ = "accounts"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    owner_name: Mapped[str] = mapped_column(String(255), default="")
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    balance: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    #: Derived projection of `holds` (the sum) — kept as a column so the
    #: "how much is held" question is a plain indexed read, and so the
    #: existing queries/alert rules keep working.
    reserved: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    #: payment_id -> held amount (minor units). The per-payment holds are
    #: the SOURCE OF TRUTH for `reserved`: a debit consumes the hold of its
    #: own payment (ADR-010). A JSONB map keeps the hold attached to the
    #: account row it belongs to (loaded/saved in the same transaction);
    #: the alternative — a separate `account_holds` table — costs an extra
    #: query + insert/delete per reserve/cancel for the same information.
    holds: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
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


# NOTE: a `payments` table used to live here as a Phase-2 "thin mirror" of the
# payment being acted upon. Phase 3 moved ownership of the full payment
# lifecycle to the orchestrator (`orchestrator.sagas` + `ledger.outbox_events`).
# This class + its DDL were dead code (never INSERTed, never SELECTed, 0 rows
# in production) and were removed on 2026-10-19.


class TransactionRow(Base):
    """Append-only per-account balance journal (write-side, DB `ledger`).

    One row per balance-moving op (debit / refund), written in the SAME
    transaction as the `accounts` mutation, the idempotency guard and the
    outbox event — so a commit atomically reflects both the balance AND its
    audit row, and a rolled-back / replayed command leaves *zero* rows.

    Shape is a classic running-balance journal:
      * `amount`       — magnitude of the op (always >= 0, minor units);
      * `event_type`   — direction: `FundsDebited` (= money left) vs
                         `RefundPosted` (= money came back);
      * `balance_after`— the account balance immediately AFTER the op. The
                         last row's `balance_after` == current `accounts.balance`,
                         so the whole history can be audited in one query.
    `payment_id` + `event_id` tie the row back to the saga / canonical event.
    """

    __tablename__ = "transactions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    event_id: Mapped[str] = mapped_column(String(36), unique=True, index=True)
    event_type: Mapped[str] = mapped_column(String(40), nullable=False)  # FundsDebited / RefundPosted
    amount: Mapped[int] = mapped_column(BigInteger, nullable=False)       # magnitude, >= 0
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    balance_after: Mapped[int] = mapped_column(BigInteger, nullable=False)
    payment_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC))
