"""Profile read-model rows (DB ``profile``).

The Read-Side is a **denormalized copy** of the write-side balance (pitfall #4,
phase-5 spec §3.2): one row per user = ``id, name, email, currency, balance``.
Profile **never** JOINs the ``ledger`` schema — a separate bounded context (and,
in prod, a separate database). It keeps its own balance up to date by consuming
``ledger.events``.

Balance is ``BigInteger`` (minor units). The ``email``/``name``/``currency``
columns are read-model conveniences (derived from ``ledger.accounts``).

``processed_events`` is the **idempotency guard** (pitfall #5): Kafka delivers
at-least-once, so the same ``event_id`` can arrive twice. Inserting one row per
``event_id`` (``UNIQUE``) and checking BEFORE applying makes a replay a no-op.
"""
from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import BigInteger, DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from shared.db.base import Base


class UserRow(Base):
    """One row per user — the denormalized read model.

    Lives in the ``profile`` **database**'s public schema (the DB boundary IS
    the bounded context, TЗ §2.2) — exactly like ``ledger.accounts`` lives in
    its own DB. No ``schema=`` arg: that would force a Postgres *schema* named
    ``profile`` and break the shared-``Base`` + SQLite test engines that rely
    on the public schema.
    """

    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), default="")
    email: Mapped[str] = mapped_column(String(255), default="")
    currency: Mapped[str] = mapped_column(String(8), nullable=False, default="USD")
    balance: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )


class ProcessedEventRow(Base):
    """Idempotency guard — one row per handled ``ledger.events`` ``event_id``.

    The consumer attempts the INSERT (guarded) and only applies the balance
    change when the insert wins; the UNIQUE pk makes an at-least-once
    redelivery of the same event a no-op (pitfall #5).
    """

    __tablename__ = "processed_events"

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(64), default="")
    account_id: Mapped[str] = mapped_column(String(64), default="")
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC))
