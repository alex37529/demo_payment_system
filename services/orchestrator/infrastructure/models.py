"""Orchestrator ORM rows (DB `orchestrator`, schema `public`).

`sagas` — the saga state. Key invariants:
  * `saga_id`    — PK, unique per coordination run;
  * `payment_id` — UNIQUE: exactly one saga per payment (replay `Create` is a no-op,
                   see `tz_v2 §3.11.1` + phase-3 spec §4);
  * `history` / `payload` / `compensation` — JSON (SQLite + Postgres compatible);
  * `state` mirrors the `SagaStateMachine` state name (top-level), so a restart
    can rebuild the machine with `start_value=self.state` (pitfall #2).
"""
from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import JSON, DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from shared.db.base import Base


class SagaRow(Base):
    __tablename__ = "sagas"

    saga_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    payment_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    state: Mapped[str] = mapped_column(String(32), default="Created", index=True)
    history: Mapped[list] = mapped_column(JSON, default=list)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    compensation: Mapped[list] = mapped_column(JSON, default=list)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )
