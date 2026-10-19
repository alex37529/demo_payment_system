"""Intake infrastructure — PG persistence for the idempotency fallback copy.

`IdempotencyRow` (table `intake.idempotency_keys`) is the *persistent* mirror
of the Redis `idem:{key}` entry. It exists so that, if Redis is restarted or
down, a duplicate request is still detected (phase-4 spec §3.3 "dual-write").
The PG row is the source of truth for the cached response; Redis is the fast
path.
"""
from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import JSON, DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from shared.db.base import Base


class IdempotencyRow(Base):
    __tablename__ = "idempotency_keys"

    key: Mapped[str] = mapped_column(String(191), primary_key=True, index=True)
    payment_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    response: Mapped[dict | None] = mapped_column(JSON, nullable=True, default=None)
    ttl_until: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), index=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )
