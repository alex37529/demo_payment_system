"""Transactional Outbox (§3.2 TЗ v2).

The event row is written in the SAME transaction as the data change. Debezium
reads the Postgres WAL and publishes the insert to Kafka. This removes the
Dual-Write problem: you cannot "commit the balance but lose the event".

Pitfall #14: append the outbox row BEFORE commit, and commit once — never
publish after commit (an event could be lost on a crash in between).
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import JSON, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from shared.db.base import Base

# Outbox row statuses
PENDING = "pending"        # just written, Debezium has not claimed it yet
PUBLISHED = "published"    # Debezium delivered to Kafka (set by the delivery tracker)
FAILED = "failed"          # Debezium could not deliver (kept for post-mortem / DLQ routing)
STATUS_VALUES = (PENDING, PUBLISHED, FAILED)


class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(36), unique=True, index=True)
    topic: Mapped[str] = mapped_column(String(128), index=True)  # ledger.events / payments.events
    payload: Mapped[dict] = mapped_column(JSON)
    # Delivery status (see PENDING / PUBLISHED / FAILED above). Use-case writes
    # PENDING; the Debezium-side tracker (or an ops job) flips to PUBLISHED
    # (or FAILED with `last_error` filled). This lets us query "what's still
    # in the outbox" without scanning Kafka.
    status: Mapped[str] = mapped_column(String(32), default=PENDING, index=True)
    # When Debezium flipped us to PUBLISHED (NULL until then).
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, default=None)
    # Reason (set only on FAILED path).
    last_error: Mapped[str | None] = mapped_column(String(512), nullable=True, default=None)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), index=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )

    def __init__(self, event: Any, topic: str) -> None:
        self.event_id = str(getattr(event, "event_id", uuid.uuid4()))
        self.topic = topic
        self.payload = event.to_json() if hasattr(event, "to_json") else dict(event)
        self.status = PENDING
        self.published_at = None
        self.last_error = None


class OutboxMixin:
    """Use-case mixin: append an event to the outbox in the current session/TX.

    Usage (phase 2 debit use-case):
        account.debit(Money(...))          # invariant inside the aggregate
        self._publish_outbox(session, "ledger.events", FundsDebited(...))
        await session.commit()             # ONE TX: balance change + outbox row
    """

    def _publish_outbox(self, session: Any, topic: str, event: Any) -> OutboxEvent:
        row = OutboxEvent(event=event, topic=topic)
        session.add(row)  # commit happens later, in the use-case
        return row
