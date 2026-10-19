"""Transactional Outbox (§3.2 TЗ v2).

The event row is written in the SAME transaction as the data change. Debezium
reads the Postgres WAL and publishes the insert to Kafka. This removes the
Dual-Write problem: you cannot "commit the balance but lose the event".

Pitfall #14: append the outbox row BEFORE commit, and commit once — never
publish after commit (an event could be lost on a crash in between).
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import BigInteger, DateTime, JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from shared.db.base import Base


class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(36), unique=True, index=True)
    topic: Mapped[str] = mapped_column(String(128), index=True)  # ledger.events / payments.events
    payload: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True
    )

    def __init__(self, event: Any, topic: str) -> None:
        self.event_id = str(getattr(event, "event_id", uuid.uuid4()))
        self.topic = topic
        self.payload = event.to_json() if hasattr(event, "to_json") else dict(event)


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
