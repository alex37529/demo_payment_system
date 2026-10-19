"""Profile read-side repository (DB ``profile``).

Session ownership: the **consumer / use-case** owns the session (one event in,
one TX out). ``apply_event`` does the whole decision in one transaction:

  1. guarded insert into ``processed_events`` (idempotency, pitfall #5);
  2. if the insert won, upsert ``profile.users.balance``;
  3. commit.

So an at-least-once redelivery of the same ``event_id`` is a no-op (the insert
conflicts → no balance change). ``list_all`` / ``get`` are the read paths for
``GET /v1/users``.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import select

from services.profile.infrastructure.models import ProcessedEventRow, UserRow


class ProfileRepo:
    """Async SQLAlchemy access to the ``profile`` read model."""

    def __init__(self, session_factory: Any) -> None:
        self._sf = session_factory

    async def apply_event(
        self, *, event_id: str, account_id: str, new_balance: int,
        event_type: str = "",
    ) -> bool:
        """Apply one ``FundsDebited`` to the read model, idempotently.

        Returns True if the balance was updated, False if this ``event_id`` was
        already handled (replayed redelivery).
        """
        async with self._sf() as session:
            async with session.begin():
                existing = await session.get(ProcessedEventRow, event_id)
                if existing is not None:
                    return False  # already applied — idempotent no-op
                session.add(ProcessedEventRow(
                    event_id=event_id, event_type=event_type, account_id=account_id))
                await session.flush()  # raises IntegrityError on a concurrent dup

                row = await session.get(UserRow, account_id)
                if row is None:
                    # Unknown account (account_id not seeded) — record the event
                    # (so the idempotency guard holds) but there is nothing to
                    # update. Defensive; all happy-path accounts are seeded.
                    return False
                row.balance = new_balance
            return True  # committed

    async def mark_processed(self, event_id: str, *, event_type: str = "",
                             account_id: str = "") -> None:
        """Record an event that was *seen* but did not move the balance (a delta
        event), so a redelivery is recognised as already handled. Best-effort."""
        async with self._sf() as session, session.begin():
            if await session.get(ProcessedEventRow, event_id) is None:
                session.add(ProcessedEventRow(
                    event_id=event_id, event_type=event_type,
                    account_id=account_id))

    async def is_processed(self, event_id: str) -> bool:
        async with self._sf() as session:
            return await session.get(ProcessedEventRow, event_id) is not None

    async def get(self, account_id: str) -> dict | None:
        async with self._sf() as session:
            row = await session.get(UserRow, account_id)
            if row is None:
                return None
            return self._to_dict(row)

    async def list_all(self) -> list[dict]:
        async with self._sf() as session:
            rows = (await session.execute(
                select(UserRow).order_by(UserRow.id))).scalars().all()
            return [self._to_dict(r) for r in rows]

    @staticmethod
    def _to_dict(row: UserRow) -> dict:
        return {
            "id": row.id, "name": row.name, "email": row.email,
            "currency": row.currency, "balance": row.balance,
        }
