"""Orchestrator async repository — persists the Saga aggregate (DB `orchestrator`).

Session ownership: the repository is built with a `session_factory` and opens a
*short* session per call. There is no multi-step TX here — each saga state
change is small and idempotent per `payment_id` (the UNIQUE index + the
ledger-side `applied_commands` guard together guarantee no double-spend).
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from services.orchestrator.domain import Saga
from services.orchestrator.infrastructure.models import SagaRow


class SagaRepository:
    def __init__(self, session_factory: Any) -> None:
        self.session_factory = session_factory

    async def get(self, saga_id: str) -> SagaRow | None:
        async with self.session_factory() as s:
            return await s.get(SagaRow, saga_id)

    async def get_by_payment(self, payment_id: str) -> SagaRow | None:
        async with self.session_factory() as s:
            r = await s.execute(select(SagaRow).where(SagaRow.payment_id == payment_id))
            return r.scalars().first()

    async def save(self, saga: Saga) -> None:
        """Insert-or-update the saga (idempotent: same `payment_id` re-writes state)."""
        async with self.session_factory() as s:
            row = await s.get(SagaRow, saga.saga_id)
            if row is None:
                s.add(SagaRow(**saga.to_row()))
            else:
                for key, value in saga.to_row().items():
                    setattr(row, key, value)
            await s.commit()

    async def create_unique(self, saga: Saga) -> bool:
        """Create the saga if no saga exists for this `payment_id`.

        Returns ``True`` when a brand-new row was inserted (caller proceeds with
        the saga), ``False`` when one already exists (replay of the same
        `payment_id` -> do NOT re-run the steps). The UNIQUE index on
        `payment_id` is the authoritative guard (pitfall-safe against races).
        """
        async with self.session_factory() as s:
            existing = (await s.execute(
                select(SagaRow).where(SagaRow.payment_id == saga.payment_id))).scalars().first()
            if existing is not None:
                await s.rollback()
                return False
            s.add(SagaRow(**saga.to_row()))
            try:
                await s.commit()
            except IntegrityError:
                await s.rollback()
                return False
            return True
