"""Ledger SQLAlchemy async repository — concrete for ``IAccountRepository``.

Session ownership (pitfall #5, phase-2 spec §5): the **use-case** owns the
session. Every mutation method here takes the use-case's already-open session,
so one use-case == one async transaction. ``get()`` is a read path used by the
API/UI; it opens its own read-only session.
"""
from __future__ import annotations

from typing import Any

from services.ledger.infrastructure.models import AccountRow, AppliedCommandRow
from shared.db.repository import IAccountRepository


class SQLAlchemyAccountRepository(IAccountRepository):
    """Async SQLAlchemy implementation of the account repository interface."""

    # ---- interface conformance (session is injected by the use-case) ----
    async def get(self, account_id: str, *, session: Any = None) -> AccountRow | None:
        return await session.get(AccountRow, account_id)

    async def save(self, account: Any, *, session: Any) -> None:
        """Apply the aggregate's state to the accounts row (no commit here)."""
        row = await session.get(AccountRow, account.account_id)
        if row is None:
            row = AccountRow(id=account.account_id, owner_name=account.owner_name,
                             currency=account.currency)
            session.add(row)
        for key, value in account.to_row().items():
            setattr(row, key, value)

    # ---- idempotency guard (the crux of phase-2 §12) ----
    async def mark_applied(self, *, idem_key: str, op: str, account_id: str,
                           payment_id: str, saga_id: str, session: Any) -> bool:
        """Atomically record that ``(account, op, ref)`` has been applied.

        Returns False and lets the caller roll back when this exact ref was
        already applied in a previous (committed) transaction — at-least-once
        Kafka redelivery or a manual replay. ``IntegrityError`` (UNIQUE pk) =>
        already applied.
        """
        already = await session.get(AppliedCommandRow, idem_key)
        if already is not None:
            return False
        session.add(AppliedCommandRow(
            idem_key=idem_key, op=op, account_id=account_id,
            payment_id=payment_id, saga_id=saga_id,
        ))
        await session.flush()  # raises IntegrityError on UNIQUE conflict, pre-commit
        return True

    # ---- the unused interface methods (aggregate is the single writer) ----
    async def debit(self, account_id: str, amount: int, currency: str) -> Any:
        raise NotImplementedError("use the Account aggregate's .debit() method")

    async def reserve(self, account_id: str, amount: int, currency: str) -> Any:
        raise NotImplementedError("use the Account aggregate's .reserve() method")

    async def cancel_reservation(self, account_id: str, ref: str) -> None:
        raise NotImplementedError("use the Account aggregate's .cancel_reservation() method")

    async def refund(self, ref: str, amount: int) -> None:
        raise NotImplementedError("use the Account aggregate's .refund() method")
