"""Use-case tests: one TX (accounts + applied_commands + outbox_events), idempotency, invariants.

Uses an in-memory async SQLite DB (aiosqlite) so the REAL repository + aggregate +
outbox path runs — no mocks of the TX. Mirrors the prod Postgres behaviour for the
phase-2 exit: debit -> balance-X + outbox row + (Kafka via Debezium in infra).
"""
from __future__ import annotations

from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from services.ledger.application import (
    LedgerUseCaseRunner,
    UseCase_CancelReservation,
    UseCase_Debit,
    UseCase_Refund,
    UseCase_Reserve,
)
from services.ledger.domain import AccountNotFound, InsufficientFundsError, Money
from services.ledger.infrastructure.models import AppliedCommandRow, TransactionRow
from services.ledger.infrastructure.repository import SQLAlchemyAccountRepository
from shared.db.base import Base
from shared.outbox import OutboxEvent


@pytest_asyncio.fixture
async def env():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sf = async_sessionmaker(engine, expire_on_commit=False)
    repo = SQLAlchemyAccountRepository()
    runner = LedgerUseCaseRunner(session_factory=sf, repo=repo)

    # seed one account: A1, USD, 500
    from services.ledger.infrastructure.models import AccountRow
    async with sf() as s:
        s.add(AccountRow(id="A1", owner_name="John", currency="USD", balance=500, reserved=0, version=1))
        await s.commit()

    yield runner, repo, sf
    await engine.dispose()


async def _balance(sf, account_id: str) -> int:
    from services.ledger.infrastructure.models import AccountRow
    async with sf() as s:
        return (await s.get(AccountRow, account_id)).balance


async def _outbox_n(sf) -> int:
    async with sf() as s:
        from sqlalchemy import func, select
        return (await s.execute(select(func.count()).select_from(OutboxEvent))).scalar_one()


async def test_debit_reduces_balance_and_writes_outbox(env):
    runner, repo, sf = env
    res = await UseCase_Debit(session_factory=sf, repo=repo).execute(
        account_id="A1", amount=Money(100, "USD"), payment_id="P1")
    assert res["applied"] is True
    assert res["event_type"] == "FundsDebited"
    assert await _balance(sf, "A1") == 400
    assert await _outbox_n(sf) == 1
    # outbox payload is the canonical FundsDebited shape
    from sqlalchemy import select
    async with sf() as s:
        row = (await s.execute(select(OutboxEvent))).scalars().one()
    assert row.topic == "ledger.events"
    assert row.payload["new_balance"] == {"value": 400, "currency": "USD"}
    assert row.payload["payment_id"] == "P1"


async def test_debit_insufficient_rolls_back_cleanly(env):
    runner, repo, sf = env
    before = await _outbox_n(sf)
    with pytest.raises(InsufficientFundsError):
        await UseCase_Debit(session_factory=sf, repo=repo).execute(
            account_id="A1", amount=Money(600, "USD"), payment_id="P2")
    assert await _balance(sf, "A1") == 500  # unchanged
    assert await _outbox_n(sf) == before   # no outbox row
    async with sf() as s:
        from sqlalchemy import select
        assert (await s.execute(select(AppliedCommandRow))).scalars().all() == []


async def test_debit_unknown_account(env):
    runner, repo, sf = env
    with pytest.raises(AccountNotFound):
        await UseCase_Debit(session_factory=sf, repo=repo).execute(
            account_id="NOPE", amount=Money(1, "USD"), payment_id="P3")


async def test_debit_idempotent_replay(env):
    runner, repo, sf = env
    uc = UseCase_Debit(session_factory=sf, repo=repo)
    first = await uc.execute(account_id="A1", amount=Money(100, "USD"), payment_id="SAME-REF")
    second = await uc.execute(account_id="A1", amount=Money(100, "USD"), payment_id="SAME-REF")
    assert first["applied"] is True
    assert second["applied"] is False  # replay -> no-op
    assert await _balance(sf, "A1") == 400      # NOT 300
    assert await _outbox_n(sf) == 1             # single event


async def test_reserve_then_debit_blocks_overdraw(env):
    runner, repo, sf = env
    await UseCase_Reserve(session_factory=sf, repo=repo).execute(
        account_id="A1", amount=Money(200, "USD"), payment_id="R1")
    assert await _balance(sf, "A1") == 500  # reserve holds, doesn't move balance
    # available = 300; debit 400 must fail
    with pytest.raises(InsufficientFundsError):
        await UseCase_Debit(session_factory=sf, repo=repo).execute(
            account_id="A1", amount=Money(400, "USD"), payment_id="P4")


async def test_refund_increases_balance(env):
    runner, repo, sf = env
    await UseCase_Debit(session_factory=sf, repo=repo).execute(
        account_id="A1", amount=Money(100, "USD"), payment_id="D1")
    assert await _balance(sf, "A1") == 400
    await UseCase_Refund(session_factory=sf, repo=repo).execute(
        account_id="A1", amount=Money(100, "USD"), payment_id="F1")
    assert await _balance(sf, "A1") == 500
    assert await _outbox_n(sf) == 2


async def test_cancel_reservation_frees_reserved(env):
    runner, repo, sf = env
    await UseCase_Reserve(session_factory=sf, repo=repo).execute(
        account_id="A1", amount=Money(200, "USD"), payment_id="R2")
    from services.ledger.infrastructure.models import AccountRow
    async with sf() as s:
        assert (await s.get(AccountRow, "A1")).reserved == 200
    await UseCase_CancelReservation(session_factory=sf, repo=repo).execute(
        account_id="A1", amount=Money(200, "USD"), payment_id="CR1")
    async with sf() as s:
        assert (await s.get(AccountRow, "A1")).reserved == 0


# --------------------------------------------------------------------------- #
# Balance journal (ledger.transactions) — append-only audit, written in the
# SAME TX as the balance mutation.
# --------------------------------------------------------------------------- #
async def _tx_rows(sf, account_id: str) -> list[TransactionRow]:
    from sqlalchemy import select
    async with sf() as s:
        rows = (await s.execute(
            select(TransactionRow).where(TransactionRow.account_id == account_id)
            .order_by(TransactionRow.id))).scalars().all()
    return list(rows)


async def test_debit_writes_journal_row(env):
    runner, repo, sf = env
    await UseCase_Debit(session_factory=sf, repo=repo).execute(
        account_id="A1", amount=Money(100, "USD"), payment_id="P-J1")
    rows = await _tx_rows(sf, "A1")
    assert len(rows) == 1
    r = rows[0]
    assert r.event_type == "FundsDebited"
    assert r.amount == 100          # magnitude
    assert r.balance_after == 400   # 500 - 100
    assert r.currency == "USD"
    assert r.payment_id == "P-J1"
    assert r.event_id  # ties back to the canonical event


async def test_refund_writes_journal_row(env):
    runner, repo, sf = env
    await UseCase_Debit(session_factory=sf, repo=repo).execute(
        account_id="A1", amount=Money(100, "USD"), payment_id="P-J2")
    await UseCase_Refund(session_factory=sf, repo=repo).execute(
        account_id="A1", amount=Money(100, "USD"), payment_id="P-J3")
    rows = await _tx_rows(sf, "A1")
    assert [r.event_type for r in rows] == ["FundsDebited", "RefundPosted"]
    # running balance: 500 -> 400 (debit) -> 500 (refund)
    assert [r.balance_after for r in rows] == [400, 500]


async def test_reserve_is_not_journaled(env):
    runner, repo, sf = env
    await UseCase_Reserve(session_factory=sf, repo=repo).execute(
        account_id="A1", amount=Money(200, "USD"), payment_id="R-J4")
    assert await _tx_rows(sf, "A1") == []   # reservation holds, doesn't move balance


async def test_journal_is_idempotent_no_double_row_on_replay(env):
    runner, repo, sf = env
    uc = UseCase_Debit(session_factory=sf, repo=repo)
    first = await uc.execute(account_id="A1", amount=Money(100, "USD"), payment_id="SAME-J")
    second = await uc.execute(account_id="A1", amount=Money(100, "USD"), payment_id="SAME-J")
    assert first["applied"] is True and second["applied"] is False
    assert len(await _tx_rows(sf, "A1")) == 1   # replay must NOT append a second row
    assert await _balance(sf, "A1") == 400


async def test_failed_debit_rolls_back_journal(env):
    runner, repo, sf = env
    with pytest.raises(InsufficientFundsError):
        await UseCase_Debit(session_factory=sf, repo=repo).execute(
            account_id="A1", amount=Money(600, "USD"), payment_id="J-FAIL")
    assert await _tx_rows(sf, "A1") == []   # no ledger row for a failed op


# --------------------------------------------------------------------------- #
# Pessimistic lock: the write-side use-case MUST load the account row under a
# row lock (Postgres: SELECT ... FOR UPDATE) so concurrent debits/refunds
# serialize and balance_after always equals the committed accounts.balance.
# --------------------------------------------------------------------------- #
class _SessionSpy:
    """Just enough surface for repository.get_for_update: dialect + execute() + close()."""

    def __init__(self, dialect_name: str):
        class _Bind:
            pass

        self._bind = _Bind()
        type(self._bind).dialect = type("Dialect", (), {"name": dialect_name})()
        self.calls = 0
        self.last_stmt = None

    @property
    def bind(self):
        return self._bind

    async def execute(self, stmt):
        self.calls += 1
        self.last_stmt = stmt
        from types import SimpleNamespace
        return SimpleNamespace(scalar_one_or_none=lambda: None)

    async def rollback(self):
        return None

    async def close(self):
        return None


def _compiled(select_stmt, dialect_name: str) -> str:
    from sqlalchemy.dialects import postgresql, sqlite

    dialect = {"postgresql": postgresql.dialect, "sqlite": sqlite.dialect}[dialect_name]()
    return str(select_stmt.compile(dialect=dialect))


class _SpyRepo(SQLAlchemyAccountRepository):
    """Delegates to the real repo but records whether the use-case used the lock path."""

    def __init__(self) -> None:
        self.for_update_calls = 0

    async def get_for_update(self, account_id: str, *, session: Any) -> Any:
        self.for_update_calls += 1
        return await super().get_for_update(account_id, session=session)


async def test_use_case_uses_get_for_update():
    repo = _SpyRepo()
    uc = UseCase_Debit(session_factory=lambda: _SessionSpy("postgresql"), repo=repo)
    with pytest.raises(AccountNotFound):  # the spy returns None -> expected path
        await uc.execute(account_id="A1", amount=Money(100, "USD"), payment_id="P-LOCK")
    assert repo.for_update_calls == 1   # the write-side took the lock path


@pytest.mark.parametrize("dialect,expect_locking", [("postgresql", True), ("sqlite", False)])
async def test_get_for_update_locks_only_on_postgres(dialect: str, expect_locking: bool):
    repo = SQLAlchemyAccountRepository()
    spy = _SessionSpy(dialect)
    await repo.get_for_update("A1", session=spy)
    assert spy.calls == 1
    sql = _compiled(spy.last_stmt, dialect).upper()
    assert ("FOR UPDATE" in sql) is expect_locking
