"""Use-case tests: one TX (accounts + applied_commands + outbox_events), idempotency, invariants.

Uses an in-memory async SQLite DB (aiosqlite) so the REAL repository + aggregate +
outbox path runs — no mocks of the TX. Mirrors the prod Postgres behaviour for the
phase-2 exit: debit -> balance-X + outbox row + (Kafka via Debezium in infra).
"""
from __future__ import annotations

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
from services.ledger.infrastructure.models import AppliedCommandRow
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
