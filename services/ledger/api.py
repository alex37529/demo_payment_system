"""Ledger HTTP command API (manual-test surface; main path is `ledger.commands`).

Endpoints (port 8003):
  POST /v1/accounts/{account_id}/debit
  POST /v1/accounts/{account_id}/reserve
  POST /v1/accounts/{account_id}/refund
  POST /v1/accounts/{account_id}/cancel-reservation
  GET  /v1/accounts/{account_id}
  GET  /v1/accounts

Both the HTTP API and the Kafka consumer funnel into the SAME use-cases
(single source of truth — phase-2 spec §8, pitfall #10).
"""
from __future__ import annotations

import asyncio
import logging
import os

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import text
from starlette.responses import JSONResponse

from services.ledger.application import (
    DeliveryTracker,
    LedgerUseCaseRunner,
    UseCase_CancelReservation,
    UseCase_Debit,
    UseCase_Refund,
    UseCase_Reserve,
)
from services.ledger.application.use_cases import TOPIC_LEDGER_EVENTS
from services.ledger.domain import (
    Account,
    AccountNotFound,
    InsufficientFundsError,
    LedgerError,
    Money,
)
from services.ledger.infrastructure.models import AccountRow, TransactionRow
from services.ledger.infrastructure.repository import SQLAlchemyAccountRepository

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# DTOs
# --------------------------------------------------------------------------- #
class MoneyIn(BaseModel):
    """``{"value":100,"currency":"USD"}`` or ``{"amount":100,"currency":"USD"}``."""

    value: int | None = None
    amount: int | None = None
    currency: str = "USD"

    def to_money(self) -> Money:
        amt = self.value if self.value is not None else self.amount
        if amt is None:
            raise ValueError("money requires `value` or `amount` (int, minor units)")
        return Money(int(amt), self.currency.upper())


class LedgerCommandIn(BaseModel):
    """Body for debit / reserve / refund / cancel-reservation."""

    payment_id: str = ""
    saga_id: str = ""
    reason: str = ""
    amount: int | MoneyIn | None = None
    currency: str = "USD"
    value: int | None = None  # shorthand: bare amount + separate currency field

    def to_money(self) -> Money:
        if isinstance(self.amount, MoneyIn):
            return self.amount.to_money()
        if isinstance(self.amount, int):
            return Money(self.amount, self.currency.upper())
        if self.value is not None:
            return Money(self.value, self.currency.upper())
        raise ValueError("`amount` (or `value`) is required")


class CommandOut(BaseModel):
    ok: bool = True
    payment_id: str = ""
    account_id: str = ""
    amount: dict = {}
    applied: bool = True
    event_type: str = ""


# --------------------------------------------------------------------------- #
# Bootstrap (engine / session factory / repo / runner / consumer)
# --------------------------------------------------------------------------- #
def _dsn() -> str:
    dsn = os.environ.get("LEDGER_DSN") or os.environ.get("DATABASE_URL", "")
    for p in ("postgresql://", "postgres://"):
        if dsn.startswith(p):
            dsn = dsn.replace(p, "postgresql+asyncpg://", 1)
            break
    if not dsn:  # default for compose (postgres service, role fintech)
        dsn = "postgresql+asyncpg://fintech:fintech@postgres:5432/ledger"
    return dsn


def _engine_for(dsn: str):
    from sqlalchemy.ext.asyncio import create_async_engine

    from shared.db.base import make_engine

    if dsn.startswith("sqlite"):
        # sqlite (tests): keep a single shared connection for :memory: DBs
        from sqlalchemy.pool import StaticPool

        return create_async_engine(dsn, poolclass=StaticPool)
    return make_engine(dsn)


def _consumer_enabled() -> bool:
    return os.environ.get("LEDGER_ENABLE_CONSUMER", "1") not in ("0", "false", "False")


def _tracker_enabled() -> bool:
    return os.environ.get("LEDGER_ENABLE_TRACKER", "1") not in ("0", "false", "False")


def attach_ledger(app: FastAPI) -> None:
    """Wire the ledger service onto an existing FastAPI app (shared.observability)."""
    from contextlib import asynccontextmanager

    dsn = _dsn()
    engine = _engine_for(dsn)
    from shared.db.base import Base, make_session_factory

    sf = make_session_factory(engine)
    runner = LedgerUseCaseRunner(session_factory=sf, repo=SQLAlchemyAccountRepository())
    app.state.ledger_runner = runner
    app.state.ledger_engine = engine

    brokers = os.environ.get("KAFKA_BROKERS", "kafka:9092")
    tracker = DeliveryTracker(brokers=brokers, session_factory=sf,
                              topic=TOPIC_LEDGER_EVENTS)
    app.state.ledger_tracker = tracker

    register_exception_handlers(app)
    register_routes(app, tracker)

    @asynccontextmanager
    async def _ledger_lifespan(_app):
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            # `create_all` CREATES missing tables but never ALTERS an existing
            # one, so the `accounts.holds` column (ADR-010) has to be added
            # explicitly for a volume created before it existed.
            # Idempotent — and Postgres-only syntax, so the SQLite test
            # harness (which creates the schema from the models) skips it.
            if conn.dialect.name == "postgresql":
                await conn.execute(text(
                    "ALTER TABLE accounts ADD COLUMN IF NOT EXISTS "
                    "holds JSONB NOT NULL DEFAULT '{}'::jsonb"
                ))
        consumer_task = None
        tracker_task = None
        if _consumer_enabled():
            from services.ledger import kafka_consumer

            consumer_task = asyncio.create_task(
                kafka_consumer.run_consumer(brokers=brokers, runner=runner),
                name="ledger-commands")
        if _tracker_enabled():
            tracker_task = asyncio.create_task(
                tracker.run(),
                name="ledger-outbox-tracker")
            log.info("ledger outbox tracker started (%s)", brokers)
        log.info("ledger ready (dsn=%s, consumer=%s, tracker=%s)", dsn.split("@")[-1],
                 _consumer_enabled(), _tracker_enabled())
        try:
            yield
        finally:
            for t in (consumer_task, tracker_task):
                if t is not None and not t.done():
                    t.cancel()
            await engine.dispose()
            log.info("ledger engine disposed")

    # Starlette (1.0+) uses a single `lifespan` callable, invoked once on
    # app start. Replace any prior handler so calling attach_ledger() twice
    # is safe, and so we can still run it manually in tests (via `start()`).
    app.router.lifespan_context = _ledger_lifespan


async def start(app: FastAPI) -> None:
    """Manually trigger the startup portion (used by the API tests)."""
    # Grab the lifespan context manager from the app and invoke its __aenter__.
    lf = app.router.lifespan_context
    cm = lf(app)
    try:
        await cm.__aenter__()
    except Exception:
        await cm.__aexit__(None, None, None)
        raise
    # stash so `stop()` can close the same context
    app.state.ledger_lf_cm = cm


async def stop(app: FastAPI) -> None:
    cm = getattr(app.state, "ledger_lf_cm", None)
    if cm is not None:
        await cm.__aexit__(None, None, None)


# --------------------------------------------------------------------------- #
# Routes
# --------------------------------------------------------------------------- #
async def _run_command(app: FastAPI, uc_cls, account_id: str, body: LedgerCommandIn) -> CommandOut:
    try:
        amount = body.to_money()
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=422, detail=str(e)) from e

    runner = app.state.ledger_runner
    uc = uc_cls(session_factory=runner.session_factory, repo=runner.repo)
    res = await uc.execute(account_id=account_id, amount=amount,
                           payment_id=body.payment_id, reason=body.reason)
    return CommandOut(
        payment_id=res.get("payment_id", body.payment_id),
        account_id=account_id,
        amount=res.get("amount", amount.to_dict()),
        applied=res.get("applied", True),
        event_type=res.get("event_type", ""),
    )


def register_routes(app: FastAPI, tracker: DeliveryTracker) -> None:
    @app.post("/v1/outbox/sweep")
    async def _sweep_outbox():
        """Manually run one outbox-delivery sweep (ops / smoke)."""
        counts = await tracker.sweep_once()
        return JSONResponse({"ok": True, "counts": counts})

    @app.get("/v1/outbox")
    async def _list_outbox(limit: int = 20):
        from sqlalchemy import select

        from shared.outbox import OutboxEvent

        sf = app.state.ledger_runner.session_factory
        async with sf() as s:
            rows = (await s.execute(
                select(OutboxEvent).order_by(OutboxEvent.created_at.desc())
                .limit(min(200, max(1, limit)))
            )).scalars().all()
            return [{
                "event_id": r.event_id, "topic": r.topic, "status": r.status,
                "published_at": r.published_at.isoformat() if r.published_at else None,
                "last_error": r.last_error,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            } for r in rows]

    @app.post("/v1/accounts/{account_id}/debit", response_model=CommandOut)
    async def debit(account_id: str, body: LedgerCommandIn):
        return await _run_command(app, UseCase_Debit, account_id, body)

    @app.post("/v1/accounts/{account_id}/reserve", response_model=CommandOut)
    async def reserve(account_id: str, body: LedgerCommandIn):
        return await _run_command(app, UseCase_Reserve, account_id, body)

    @app.post("/v1/accounts/{account_id}/refund", response_model=CommandOut)
    async def refund(account_id: str, body: LedgerCommandIn):
        return await _run_command(app, UseCase_Refund, account_id, body)

    @app.post("/v1/accounts/{account_id}/cancel-reservation", response_model=CommandOut)
    async def cancel_reservation(account_id: str, body: LedgerCommandIn):
        return await _run_command(app, UseCase_CancelReservation, account_id, body)

    @app.get("/v1/accounts/{account_id}/transactions")
    async def transactions(account_id: str, limit: int = 50, offset: int = 0):
        """Balance journal for an account (append-only, newest first).

        Each row is self-describing: `amount`, `event_type` (debit vs refund),
        and `balance_after` (running balance). The last row's `balance_after`
        equals the account's current balance.
        """
        from sqlalchemy import func, select

        sf = app.state.ledger_runner.session_factory
        limit = min(500, max(1, limit))
        offset = max(0, offset)
        async with sf() as s:
            base = select(TransactionRow).where(TransactionRow.account_id == account_id)
            total = (await s.execute(
                select(func.count()).select_from(base.subquery()))).scalar_one()
            rows = (await s.execute(
                base.order_by(TransactionRow.id.desc())
                .limit(limit).offset(offset))).scalars().all()
            current = await s.get(AccountRow, account_id)
            if current is None:
                raise AccountNotFound(account_id)
            return {
                "account_id": account_id,
                "current_balance": current.balance,
                "limit": limit,
                "offset": offset,
                "total": total,
                "transactions": [
                    {
                        "id": r.id,
                        "event_type": r.event_type,
                        "amount": r.amount,
                        "currency": r.currency,
                        "balance_after": r.balance_after,
                        "payment_id": r.payment_id,
                        "event_id": r.event_id,
                        "created_at": r.created_at.isoformat() if r.created_at else None,
                    } for r in rows
                ],
            }

    @app.get("/v1/accounts/{account_id}")
    async def get_account(account_id: str):
        sf = app.state.ledger_runner.session_factory
        async with sf() as s:
            row = await s.get(AccountRow, account_id)
            if row is None:
                raise AccountNotFound(account_id)
            agg = Account.load(row)
            return {
                "id": agg.account_id, "owner_name": agg.owner_name, "currency": agg.currency,
                "balance": agg.balance.to_dict(), "reserved": agg.reserved.to_dict(),
                "available": agg.available.to_dict(),
                # The per-payment holds (ADR-010): which reservation is still
                # holding money, and for which payment. Without this, a stuck
                # hold is invisible — the account just looks "almost empty".
                "holds": {pid: amount for pid, amount in agg.holds_items()},
            }


# --------------------------------------------------------------------------- #
# Error handlers (domain -> HTTP)
# --------------------------------------------------------------------------- #
def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(AccountNotFound)
    async def _not_found(_: Request, exc: AccountNotFound) -> JSONResponse:
        return JSONResponse(status_code=404,
                            content={"ok": False, "error": "AccountNotFound", "detail": str(exc)})

    @app.exception_handler(InsufficientFundsError)
    async def _insufficient(_: Request, exc: InsufficientFundsError) -> JSONResponse:
        return JSONResponse(status_code=409,
                            content={"ok": False, "error": "InsufficientFundsError", "detail": str(exc)})

    @app.exception_handler(LedgerError)
    async def _ledger_error(_: Request, exc: LedgerError) -> JSONResponse:
        return JSONResponse(status_code=400,
                            content={"ok": False, "error": type(exc).__name__, "detail": str(exc)})
