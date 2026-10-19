"""Orchestrator HTTP API (port 8002).

Read-only status surface (spec §6, phase-3 spec §9):
    GET /v1/sagas/{saga_id}                       — full saga (state, history, compensation)
    GET /v1/sagas/by-payment/{payment_id}          — look up by payment_id
    GET /v1/circuit-breakers                       — CB state for `/_status` + acceptance scripts

The **main** path is the Kafka consumer (`payments.commands` + `ledger.events`).
The HTTP API is for observability and smoke testing (analogous to the phase-2
"manual-test surface" on the ledger).
"""
from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Iterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from starlette.responses import JSONResponse

from services.orchestrator.application import SagaHandler, run_watchdog
from services.orchestrator.domain import Saga
from services.orchestrator.infrastructure.fraud_client import make_fraud_client
from services.orchestrator.infrastructure.models import SagaRow
from services.orchestrator.infrastructure.repository import SagaRepository
from shared.kafka import KafkaProducer

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------- #
# DSN / engine bootstrap (mirrors ledger/api.py)
# -------------------------------------------------------------------------- #
def _dsn() -> str:
    dsn = os.environ.get("ORCHESTRATOR_DSN") or os.environ.get("DATABASE_URL", "")
    for prefix in ("postgresql://", "postgres://"):
        if dsn.startswith(prefix):
            dsn = dsn.replace(prefix, "postgresql+asyncpg://", 1)
            break
    if not dsn:
        dsn = "postgresql+asyncpg://fintech:fintech@postgres:5432/orchestrator"
    return dsn


def _engine_for(dsn: str):
    from sqlalchemy.ext.asyncio import create_async_engine

    from shared.db.base import make_engine

    if dsn.startswith("sqlite"):
        from sqlalchemy.pool import StaticPool
        return create_async_engine(dsn, poolclass=StaticPool)
    return make_engine(dsn)


def _consumer_enabled() -> bool:
    return os.environ.get("ORCH_ENABLE_CONSUMER", "1") not in ("0", "false", "False")


# -------------------------------------------------------------------------- #
# FastAPI wiring
# -------------------------------------------------------------------------- #
def attach_orchestrator(app: FastAPI) -> None:
    from shared.db.base import Base, make_session_factory

    dsn = _dsn()
    engine = _engine_for(dsn)
    sf = make_session_factory(engine)
    repo = SagaRepository(sf)
    fraud, fraud_url = make_fraud_client(os.environ.get("FRAUD_URL", ""))
    publisher = KafkaProducer(os.environ.get("KAFKA_BROKERS", "kafka:9092"))

    handler = SagaHandler(repo=repo, publisher=publisher, fraud=fraud, fraud_url=fraud_url)

    app.state.orchestrator = handler
    app.state.orchestrator_repo = repo
    app.state.orchestrator_engine = engine
    app.state.orchestrator_fraud = fraud

    _register_routes(app, repo)

    @asynccontextmanager
    async def _lifespan(_app: FastAPI) -> Iterator[None]:
        # 1. create the `sagas` table (fresh DB or new deployment)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        # 2. start publisher (Kafka — best-effort; if brokers unreachable, the
        #    handler will log-and-fallback on every publish, so the service still
        #    comes up (matching the ledger behaviour of starting only if
        #    `ORCH_ENABLE_CONSUMER=1`).)
        publisher_enabled = _consumer_enabled()
        if publisher_enabled:
            # wrap in try so tests (no Kafka) and smoke (offline) can proceed.
            # The handler's own `_publish_raw` catches `Exception` and logs,
            # so a broken publisher never kills the service.
            try:
                await publisher.start()
            except Exception:  # noqa: BLE001
                log.warning("publisher start failed (tests/offline mode); "
                            "Kafka publishes will log-and-fallback")

        # 3. start consumers (if enabled)
        tasks: list[asyncio.Task] = []
        if _consumer_enabled():
            from services.orchestrator import kafka_consumer as kc

            cmd_task, evt_task = await kc.run_consumers(
                handler, brokers=os.environ.get("KAFKA_BROKERS", "kafka:9092"),
                enabled=True)
            tasks.extend(t for t in (cmd_task, evt_task) if t is not None)
            log.info("orchestrator consumers: cmds=%s evt=%s",
                     "on" if cmd_task else "off", "on" if evt_task else "off")

        # 4. start watchdog (default 60s; disable with ORCH_WATCHDOG_S=0)
        wd = asyncio.create_task(run_watchdog(handler), name="orch-watchdog")
        tasks.append(wd)
        log.info("orchestrator: watchdog %s", "on" if wd else "off")

        log.info("orchestrator ready (dsn=%s)", dsn.split("@")[-1])
        try:
            yield
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()
            if publisher_enabled:
                await publisher.stop()
            await fraud.close()
            await engine.dispose()
            log.info("orchestrator shutdown complete")

    app.router.lifespan_context = _lifespan


# -------------------------------------------------------------------------- #
# test helpers (mirror ledger/api.py start / stop)
# -------------------------------------------------------------------------- #
async def start(app: FastAPI) -> None:
    lf = app.router.lifespan_context
    cm = lf(app)
    try:
        await cm.__aenter__()
    except Exception:
        await cm.__aexit__(None, None, None)
        raise
    app.state.orchestrator_lf = cm


async def stop(app: FastAPI) -> None:
    cm = getattr(app.state, "orchestrator_lf", None)
    if cm is not None:
        await cm.__aexit__(None, None, None)


# -------------------------------------------------------------------------- #
# Routes (spec §6)
# -------------------------------------------------------------------------- #
def _saga_to_dict(row: SagaRow) -> dict:
    saga = Saga.load(row)
    return {
        "id": saga.saga_id,
        "payment_id": saga.payment_id,
        "state": saga.state,
        "history": saga.history,
        "compensation": saga.compensation,
        "reservation_confirmed": saga.has_confirmed_reservation(),
    }


def _register_routes(app: FastAPI, repo: SagaRepository) -> None:
    @app.get("/v1/sagas/{saga_id}")
    async def get_saga(saga_id: str) -> JSONResponse:
        row = await repo.get(saga_id)
        if row is None:
            raise HTTPException(status_code=404, detail=f"saga {saga_id} not found")
        return JSONResponse(_saga_to_dict(row))

    @app.get("/v1/sagas/by-payment/{payment_id}")
    async def get_saga_by_payment(payment_id: str) -> JSONResponse:
        row = await repo.get_by_payment(payment_id)
        if row is None:
            raise HTTPException(status_code=404, detail=f"no saga for payment {payment_id}")
        return JSONResponse(_saga_to_dict(row))

    @app.get("/v1/circuit-breakers")
    async def circuit_breakers() -> JSONResponse:
        fraud: object = app.state.orchestrator_fraud
        return JSONResponse({
            "fraud": {
                "state": fraud.state(),
                "is_open": fraud.is_open(),
            }
        })
