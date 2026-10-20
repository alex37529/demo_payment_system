"""Intake HTTP API (port 8001): POST /v1/payments + GET /v1/payments/{id}.

The **create** path (`POST /v1/payments`) enforces the spec contract:
  1. require `Idempotency-Key` header (else 400, spec §6 red flag);
  2. run `IntakeService.create` (acquire -> publish -> complete/rollback);
  3. map the service's `(status, body)` onto the response;
  4. on `KafkaUnavailable` → 503 (key released, client may retry).

`GET /v1/payments/{payment_id}` returns the cached accepted-payment record from
`intake.idempotency_keys` (spec §5 commit #4: "add POST /v1/payments + GET
/v1/payments/{id}"), so an idempotent replay can always be re-derived even
after Redis eviction.

Wiring mirrors `services/orchestrator/api.py::attach_orchestrator` (engine /
session-factory bootstrap + lifespan start/stop). The `attach_intake` hook
accepts *optional* pre-built collaborators (`repo`, `idem`, `publisher`) so
tests / smoke can inject fakes deterministically — the routes always resolve
the current collaborators from `app.state` at request time (closure-free),
which keeps that contract honest.
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from fastapi import FastAPI, Header, Request
from opentelemetry import trace
from starlette.responses import JSONResponse

from services.intake.application import (
    CreatePaymentRequest,
    IntakeService,
    KafkaUnavailable,
)
from services.intake.application.idempotency import IdempotencyService
from services.intake.domain import PaymentNotFound
from services.intake.infrastructure import IdempotencyRepo
from shared.kafka import KafkaProducer

log = logging.getLogger(__name__)

#: how often the PG idempotency mirror is pruned of expired rows.
CLEANUP_INTERVAL_S = 60

#: keys on `app.state` the routes read at request time (closure-free so a test
#: can substitute any of the collaborators after `attach_intake`; see spec §5/§7).
ST_STATE = "intake_service"
ST_IDEM = "intake_idem"
ST_REPO = "intake_repo"
ST_ENGINE = "intake_engine"


def _dsn() -> str:
    dsn = os.environ.get("INTAKE_DSN") or os.environ.get("DATABASE_URL", "")
    for prefix in ("postgresql://", "postgres://"):
        if dsn.startswith(prefix):
            dsn = dsn.replace(prefix, "postgresql+asyncpg://", 1)
            break
    if not dsn:
        dsn = "postgresql+asyncpg://fintech:fintech@postgres:5432/intake"
    return dsn


def _engine_for(dsn: str):
    from sqlalchemy.ext.asyncio import create_async_engine

    from shared.db.base import make_engine

    if dsn.startswith("sqlite"):
        from sqlalchemy.pool import StaticPool
        return create_async_engine(dsn, poolclass=StaticPool)
    return make_engine(dsn)


def _cleanup_enabled() -> bool:
    env = os.environ.get("INTAKE_IDEM_CLEANUP", "1").lower()
    return env not in ("0", "false", "no")


def attach_intake(
    app: FastAPI,
    *,
    repo: IdempotencyRepo | None = None,
    idem: IdempotencyService | None = None,
    publisher: KafkaProducer | None = None,
) -> None:
    """Wire the intake service onto an existing app.

    Any of `repo` / `idem` / `publisher` may be supplied (e.g. by a test);
    the rest are built from env vars. All live on `app.state` and are the
    *single source of truth* the routes consume at call time.
    """
    from contextlib import asynccontextmanager

    from shared.db.base import Base, make_session_factory

    if repo is None:
        dsn = _dsn()
        engine = _engine_for(dsn)
        repo = IdempotencyRepo(make_session_factory(engine))
        own_engine: Any = engine
    else:
        own_engine = None    # an injected repo owns its engine (or the tests do)

    if idem is None:
        redis_url = os.environ.get("REDIS_URL", "redis://redis:6379/0")
        idem = IdempotencyService(redis_url=redis_url, repo=repo)
    if publisher is None:
        publisher = KafkaProducer(os.environ.get("KAFKA_BROKERS", "kafka:9092"))

    service = IntakeService(idem=idem, publisher=publisher)

    setattr(app.state, ST_STATE, service)
    setattr(app.state, ST_IDEM, idem)
    setattr(app.state, ST_REPO, repo)
    setattr(app.state, ST_ENGINE, own_engine)

    _register_routes(app)
    register_exception_handlers(app)

    @asynccontextmanager
    async def _intake_lifespan(_app: FastAPI):
        # create the idempotency table if we own (or share) a SQLAlchemy
        # engine — for tests that inject a custom engine the table has usually
        # already been created, so this is a defensive no-op there.
        eng = getattr(app.state, ST_ENGINE, None)
        if eng is not None:
            async with eng.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)

        tasks: list[asyncio.Task] = []
        # start the Kafka producer (best-effort — the use-case handles publish
        # failure with rollback + 503).
        if hasattr(publisher, "start"):
            try:
                await publisher.start()
            except Exception:  # noqa: BLE001
                log.warning("intake: kafka producer start failed "
                            "(publishes will surface as 503 + rollback)")

        # periodic cleanup of expired PG idempotency rows (spec §3.3: PG rows
        # don't self-expire like Redis — the mirror needs a sweep).
        if _cleanup_enabled():
            tasks.append(asyncio.create_task(
                _cleanup_loop(idem, CLEANUP_INTERVAL_S), name="intake-idem-cleanup"))

        log.info("intake ready (repo=%s)", type(repo).__name__)
        try:
            yield
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()
            if hasattr(publisher, "stop"):
                try:
                    await publisher.stop()
                except Exception as e:  # noqa: BLE001
                    log.debug("intake: kafka producer stop failed: %s", e)
            await idem.aclose()
            if own_engine is not None:
                await own_engine.dispose()
            log.info("intake shutdown complete")

    app.router.lifespan_context = _intake_lifespan


async def _cleanup_loop(idem: IdempotencyService, interval_s: int) -> None:
    while True:
        await asyncio.sleep(interval_s)
        try:
            removed = await idem.cleanup()
            if removed:
                log.info("intake: pruned %d expired idempotency rows", removed)
        except asyncio.CancelledError:  # type: ignore[misc]
            raise
        except Exception:
            log.exception("intake: idempotency cleanup pass failed")


# --------------------------------------------------------------------------- #
# test helpers (mirror orchestrator/api.py start / stop)
# --------------------------------------------------------------------------- #
async def start(app: FastAPI) -> None:
    lf = app.router.lifespan_context
    cm = lf(app)
    try:
        await cm.__aenter__()
    except Exception:
        await cm.__aexit__(None, None, None)
        raise
    app.state.intake_lf = cm


async def stop(app: FastAPI) -> None:
    cm = getattr(app.state, "intake_lf", None)
    if cm is not None:
        await cm.__aexit__(None, None, None)


# --------------------------------------------------------------------------- #
# Routes (read collaborators from app.state at request time — single source of
# truth that `attach_intake` / tests both write to).
# --------------------------------------------------------------------------- #
def _register_routes(app: FastAPI) -> None:
    @app.post("/v1/payments")
    async def create_payment(
        req: CreatePaymentRequest,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> JSONResponse:
        if not idempotency_key:
            # spec §6 red flag: a create without an Idempotency-Key must 400,
            # not silently create a payment.
            return JSONResponse(status_code=400,
                                content={"ok": False, "error": "Idempotency-Key required"})
        service: IntakeService = getattr(app.state, ST_STATE)
        try:
            status, body = await service.create(req, idem_key=idempotency_key)
        except KafkaUnavailable as e:
            return JSONResponse(status_code=503,
                                content={"ok": False, "error": "kafka_unavailable",
                                         "detail": str(e)})
        # Tag the span with the payment id, so a trace can be FOUND by
        # payment in Tempo (`/api/search?tags=payment_id=...`) — the
        # trace id is only known to the client that made the request.
        payment_id = str((body or {}).get("payment_id") or "")
        if payment_id:
            span = trace.get_current_span()
            if span.is_recording():
                span.set_attribute("payment_id", payment_id)
        return JSONResponse(status_code=status, content=body)

    @app.get("/v1/payments/{payment_id}")
    async def get_payment(payment_id: str) -> JSONResponse:
        repo: IdempotencyRepo = getattr(app.state, ST_REPO)
        body = await repo.get_by_payment(payment_id)
        if body is None:
            raise PaymentNotFound(payment_id)
        return JSONResponse(body)


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(PaymentNotFound)
    async def _not_found(_: Request, exc: PaymentNotFound) -> JSONResponse:
        return JSONResponse(status_code=404,
                            content={"ok": False, "error": "PaymentNotFound",
                                     "detail": str(exc)})
