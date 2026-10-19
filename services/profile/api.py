"""Profile HTTP API (port 8004): GET /v1/users (+ GET /v1/users/{id}).

Read-Side (CQRS, phase-5 spec §3.3): a denormalized copy of the write-side
balance, kept fresh by the ``ledger.events`` consumer. The API is pure reads —
it never mutates and never reads the ``ledger`` DB (pitfall #4: no cross-
schema JOIN).

Wiring mirrors ``services/intake/api.py::attach_intake`` (engine/session-factory
bootstrap + lifespan start/stop). ``attach_profile`` accepts an optional pre-
built ``repo`` so tests / smoke can inject a session bound to an in-memory DB —
the routes resolve the current repo from ``app.state`` at request time
(closure-free). The consumer task is started in the lifespan and disabled by
``PROFILE_ENABLE_CONSUMER=0`` (tests / hermetic runs).
"""
from __future__ import annotations

import asyncio
import logging
import os

from fastapi import FastAPI
from starlette.responses import JSONResponse

from services.profile.infrastructure import ProfileRepo

log = logging.getLogger(__name__)


def _dsn() -> str:
    dsn = os.environ.get("PROFILE_DSN") or os.environ.get("DATABASE_URL", "")
    for prefix in ("postgresql://", "postgres://"):
        if dsn.startswith(prefix):
            dsn = dsn.replace(prefix, "postgresql+asyncpg://", 1)
            break
    if not dsn:
        dsn = "postgresql+asyncpg://fintech:fintech@postgres:5432/profile"
    return dsn


def _engine_for(dsn: str):
    from sqlalchemy.ext.asyncio import create_async_engine

    from shared.db.base import make_engine

    if dsn.startswith("sqlite"):
        from sqlalchemy.pool import StaticPool
        return create_async_engine(dsn, poolclass=StaticPool)
    return make_engine(dsn)


def _consumer_enabled() -> bool:
    return os.environ.get("PROFILE_ENABLE_CONSUMER", "1") not in ("0", "false", "False")


def attach_profile(app: FastAPI, *, repo: ProfileRepo | None = None) -> None:
    """Wire the profile service onto an existing FastAPI app.

    An injected ``repo`` owns its engine (tests); otherwise one is built from
    ``PROFILE_DSN``. Either way the routes read ``app.state.profile_repo`` at
    request time (single source of truth, closure-free — intake pattern §).
    """
    from contextlib import asynccontextmanager

    from shared.db.base import make_session_factory

    if repo is None:
        dsn = _dsn()
        engine = _engine_for(dsn)
        repo = ProfileRepo(make_session_factory(engine))
        own_engine: object = engine
    else:
        own_engine = None  # injected repo owns its engine (or the test does)

    app.state.profile_repo = repo
    app.state.profile_engine = own_engine

    _register_routes(app)

    @asynccontextmanager
    async def _profile_lifespan(_app: FastAPI):
        # The `profile` schema + tables + 20 seeded users are provisioned by
        # init/50_seed.sql at first postgres boot (see phase-5 spec §2, pitfall
        # #2: seed is ONE atomic TX touching BOTH the ledger and profile
        # schemas). So there is nothing to `create_all` here — the Read-Side
        # assumes the table already exists. Tests seed their own SQLite schema
        # directly (conftest).
        consumer_task: asyncio.Task | None = None
        if _consumer_enabled():
            from services.profile import kafka_consumer

            consumer_task = asyncio.create_task(
                kafka_consumer.run_consumer(
                    brokers=os.environ.get("KAFKA_BROKERS", "kafka:9092"),
                    repo=repo),
                name="profile-events")
        log.info("profile ready (consumer=%s)", _consumer_enabled())
        try:
            yield
        finally:
            if consumer_task is not None and not consumer_task.done():
                consumer_task.cancel()
            if own_engine is not None:
                await own_engine.dispose()
            log.info("profile engine disposed")

    app.router.lifespan_context = _profile_lifespan


async def start(app: FastAPI) -> None:
    """Manually trigger the startup portion (used by the API tests)."""
    lf = app.router.lifespan_context
    cm = lf(app)
    try:
        await cm.__aenter__()
    except Exception:
        await cm.__aexit__(None, None, None)
        raise
    app.state.profile_lf = cm


async def stop(app: FastAPI) -> None:
    cm = getattr(app.state, "profile_lf", None)
    if cm is not None:
        await cm.__aexit__(None, None, None)


def _register_routes(app: FastAPI) -> None:
    @app.get("/v1/users")
    async def list_users():
        repo: ProfileRepo = app.state.profile_repo
        return await repo.list_all()

    @app.get("/v1/users/{account_id}")
    async def get_user(account_id: str):
        repo: ProfileRepo = app.state.profile_repo
        row = await repo.get(account_id)
        if row is None:
            return JSONResponse(status_code=404,
                                content={"ok": False, "error": "UserNotFound",
                                         "detail": account_id})
        return row
