"""Intake test fixtures — local SQLite + a fake Redis + fake Kafka publisher.

Nothing here touches a real broker or Redis: `harness` swaps the service's
`redis`/`publisher` collaborators for in-process fakes so we can assert the
exact acquire → publish → complete/rollback flow (phase-4 spec §3.3/§5).

A local *file* SQLite DB (not `:memory:`) is shared by both the `harness` and
the FastAPI `client` so direct-service writes are visible to the HTTP layer
(and vice-versa). The file lives under the OS temp dir and is removed after each
test (best-effort).
"""
from __future__ import annotations

import os
import tempfile
import types
import uuid
from dataclasses import dataclass, field

import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from services.intake.application.idempotency import IdempotencyService
from services.intake.application.use_case import IntakeService
from services.intake.infrastructure import IdempotencyRepo
from shared.db.base import Base


class FakeRedis:
    """In-memory substitute for `redis.asyncio.Redis` (subset of methods used)."""

    def __init__(self) -> None:
        self.store: dict[str, object] = {}
        self.fail = False          # set True to simulate "redis down"

    async def set(self, key: str, value: object, *, nx: bool = False,
                  ex: int | None = None) -> bool:
        if self.fail:
            raise ConnectionError("redis down (test)")
        if nx and key in self.store:
            return False
        self.store[key] = value
        return True

    async def get(self, key: str) -> object | None:
        if self.fail:
            raise ConnectionError("redis down (test)")
        return self.store.get(key)

    async def delete(self, key: str) -> int:
        if self.fail:
            raise ConnectionError("redis down (test)")
        self.store.pop(key, None)
        return 1

    async def aclose(self) -> None:
        pass


class FakePublisher:
    """Records publishes; `fail=True` simulates Kafka being unreachable."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str, dict]] = []
        self.fail = False

    async def publish(self, topic: str, key: str, value: dict) -> None:
        if self.fail:
            raise RuntimeError("kafka down (test)")
        self.published.append((topic, key, value))


@dataclass
class Harness:
    redis: FakeRedis
    publisher: FakePublisher
    idem: IdempotencyService
    service: IntakeService
    repo: IdempotencyRepo
    session_factory: object = field(repr=False)
    db_path: str = ""


@pytest_asyncio.fixture
async def harness():
    tmp = tempfile.gettempdir()
    db_path = os.path.join(tmp, f"intake_test_{uuid.uuid4().hex}.sqlite")
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}",
                                 connect_args={"check_same_thread": False})
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    repo = IdempotencyRepo(session_factory)
    idem = IdempotencyService(redis_url="redis://test", repo=repo)
    idem.redis = FakeRedis()                       # swap in the fake
    publisher = FakePublisher()
    service = IntakeService(idem=idem, publisher=publisher)

    yield Harness(redis=idem.redis, publisher=publisher, idem=idem,
                  service=service, repo=repo, session_factory=session_factory,
                  db_path=db_path)

    await idem.redis.aclose()
    await engine.dispose()
    try:
        os.remove(db_path)
    except OSError:
        pass


@pytest_asyncio.fixture
async def client(harness) -> types.SimpleNamespace:
    """A FastAPI app wired to the SAME PG / fake Redis / fake publisher as
    `harness`, so HTTP responses reflect the very same state the tests assert on.

    Sharing the single SQLAlchemy engine (one connection pool) is essential — two
    engines on the same SQLite file would deadlock with concurrent writers.
    """
    os.environ["INTAKE_IDEM_CLEANUP"] = "0"  # keep the app hermetic (no bg task)

    from httpx import ASGITransport, AsyncClient

    from services.intake.api import attach_intake, start, stop
    from services.shared_app import build_app

    app = build_app("intake-test")
    # one shared engine: `harness.session_factory` owns it; `attach_intake`
    # reuses the harness's idem/publisher fakes (so the Kafka fake is in play).
    attach_intake(
        app,
        repo=harness.repo,
        idem=harness.idem,
        publisher=harness.publisher,
    )
    await start(app)

    async with AsyncClient(transport=ASGITransport(app=app),
                           base_url="http://test") as c:
        yield types.SimpleNamespace(app=app, http=c, redis=harness.redis,
                                    publisher=harness.publisher,
                                    repo=harness.repo, harness=harness)
    await stop(app)
