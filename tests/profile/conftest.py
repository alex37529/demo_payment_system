"""Profile read-side tests (phase-5 spec §3, pitfall #5) — hermetic (in-memory
SQLite, no Kafka / no real PG).

The models live in the ``profile`` DB's public schema (the DB *is* the bounded
context). For the in-memory SQLite harness we simply create the same two tables
(``users`` / ``processed_events``) in the default schema and seed three users,
so ``ProfileRepo`` queries hit exactly what the tests expect.
"""
from __future__ import annotations

import os

import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

# Disable the Profile consumer for all tests in this dir: hermetic, no Kafka.
os.environ["PROFILE_ENABLE_CONSUMER"] = "0"
# Keep the shared observability middleware from trying to reach a real OTel endpoint.
os.environ.setdefault("OTEL_EXPORTER_OTLP_ENDPOINT", "")


@pytest_asyncio.fixture
async def harness():
    # StaticPool: in-memory SQLite is per-connection, so a single shared engine
    # is required for every session to see the same data (the classic gotcha).
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.execute(text(
            "CREATE TABLE users ("
            "id TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '',"
            " email TEXT NOT NULL DEFAULT '', currency TEXT NOT NULL DEFAULT 'USD',"
            " balance INTEGER NOT NULL DEFAULT 0, updated_at DATETIME)"))
        await conn.execute(text(
            "CREATE TABLE processed_events ("
            "event_id TEXT PRIMARY KEY, event_type TEXT NOT NULL DEFAULT '',"
            " account_id TEXT NOT NULL DEFAULT '', first_seen_at DATETIME)"))
        seed = [
            ("U001", "Alice Alvarez", "alice.alvarez@example.com", "USD", 620000),
            ("U002", "Benjamin Baker", "benjamin.baker@example.com", "EUR", 480000),
            ("U003", "Carla Chen", "carla.chen@example.com", "USD", 350000),
        ]
        for (uid, name, email, curr, bal) in seed:
            await conn.execute(
                text("INSERT INTO users (id, name, email, currency, balance) "
                     "VALUES (:u, :n, :e, :c, :b)"),
                {"u": uid, "n": name, "e": email, "c": curr, "b": bal},
            )

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    from services.profile.infrastructure import ProfileRepo
    repo = ProfileRepo(session_factory)
    yield {"repo": repo, "engine": engine, "sf": session_factory}
    await engine.dispose()
