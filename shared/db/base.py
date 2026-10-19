"""Async SQLAlchemy 2.0 base: DeclarativeBase + engine/session factory helpers."""
from __future__ import annotations

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Common declarative base for all services (shared models)."""


def make_engine(dsn: str, *, echo: bool = False) -> AsyncEngine:
    return create_async_engine(dsn, echo=echo, pool_size=10, max_overflow=20)


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    # expire_on_commit=False is REQUIRED for async: avoids DetachedInstanceError
    # when reading mapped attributes after commit (pitfall #13).
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
