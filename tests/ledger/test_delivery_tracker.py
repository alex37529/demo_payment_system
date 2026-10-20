"""Outbox Delivery Tracker tests — closes the Phase-2 `outbox_events.status` gap.

Verifies the three transitions `sweep_once()` can produce, without touching
real Kafka (we monkeypatch `_detect_delivered_event_ids` to return a fixed set):
  * pending + seen        -> published (published_at set, last_error cleared);
  * pending + old + not seen -> failed (last_error filled);
  * pending + recent + not seen -> stays pending.
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from services.ledger.application import DeliveryTracker
from shared.db.base import Base
from shared.outbox import FAILED, PENDING, PUBLISHED, OutboxEvent


@pytest_asyncio.fixture
async def env():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as c:
        await c.run_sync(Base.metadata.create_all)
    sf = async_sessionmaker(engine, expire_on_commit=False)
    yield sf
    await engine.dispose()


def _event_id(seed: str) -> str:
    import uuid

    return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))


async def _insert(sf, *, when: datetime, seen: bool) -> OutboxEvent:
    eid = _event_id(f"e-{seen}-{when.timestamp()}")
    async with sf() as s:
        row = OutboxEvent(event={"event_id": eid, "x": 1}, topic="ledger.events")
        row.created_at = when
        s.add(row)
        await s.commit()
    return row


async def _get(sf, eid: str) -> OutboxEvent:
    async with sf() as s:
        r = (await s.execute(select(OutboxEvent).where(OutboxEvent.event_id == eid))).scalars().one()
    return r


async def test_seen_becomes_published(env):
    sf = env
    now = datetime.now(UTC)
    row = await _insert(sf, when=now - timedelta(seconds=5), seen=True)
    tracker = DeliveryTracker(brokers="x", session_factory=sf, topic="ledger.events")
    tracker._detect_delivered_event_ids = _stub_for(frozenset({row.event_id}))
    counts = await tracker.sweep_once(horizon_seconds=300)
    assert counts["published"] == 1 and counts["failed"] == 0
    again = await _get(sf, row.event_id)
    assert again.status == PUBLISHED
    assert again.published_at is not None
    assert again.last_error is None


async def test_old_unseen_becomes_failed(env):
    sf = env
    now = datetime.now(UTC)
    row = await _insert(sf, when=now - timedelta(minutes=10), seen=False)
    tracker = DeliveryTracker(brokers="x", session_factory=sf, topic="ledger.events")
    tracker._detect_delivered_event_ids = _stub_for(frozenset())
    counts = await tracker.sweep_once(horizon_seconds=60)
    assert counts["failed"] == 1 and counts["published"] == 0
    again = await _get(sf, row.event_id)
    assert again.status == FAILED
    assert again.last_error is not None and "no delivery" in again.last_error
    assert again.published_at is None


async def test_recent_unseen_stays_pending(env):
    sf = env
    now = datetime.now(UTC)
    row = await _insert(sf, when=now - timedelta(seconds=1), seen=False)
    tracker = DeliveryTracker(brokers="x", session_factory=sf, topic="ledger.events")
    tracker._detect_delivered_event_ids = _stub_for(frozenset())
    counts = await tracker.sweep_once(horizon_seconds=300)
    assert counts["kept"] == 1 and counts["published"] == 0 and counts["failed"] == 0
    again = await _get(sf, row.event_id)
    assert again.status == PENDING


async def test_sweep_only_touches_pending_rows(env):
    sf = env
    now = datetime.now(UTC)
    fresh = await _insert(sf, when=now - timedelta(seconds=1), seen=True)
    # pre-mark the fresh row as PUBLISHED directly to simulate "already handled".
    async with sf() as s:
        r = (await s.execute(select(OutboxEvent).where(OutboxEvent.event_id == fresh.event_id))).scalars().one()
        r.status = PUBLISHED
        r.published_at = now
        await s.commit()
    tracker = DeliveryTracker(brokers="x", session_factory=sf, topic="ledger.events")
    tracker._detect_delivered_event_ids = _stub_for(frozenset())
    counts = await tracker.sweep_once(horizon_seconds=300)
    # nothing to do: the only row is already published, not pending.
    assert counts["examined"] == 0


def _stub_for(seen: frozenset[str] | None = None) -> Callable[[], Awaitable[set[str]]]:
    """Return an async callable that yields the fixed `seen` set — tests
    monkeypatch `tracker._detect_delivered_event_ids` with it, so no real
    Kafka is touched."""
    fixed = set(seen) if seen is not None else set()

    async def fake() -> set[str]:
        return fixed

    return fake


async def test_tracker_uses_injected_detection():
    """Guard: overridden detection returns the fixed set (no real Kafka)."""
    tracker = DeliveryTracker(brokers="x", session_factory=object(), topic="ledger.events")
    tracker._detect_delivered_event_ids = _stub_for(frozenset({"a", "b"}))
    assert await tracker._detect_delivered_event_ids() == {"a", "b"}


def test_aiokafka_consumer_signature_matches_delivery_tracker_usage():
    """Guard: the three methods `_detect_delivered_event_ids` calls as SYNCHRONOUS
    (no `await`) MUST remain plain methods — if a future aiokafka makes them
    coroutines, the delivery tracker silently goes into the `except` branch
    (and marks all outbox rows `failed`). Pin the signature to catch that.
    """
    import inspect

    from aiokafka import AIOKafkaConsumer

    for name in ("partitions_for_topic", "assign", "seek"):
        attr = getattr(AIOKafkaConsumer, name)
        assert not inspect.iscoroutinefunction(attr), f"AIOKafkaConsumer.{name} became async — delivery_tracker needs a fix"

    # And the ones we DO await must still be async — pin those too.
    for name in ("start", "stop", "end_offsets", "getmany"):
        attr = getattr(AIOKafkaConsumer, name)
        assert inspect.iscoroutinefunction(attr), f"AIOKafkaConsumer.{name} became sync — delivery_tracker needs a fix"
