"""Outbox Delivery Tracker — closes the Phase-2 gap on `outbox_events.status`.

Phase 2 left the `status` / `published_at` / `last_error` column trio in place
and documented ``PUBLISHED`` / ``FAILED`` but shipped no component that would
ever flip them. This tracker closes that gap inside Phase 4 (Intake) so the
outbox lifecycle is complete before we move to read-side work.

Mechanism
---------
For each `outbox_events` row that is still ``pending``, we (1) ask Kafka for
recent `event_id`s on the target topic (via a short read of the most recent
records), and (2) update the row:

* if the row's `event_id` is in the set of delivered `event_id`s
  → ``status='published'``, ``published_at=now()``, ``last_error=NULL``;
* elif the row is older than ``horizon_seconds`` (default 5min) and still not
  in the set → ``status='failed'``, ``last_error='no delivery after Ns'``;
* else → stays ``pending`` (Dezium has not yet picked it up).

The tracker is a **background loop** (started at app start) that runs every
``interval`` seconds. A manual endpoint (``POST /v1/outbox/sweep``) lets a dev
force a sweep. Both share `sweep_once()` so behaviour is identical.

The component is **intentionally simple**: it only looks at "recent records
on the topic" — not a full WAL-scan or offset bookkeeping. That is enough for
this PoC; a production-grade one could be added in Phase 7-8.

Testing
-------
Tests bypass real Kafka: they monkeypatch `DeliveryTracker._detect_delivered_
event_ids` to return a fixed set of event_ids, then drive `sweep_once()` and
assert the DB mutations.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime

from sqlalchemy import select

from shared.outbox import (
    FAILED,
    PENDING,
    PUBLISHED,
    OutboxEvent,
)

log = logging.getLogger(__name__)


DEFAULT_INTERVAL_S = 5.0
DEFAULT_HORIZON_S = 300.0     # 5 min pending → treat as failed
DEFAULT_MAX_ROWS_PER_SWEEP = 200
DEFAULT_POLL_BATCH = 200
DEFAULT_POLL_MS = 1500


class DeliveryTracker:
    """Flips `pending` outbox rows to `published` / `failed` based on Kafka."""

    def __init__(
        self,
        *,
        brokers: str,
        session_factory,            # async_sessionmaker[AsyncSession]
        topic: str = "ledger.events",
        poll_batch: int = DEFAULT_POLL_BATCH,
        poll_ms: int = DEFAULT_POLL_MS,
    ) -> None:
        self.brokers = brokers
        self.sf = session_factory
        self.topic = topic
        self.poll_batch = poll_batch
        self.poll_ms = poll_ms

    # ------------------------------------------------------------------ #
    # public API
    # ------------------------------------------------------------------ #
    async def run(
        self,
        *,
        interval: float = DEFAULT_INTERVAL_S,
        horizon_seconds: float = DEFAULT_HORIZON_S,
        max_iters: int | None = None,
    ) -> None:
        """Background loop. `max_iters` is for tests / one-shot sweeps."""
        iters = 0
        while True:
            iters += 1
            try:
                counts = await self.sweep_once(horizon_seconds=horizon_seconds)
                if any(counts.values()):
                    log.info("delivery sweep #%d: %s", iters, counts)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("delivery sweep #%d failed", iters)
            if max_iters is not None and iters >= max_iters:
                break
            await asyncio.sleep(interval)

    async def sweep_once(self, *, horizon_seconds: float = DEFAULT_HORIZON_S) -> dict:
        """One pass: detect delivered, update DB. Returns counters."""
        now = datetime.now(UTC)
        seen = await self._detect_delivered_event_ids()

        counts = {"examined": 0, "published": 0, "failed": 0, "kept": 0}
        async with self.sf() as session:
            rows = (await session.execute(
                select(OutboxEvent)
                .where(OutboxEvent.status == PENDING)
                .order_by(OutboxEvent.created_at.desc())
                .limit(DEFAULT_MAX_ROWS_PER_SWEEP)
            )).scalars().all()

            for row in rows:
                counts["examined"] += 1
                if row.event_id in seen:
                    row.status = PUBLISHED
                    row.published_at = now
                    row.last_error = None
                    counts["published"] += 1
                    continue
                age_s = self._age_seconds(row.created_at, now)
                if age_s is not None and age_s >= horizon_seconds:
                    row.status = FAILED
                    row.last_error = f"no delivery after {int(age_s)}s (topic={self.topic})"
                    counts["failed"] += 1
                    continue
                counts["kept"] += 1
            await session.commit()
        return counts

    @staticmethod
    def _age_seconds(created_at: datetime, now: datetime) -> float | None:
        """Age in seconds, tolerant of naive vs tz-aware stored timestamps."""
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=UTC)
        return (now - created_at).total_seconds()

    # ------------------------------------------------------------------ #
    # internal
    # ------------------------------------------------------------------ #
    async def _detect_delivered_event_ids(self) -> set[str]:
        """Look at the last `poll_batch` records on `self.topic` and collect
        their `event_id`. Best-effort: if Kafka is unreachable or the read
        times out, returns an empty set (the sweep will then treat everything
        as 'not yet delivered', which is conservative and safe).

        Tests monkeypatch this to return a fixed set without touching Kafka.
        """
        from aiokafka import AIOKafkaConsumer, TopicPartition

        event_ids: set[str] = set()
        consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.brokers,
            group_id="outbox-tracker",
            enable_auto_commit=False,
            max_poll_records=self.poll_batch,
        )
        try:
            await consumer.start()
            tps = [TopicPartition(self.topic, p)
                   for p in (await consumer.partitions_for_topic(self.topic) or (0,))]
            await consumer.assign(tps)
            ends = await consumer.end_offsets(tps)           # dict[TopicPartition, int]
            for tp in tps:
                await consumer.seek(tp, max(0, int(ends[tp]) - self.poll_batch))
            deadline = time.monotonic() + self.poll_ms / 1000.0
            while time.monotonic() < deadline:
                remaining_ms = max(5.0, (deadline - time.monotonic()) * 1000.0)
                batches = await consumer.getmany(timeout_ms=int(remaining_ms),
                                                 max_records=self.poll_batch)
                for msgs in batches.values():
                    for m in msgs:
                        eid = self._extract_event_id(m.value)
                        if eid:
                            event_ids.add(eid)
                if event_ids:            # early-exit once we have some evidence
                    break
        except Exception as e:  # noqa: BLE001 — treat as "no evidence" (safe)
            log.warning("delivery tracker: Kafka poll failed (%s); assuming none delivered", e)
            return set()
        finally:
            await consumer.stop()
        return event_ids

    @staticmethod
    def _extract_event_id(value: object) -> str | None:
        """Debezium's record value is the outbox row: `{event_id, ...}` or a
        JSON-encoded version of it. Handle both."""
        if isinstance(value, (bytes, bytearray)):
            import json as _json
            try:
                value = _json.loads(value)
            except (ValueError, _json.JSONDecodeError):
                return None
        if not isinstance(value, dict):
            return None
        if "event_id" in value:
            return str(value["event_id"])
        # nested under `payload` (rare: our orchestrator already unwraps this).
        inner = value.get("payload")
        if isinstance(inner, str):
            import json as _json
            try:
                inner = _json.loads(inner)
            except (ValueError, _json.JSONDecodeError):
                inner = None
        if isinstance(inner, dict) and "event_id" in inner:
            return str(inner["event_id"])
        return None

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #
    @staticmethod
    def _now() -> datetime:
        return datetime.now(UTC)

    # (intentionally no `close()` — we hold no persistent resources)
