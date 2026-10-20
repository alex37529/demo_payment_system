"""Shared Kafka Producer/Consumer with retry, exp backoff + jitter, and DLQ (§3.8 TЗ).

aiokafka has no built-in DLQ — after max retries we publish to `<topic>.dlq`
(pitfall #16). `group_id` must be unique per consumer role so `ledger.events`
fans out to Profile AND Notification (pitfall #18).

Note: aiokafka requires a running event loop at construction, so the underlying
client is created lazily inside `start()`.
"""
from __future__ import annotations

import asyncio
import json
import random
from collections.abc import AsyncIterator
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from shared.metrics import inc_dlq, set_consumer_lag
from shared.tracing import inject_headers

#: How often (in records) the consumer lag is sampled. Every record would
#: mean one ``end_offsets`` + ``position`` round-trip per message.
LAG_SAMPLE_EVERY = 50


class KafkaProducer:
    def __init__(self, brokers: str, *, dlq_prefix: str = "") -> None:
        self.brokers = brokers
        self.dlq_prefix = dlq_prefix
        self._p: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._p = AIOKafkaProducer(bootstrap_servers=self.brokers)
        await self._p.start()

    async def stop(self) -> None:
        if self._p:
            await self._p.stop()

    async def publish(
        self,
        topic: str,
        key: str,
        value: dict,
        *,
        max_retries: int = 5,
        base_delay: float = 0.2,
    ) -> None:
        assert self._p is not None, "start() not called"
        body = json.dumps(value).encode()
        keyb = key.encode() if key else None
        # Carry the W3C trace context in the message headers so the consumer
        # can continue the trace (phase-7 pitfall #1 — without this the trace
        # breaks at every Kafka hop and Tempo shows one trace per service).
        headers = inject_headers()
        last_err: Exception | None = None
        for attempt in range(max_retries):
            try:
                await self._p.send_and_wait(topic, key=keyb, value=body, headers=headers)
                return
            except Exception as e:  # noqa: BLE001
                last_err = e
                delay = base_delay * (2 ** attempt)  # exponential backoff
                await asyncio.sleep(delay + random.uniform(0, delay * 0.5))  # + jitter
        dlq = f"{self.dlq_prefix or topic}.dlq"
        inc_dlq(dlq)
        await self._send_nowait(dlq, key, {"original_topic": topic, "value": value,
                                           "error": str(last_err), "attempts": max_retries})
        raise RuntimeError(f"publish to {topic} failed after {max_retries} retries -> {dlq}") from last_err

    async def _send_nowait(self, topic: str, key: str, value: dict) -> None:
        assert self._p is not None
        await self._p.send_and_wait(
            topic,
            key=key.encode() if key else None,
            value=json.dumps(value).encode(),
            headers=inject_headers(),
        )


class KafkaConsumer:
    def __init__(self, brokers: str, group: str, topics: list[str]) -> None:
        self.brokers = brokers
        self.group = group
        self.topics = topics
        self._c: AIOKafkaConsumer | None = None
        self._last_headers: list[tuple[str, bytes]] = []

    async def start(self) -> None:
        self._c = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=self.brokers,
            group_id=self.group,
            enable_auto_commit=True,
            auto_offset_reset="earliest",  # PoC: replay; prod: latest + commit-per-msg
        )
        await self._c.start()

    async def stop(self) -> None:
        if self._c:
            await self._c.stop()

    async def stream(self) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        """Yield ``(key, payload)`` for every record.

        The Kafka message headers (which carry the W3C trace context) are
        available via :meth:`last_headers` right after the yield — the
        consumer span is started by the caller with
        ``shared.tracing.start_consumer_span(...)``.

        The consumer lag is sampled every :data:`LAG_SAMPLE_EVERY` records
        (aiokafka exposes no lag metric — phase-7 pitfall #5). Sampling on
        every record would hammer the broker with ``end_offsets`` calls.
        """
        assert self._c is not None, "start() not called"
        seen = 0
        async for msg in self._c:
            self._last_headers = list(msg.headers or [])
            seen += 1
            if seen % LAG_SAMPLE_EVERY == 0:
                await self._sample_lag()
            yield (msg.key.decode() if msg.key else ""), json.loads(msg.value)

    @property
    def last_headers(self) -> list[tuple[str, bytes]]:
        """Headers of the most recently yielded record (trace context)."""
        return self._last_headers

    async def _sample_lag(self) -> None:
        """Publish the consumer lag (end offset - committed position).

        Best-effort: a broker hiccup must never break the consume loop, so
        any error is swallowed (the metric simply keeps its last value).
        """
        if self._c is None:
            return
        try:
            from aiokafka import TopicPartition

            parts = self._c.assignment()
            if not parts:
                return
            tps = [TopicPartition(t, p) for t, p in parts]
            ends = await self._c.end_offsets(tps)
            positions = await self._c.position(tps[0]) if len(tps) == 1 else None
            if positions is None:
                # Several partitions: sum the per-partition deltas.
                total = 0
                for tp in tps:
                    pos = await self._c.position(tp)
                    total += max(0, int(ends.get(tp, 0)) - int(pos))
                lag = total
            else:
                lag = max(0, int(ends.get(tps[0], 0)) - int(positions))
            for topic in self.topics:
                set_consumer_lag(topic, self.group, lag)
        except Exception:  # noqa: BLE001 — metrics must never break the loop
            return


def unwrap_event(record: dict[str, Any]) -> dict[str, Any]:
    """Flatten a Debezium-flavoured Kafka record to the canonical event dict.

    Debezium + ``io.debezium.transforms.ExtractNewRecordState`` (with our
    outbox table) hands us the **outbox row** as the record value, with the
    actual domain event nested inside the ``payload`` column — and because that
    column is a PG ``JSONB``, it arrives as a **JSON string**:
        ``{"id":1, "topic":"ledger.events", "payload":"{...}", "status":"..."}``

    Return the inner event dict when present (what the consumer cares about);
    otherwise pass the record through so a hand-rolled producer (inline dict, no
    Debezium) degrades gracefully. Pure function — shared by the Profile
    (phase 5) and Notification (phase 5) ``ledger.events`` consumers.
    """
    inner = record.get("payload")
    if isinstance(inner, str):
        try:
            return json.loads(inner)
        except (json.JSONDecodeError, ValueError):
            return record
    if isinstance(inner, dict):
        return inner
    return record
