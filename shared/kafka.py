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
        last_err: Exception | None = None
        for attempt in range(max_retries):
            try:
                await self._p.send_and_wait(topic, key=keyb, value=body)
                return
            except Exception as e:  # noqa: BLE001
                last_err = e
                delay = base_delay * (2 ** attempt)  # exponential backoff
                await asyncio.sleep(delay + random.uniform(0, delay * 0.5))  # + jitter
        dlq = f"{self.dlq_prefix or topic}.dlq"
        await self._send_nowait(dlq, key, {"original_topic": topic, "value": value,
                                           "error": str(last_err), "attempts": max_retries})
        raise RuntimeError(f"publish to {topic} failed after {max_retries} retries -> {dlq}") from last_err

    async def _send_nowait(self, topic: str, key: str, value: dict) -> None:
        assert self._p is not None
        await self._p.send_and_wait(
            topic, key=key.encode() if key else None, value=json.dumps(value).encode()
        )


class KafkaConsumer:
    def __init__(self, brokers: str, group: str, topics: list[str]) -> None:
        self.brokers = brokers
        self.group = group
        self.topics = topics
        self._c: AIOKafkaConsumer | None = None

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
        assert self._c is not None, "start() not called"
        async for msg in self._c:
            yield (msg.key.decode() if msg.key else ""), json.loads(msg.value)
