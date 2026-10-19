"""Notification consumer: ``ledger.events`` -> PaymentLog ring + SSE broadcast.

Wires the shared Debezium ``unwrap_event`` to two read-side sinks:

  * ``PaymentLog.push``  (last N payment events, newest-first — UI feed, spec §4.1)
  * ``SSERelay.broadcast``  (push to the currently-subscribed SSE clients, spec §4.2)

Group id ``notification-feed`` is distinct from ``profile-balance`` so the
single-partition topic fans out to BOTH read-side readers (shared/kafka.py,
pitfall #18: different groups, not split partitions).
"""
from __future__ import annotations

import logging
from typing import Any

from services.notification.ring import PaymentLog
from services.notification.sse import SSERelay, shape_hint
from shared.kafka import KafkaConsumer, unwrap_event

log = logging.getLogger(__name__)

TOPIC = "ledger.events"
GROUP = "notification-feed"


def to_payment(record: dict[str, Any]) -> dict[str, Any]:
    """Normalize a ``ledger.events`` event to the feed dict (spec §4.1).

    Accepts either the raw Debezium envelope or an already-unwrapped canonical
    event (it unwraps defensively, so tests / consumers can hand it either).
    """
    ev = unwrap_event(record)
    ts = ev.get("ts")
    if ts is not None and not isinstance(ts, (int, float)) and hasattr(ts, "isoformat"):
        ts = ts.isoformat()
    return {
        "payment_id": ev.get("payment_id") or ev.get("event_id") or "",
        "account_id": ev.get("account_id") or "",
        "event": shape_hint(ev) or (ev.get("event") or ""),
        "amount": ev.get("amount"),
        "new_balance": ev.get("new_balance"),
        "ts": ts or "",
        "event_id": ev.get("event_id") or "",
    }


async def run_consumer(
    *,
    brokers: str,
    plog: PaymentLog,
    relay: SSERelay,
) -> None:
    consumer = KafkaConsumer(brokers, GROUP, [TOPIC])
    await consumer.start()
    try:
        async for key, raw in consumer.stream():
            event = unwrap_event(raw)
            payment = to_payment(event)
            plog.push(payment)
            relay.broadcast(payment)  # non-blocking fan-out to N subscribers
            log.info("notification: feed %s (len=%d, subs=%d)",
                     payment.get("payment_id") or key, len(plog), relay.subscriber_count)
    finally:
        await consumer.stop()
