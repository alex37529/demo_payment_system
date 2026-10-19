"""Notification HTTP API (port 8005): SSE feed + latest payments.

  GET /v1/stream     -> SSE stream (keepalive 15s, fan-out, drop-on-full)
  GET /v1/payments   -> the ring buffer's latest N payments (no SSE required)

Wiring mirrors the ledger/intake ``attach_*`` pattern: lifespan starts the
Kafka consumer (group ``notification-feed``) and the relay/``PaymentLog`` are
created once and exposed on ``app.state`` for the SSE route.
"""
from __future__ import annotations

import asyncio
import logging
import os

from fastapi import FastAPI, Query
from starlette.responses import StreamingResponse

from services.notification.ring import PaymentLog
from services.notification.sse import SSERelay, SSEStream

log = logging.getLogger(__name__)

KEEPALIVE_S = float(os.environ.get("SSE_HEARTBEAT_S", "15"))
DEFAULT_LIMIT = 20


def attach_notification(
    app: FastAPI,
    *,
    plog: PaymentLog | None = None,
    relay: SSERelay | None = None,
) -> None:
    from contextlib import asynccontextmanager

    plog = plog or PaymentLog()
    relay = relay or SSERelay()
    app.state.notification_plog = plog
    app.state.notification_relay = relay

    _register_routes(app, plog, relay)

    @asynccontextmanager
    async def _noti_lifespan(_app: FastAPI):
        # Start the `ledger.events` consumer so the relay + plog are live
        # for the SSE subscribers (spec §4.1: consumer listens, fan-out to
        # N subscribers). No DB, no engine — pure in-memory state.
        consumer_task: asyncio.Task | None = None
        if _consumer_enabled():
            from services.notification import kafka_consumer
            brokers = os.environ.get("KAFKA_BROKERS", "kafka:9092")
            consumer_task = asyncio.create_task(
                kafka_consumer.run_consumer(brokers=brokers, plog=plog, relay=relay),
                name="notification-events")
            log.info("notification ready (consumer=%s, broker=%s)", True, brokers)
        else:
            log.info("notification ready (consumer=%s)", _consumer_enabled())
        try:
            yield
        finally:
            if consumer_task is not None and not consumer_task.done():
                consumer_task.cancel()
            # Unsubscribe is per-connection (SSEStream.__aiter__ finally);
            # no global cleanup needed here.
            log.info("notification shutdown complete")

    app.router.lifespan_context = _noti_lifespan


def _consumer_enabled() -> bool:
    return os.environ.get("NOTIFICATION_ENABLE_CONSUMER", "1") not in ("0", "false")


async def start(app: FastAPI) -> None:
    """Manually trigger the startup portion (used by the API tests)."""
    lf = app.router.lifespan_context
    cm = lf(app)
    try:
        await cm.__aenter__()
    except Exception:
        await cm.__aexit__(None, None, None)
        raise
    app.state.notification_lf = cm


async def stop(app: FastAPI) -> None:
    cm = getattr(app.state, "notification_lf", None)
    if cm is not None:
        await cm.__aexit__(None, None, None)


def _register_routes(app: FastAPI, plog: PaymentLog, relay: SSERelay) -> None:
    @app.get("/v1/stream")
    async def sse_stream():
        q = relay.subscribe()
        stream = SSEStream(q, relay, heartbeat_s=KEEPALIVE_S)
        return StreamingResponse(
            stream.frames(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",   # pitfall #8: nginx must not buffer
            },
        )

    @app.get("/v1/payments")
    async def latest_payments(limit: int = Query(default=DEFAULT_LIMIT, ge=1, le=1000)):
        """The newest ``limit`` payments in the ring buffer (no SSE needed)."""
        return {"count": len(plog), "payments": plog.latest(limit)}
