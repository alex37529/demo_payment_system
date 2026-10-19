"""Notification test fixtures (hermetic: no Kafka / no broker)."""
from __future__ import annotations

import os

import pytest_asyncio

# Hermetic: no consumer task, no OTel hookup.
os.environ["NOTIFICATION_ENABLE_CONSUMER"] = "0"
os.environ.setdefault("OTEL_EXPORTER_OTLP_ENDPOINT", "")
os.environ.setdefault("SSE_HEARTBEAT_S", "0.2")

from httpx import ASGITransport, AsyncClient

from services.notification.api import attach_notification, start, stop
from services.shared_app import build_app


@pytest_asyncio.fixture
async def notif():
    app = build_app("notification-test")
    attach_notification(app)
    await start(app)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as http:
        yield {
            "app": app,
            "http": http,
            "plog": app.state.notification_plog,
            "relay": app.state.notification_relay,
        }
    await stop(app)
