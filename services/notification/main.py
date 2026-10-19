"""notification — realtime payment feed / SSE (CQRS read-side; phase 5).

Port 8005. Endpoints:
  GET /v1/stream    -> SSE event-stream (fan-out, keepalive 15s, drop-on-full)
  GET /v1/payments  -> the last N payments from the in-memory ring buffer

Live feed is kept by the ``ledger.events`` consumer
(``services/notification/kafka_consumer.py``). The ring buffer is in-memory
(maxlen=1000, spec §4.1) — the moment the process restarts it is empty, which
is acceptable (it is a *recent* window for the UI, not a durable record).
"""
from services.notification.api import attach_notification
from services.shared_app import build_app

app = build_app("notification")
attach_notification(app)
