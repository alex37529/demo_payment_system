"""intake — payment intake + idempotency (phase 4).

HTTP API (port 8001):
    POST /v1/payments            — create a payment (Idempotency-Key required)
    GET  /v1/payments/{payment_id} — cached accepted payment (replay-safe)

Main path: validate the DTO → `Idempotency-Key` gate (Redis `SET NX` + PG
durable mirror, dual-write) → publish `payments.commands` (Create) → cache the
response for idempotent replay. On Kafka failure the key is rolled back and the
client gets a 503 so it can retry (no silent duplicate, no wedged key).
"""
from services.intake.api import attach_intake
from services.shared_app import build_app

app = build_app("intake")
attach_intake(app)
