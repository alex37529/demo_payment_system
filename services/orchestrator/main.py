"""orchestrator — saga orchestration (phase 3).

HTTP API (port 8002):
    GET /v1/sagas/{id} · GET /v1/sagas/by-payment/{payment_id} · GET /v1/circuit-breakers

Main path: consumer of `payments.commands` (Create) + `ledger.events`
(FundsReserved / FundsDebited / ...), driving the saga state machine, with a
circuit-breaker-protected fraud step, DLQ fallback, and compensation.
The watchdog catches sagas that stall in an in-flight state (ADR-005).
"""
from services.orchestrator.api import attach_orchestrator
from services.shared_app import build_app

app = build_app("orchestrator")
attach_orchestrator(app)
