"""ledger — write-side + Transactional Outbox (phase 2).

HTTP API (port 8003):
  POST /v1/accounts/{id}/debit
  POST /v1/accounts/{id}/reserve
  POST /v1/accounts/{id}/refund
  POST /v1/accounts/{id}/cancel-reservation
  GET  /v1/accounts, /v1/accounts/{id}

Main path (production): `ledger.commands` Kafka consumer -> use-cases (same code).
"""
from __future__ import annotations

from services.ledger.api import attach_ledger
from services.shared_app import build_app

app = build_app("ledger")
attach_ledger(app)
