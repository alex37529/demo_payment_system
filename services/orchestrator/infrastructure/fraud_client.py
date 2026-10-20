"""Fraud-check HTTP client for the Orchestrator (CB-protected).

Uses `shared.circuit.CircuitBreakerHttpClient` (aiobreaker). The **fallback is
pure logging** (returns a neutral "still pending" decision); the *business*
decision to DLQ vs. continue is made by `SagaHandler._fraud_outcome`, not here.
That keeps the CB fallback side-effect-free (no DLQ publish from inside the
fallback) — the spec's "CB open -> payment to DLQ" is honored by the handler,
and a pure fallback means the breaker never blocks on a network call.

`fail_max=3`, `reset_timeout=10s` (tighter than the phase-3 sketch's 5/30 —
pitfall #6: we want the breaker to open quickly under failure injection and
half-open-probe promptly). `timeout` must stay < the mock-fraud `timeout`
failure-injection window (5s) so an injected timeout counts as a breaker
failure (ADR-004); default 2.5s.
"""
from __future__ import annotations

import logging
from typing import Any

from shared.circuit import CircuitBreakerHttpClient
from shared.metrics import CB_OWNER, CB_PROTECTED

log = logging.getLogger(__name__)


async def _fraud_fallback(url: str, payload: dict) -> dict:
    """Circuit OPEN (or a hard HTTP error): do NOT spend time, do NOT DLQ here.

    Returns a decision the handler treats as "cannot verify" — the handler then
    routes the saga to the DLQ (failure path) *or* logs-and-continues per its
    own policy. Keeping this pure (no I/O) is the whole point of the fallback.
    """
    log.warning(
        "fraud CB fallback (open/error) payment_id=%s reason=circuit_or_http",
        payload.get("payment_id"),
    )
    return {"decision": "route_to_dlq", "score": 100.0, "reason": "circuit_open_or_error"}


def make_fraud_client(
    base_url: str,
    *,
    fail_max: int = 3,
    reset_timeout: float = 10.0,
    timeout: float = 2.5,
) -> tuple[CircuitBreakerHttpClient, str]:
    """Return ``(client, check_url)`. `base_url` defaults to the mock-fraud service."""
    base = (base_url or "http://mock-fraud:8080").rstrip("/")
    client = CircuitBreakerHttpClient(
        fallback=_fraud_fallback,
        fail_max=fail_max,
        reset_timeout=reset_timeout,
        timeout=timeout,
        # Prometheus labels for the breaker gauge: `owner` = the service that
        # HOLDS the breaker, `protected` = the dependency it guards.
        owner=CB_OWNER,
        protected=CB_PROTECTED,
    )
    return client, f"{base}/fraud/check"


def fraud_request(payload: dict, *, account_id: str = "", amount: Any = None) -> dict:
    """Body for `POST /fraud/check` (mock-fraud, tz_v2 §2.3)."""
    return {
        "payment_id": payload.get("payment_id", ""),
        "account_id": account_id or payload.get("account_id", ""),
        "amount": amount if amount is not None else payload.get("amount"),
    }
