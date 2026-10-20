"""Custom Prometheus metrics (phase-7 spec §3.2).

The FastAPI instrumentator already exposes the HTTP metrics
(``http_requests_total``, ``http_request_duration_highr_seconds`` …), but the
business signals the SLO / alerts are built on are NOT derivable from them:

* **circuit breaker state** — the only way to know the fraud breaker is OPEN
  (spec §7 check #4: ``pybreaker_open`` must flip to 1 under failure injection);
* **kafka consumer lag** — aiokafka exposes no lag metric (pitfall #5);
* **DLQ depth** — the DLQ topics are real Kafka topics, so their depth has to
  be sampled (spec §5 alert ``DLQDepth``);
* **payment counters** — processed / failed (spec §3.1);
* **SSE connections** — how many browsers are actually attached.

Design notes
------------
* **One module, one import.** Every collector is created at import time on the
  *default* registry (``prometheus_client.REGISTRY``), which is the registry
  the instrumentator exposes at ``/metrics``. Importing this module twice in
  one process would raise "Duplicated timeseries" — so the module is
  imported exactly once (by ``shared.observability``), and the collectors are
  re-exported from there.
* **The ``service`` label.** The instrumentator's default metrics carry
  ``handler``/``method``/``status`` but NOT ``service`` — the dashboard and the
  alert rules are written against ``service="<name>"`` (spec §6.1). Prometheus
  *does* attach the target's ``instance`` label (``orchestrator:8080``), so the
  service name is recoverable — but the queries are much clearer with a
  dedicated ``service`` label, so we add it to our own collectors and to the
  instrumentator's metrics via ``custom_labels``.
* **No ``pybreaker`` import.** The breaker is ``aiobreaker`` (ADR-004); the
  metric keeps the historical ``pybreaker_open`` name because the acceptance
  scripts and the dashboard already refer to it.
"""
from __future__ import annotations

from prometheus_client import Counter, Gauge

#: The service that owns the circuit breaker (e.g. ``orchestrator``). The
#: breaker protects an *external* dependency (fraud), so the label names the
#: OWNER of the breaker, not the protected service.
CB_OWNER = "orchestrator"

#: The dependency the breaker protects — a separate label so several breakers
#: can live in one service without colliding.
CB_PROTECTED = "fraud"


# --------------------------------------------------------------------------- #
# circuit breaker
# --------------------------------------------------------------------------- #
#: 1 = breaker OPEN, 0 = closed. Alert rule: ``pybreaker_open == 1``.
#: Labels: ``owner`` (service holding the breaker) + ``protected`` (dependency).
pybreaker_open = Gauge(
    "pybreaker_open",
    "1 = circuit breaker OPEN, 0 = closed",
    ["owner", "protected"],
)


def set_breaker_state(open_: bool, *, owner: str = CB_OWNER, protected: str = CB_PROTECTED) -> None:
    """Publish the breaker state (called from the aiobreaker state listener)."""
    pybreaker_open.labels(owner=owner, protected=protected).set(1 if open_ else 0)


# --------------------------------------------------------------------------- #
# kafka
# --------------------------------------------------------------------------- #
#: Consumer lag in messages (aiokafka exposes none — pitfall #5).
#: Labels: ``topic`` + ``consumer`` (the consumer group).
kafka_consumer_lag = Gauge(
    "kafka_consumer_lag",
    "Kafka consumer lag in messages",
    ["topic", "consumer"],
)


def set_consumer_lag(topic: str, consumer: str, lag: int) -> None:
    """Publish the lag of one consumer group on one topic."""
    kafka_consumer_lag.labels(topic=topic, consumer=consumer).set(max(0, int(lag)))


# --------------------------------------------------------------------------- #
# DLQ
# --------------------------------------------------------------------------- #
#: Messages sitting in a ``<topic>.dlq`` topic. Labels: ``topic``.
dlq_messages_total = Counter(
    "dlq_messages_total",
    "Messages routed to a DLQ topic (cumulative)",
    ["topic"],
)


def inc_dlq(topic: str, amount: int = 1) -> None:
    """Count a message that was routed to the DLQ."""
    dlq_messages_total.labels(topic=topic).inc(amount)


# --------------------------------------------------------------------------- #
# payments
# --------------------------------------------------------------------------- #
#: Payments that completed the write-side (debit/refund applied).
payments_processed_total = Counter(
    "payments_processed_total",
    "Payments processed by the ledger write-side",
    ["service"],
)

#: Payments that failed. Labels: ``service`` + ``reason``
#: (``fraud_decline`` / ``insufficient_funds`` / ``cb_open`` /
#: ``watchdog_stalled`` / …) + ``step`` (the saga step that failed).
#: The ``step`` label is what makes the graph actionable: it answers "which
#: step did the saga die in" without a second query.
payments_failed_total = Counter(
    "payments_failed_total",
    "Payments that failed",
    ["service", "reason", "step"],
)


def inc_processed(service: str = "ledger", amount: int = 1) -> None:
    """Count a successfully processed payment."""
    payments_processed_total.labels(service=service).inc(amount)


def inc_failed(reason: str, service: str = "orchestrator", step: str = "", amount: int = 1) -> None:
    """Count a failed payment with a stable reason + the saga step."""
    payments_failed_total.labels(service=service, reason=reason, step=step).inc(amount)


# --------------------------------------------------------------------------- #
# SSE
# --------------------------------------------------------------------------- #
#: Currently attached SSE subscribers (notification service).
sse_active_connections = Gauge(
    "sse_active_connections",
    "Active SSE connections",
    ["service"],
)


def set_sse_connections(count: int, service: str = "notification") -> None:
    """Publish the number of attached SSE subscribers."""
    sse_active_connections.labels(service=service).set(max(0, int(count)))


__all__ = [
    "CB_OWNER",
    "CB_PROTECTED",
    "dlq_messages_total",
    "inc_dlq",
    "inc_failed",
    "inc_processed",
    "kafka_consumer_lag",
    "payments_failed_total",
    "payments_processed_total",
    "pybreaker_open",
    "set_breaker_state",
    "set_consumer_lag",
    "set_sse_connections",
    "sse_active_connections",
]
