"""W3C trace-context propagation across Kafka (phase-7 spec §2.2, pitfall #1).

**The problem.** HTTP propagates the trace context for free: the client puts a
``traceparent`` header on the request, the server's OTel middleware picks it
up and makes its span a *child* of the caller's. Kafka has no such thing — a
message is just bytes plus headers, and nothing in the broker knows about
spans. Without explicit propagation the trace **breaks at every Kafka hop**:

    Gateway → Intake → payments.commands → Orchestrator → ledger.commands → Ledger
                            ^^^^^^^^^^^^^^                          ^^^^^^^^^^^^^^
                            trace ends here                          trace ends here

so Tempo shows a *different* trace per service instead of one trace per
payment. The spec's exit criterion is explicit: "Tempo: 1 trace (5+ span) на
1 payment" — that is impossible without this module.

**The fix.** Carry the W3C ``traceparent`` (and ``baggage``) in the *Kafka
message headers*:

* producer: read the currently-active span and inject its context into the
  outgoing message headers;
* consumer: extract the context from the incoming headers and start its span
  as a child of it.

Both sides use the OTel propagator, so the format is the standard W3C
``traceparent`` header — any other OTel-based service can join the trace.

Note on Debezium: the write-side events reach Kafka through **Debezium** (it
reads the Postgres WAL), not through our producer, so the traceparent cannot
be attached to those messages at publish time. The ledger's consumer therefore
starts a *new* trace for the events it reads back from ``ledger.events`` —
that hop is CDC, not a call. The synchronous call chain (the one the spec
measures: Gateway → Intake → Orchestrator → Ledger) is fully linked.
"""
from __future__ import annotations

from typing import Any

from opentelemetry import context as otel_context
from opentelemetry import propagate
from opentelemetry.trace import SpanKind, get_tracer

#: Kafka header carrying the W3C trace context (``b"traceparent"`` +
#: ``b"baggage"``). Kafka headers are byte strings, hence the ``b`` prefix.
TRACEPARENT_HEADER = "traceparent"
BAGGAGE_HEADER = "baggage"

_tracer = get_tracer("fintech.kafka")


def inject_headers() -> list[tuple[bytes, bytes]]:
    """Build the Kafka headers carrying the current trace context.

    Returns an empty list when no span is active (e.g. a background task with
    no parent) — the message is then simply untraced, which is correct: an
    untraced message must not fabricate a parent.
    """
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    headers: list[tuple[bytes, bytes]] = []
    for name, value in carrier.items():
        headers.append((name.encode("utf-8"), value.encode("utf-8")))
    return headers


def extract_context(headers: list[tuple[bytes, bytes]] | None) -> otel_context.Context:
    """Rebuild the remote trace context from Kafka message headers.

    An empty / missing header set yields the *current* (usually empty) context
    — the consumer span then becomes a new root, which is the honest outcome
    for a message that was never traced.
    """
    carrier: dict[str, str] = {}
    for name, value in headers or []:
        if isinstance(name, bytes):
            name = name.decode("utf-8", "replace")
        if isinstance(value, bytes):
            value = value.decode("utf-8", "replace")
        carrier[str(name)] = str(value)
    return propagate.extract(carrier)


def start_consumer_span(
    name: str,
    headers: list[tuple[bytes, bytes]] | None,
    *,
    topic: str = "",
    group: str = "",
    attributes: dict[str, Any] | None = None,
):
    """Start a CONSUMER span parented to the producer's span.

    Returns the span. Use it as a context manager::

        with start_consumer_span("consume payments.commands", msg.headers):
            ...

    The span is created with ``SpanKind.CONSUMER`` so Tempo/Grafana render it
    as a consumer step, and carries the messaging attributes that make the
    trace searchable by topic.
    """
    ctx = extract_context(headers)
    attrs: dict[str, Any] = {
        "messaging.system": "kafka",
        "messaging.operation.type": "process",
    }
    if topic:
        attrs["messaging.destination.name"] = topic
    if group:
        attrs["messaging.consumer.group.name"] = group
    attrs.update(attributes or {})
    return _tracer.start_as_current_span(
        name, context=ctx, kind=SpanKind.CONSUMER, attributes=attrs
    )


__all__ = [
    "BAGGAGE_HEADER",
    "TRACEPARENT_HEADER",
    "extract_context",
    "inject_headers",
    "start_consumer_span",
]
