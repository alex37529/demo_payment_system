"""Trace-context propagation across Kafka (phase-7 spec §2.2, pitfall #1).

These are the tests that would have caught the "Expected str, got bytes"
bug: the header shape is part of aiokafka's contract, and getting it wrong
breaks EVERY publish (the intake returns 503 and no payment is ever
accepted) — which is far more expensive to find than to test.
"""
from __future__ import annotations

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from shared.tracing import (
    BAGGAGE_HEADER,
    TRACEPARENT_HEADER,
    extract_context,
    inject_headers,
    start_consumer_span,
)

# The global TracerProvider can only be set ONCE per process (OTel refuses to
# override it), so the whole module shares one provider + one exporter, and
# the exporter is cleared between tests.
_provider = TracerProvider()
_exporter = InMemorySpanExporter()
_provider.add_span_processor(SimpleSpanProcessor(_exporter))
trace.set_tracer_provider(_provider)

_tracer = _provider.get_tracer("test")


@pytest.fixture(autouse=True)
def _clear_spans():
    _exporter.clear()
    yield


def test_inject_headers_shape_is_str_name_bytes_value():
    """aiokafka types headers as ``Sequence[tuple[str, bytes]]``.

    A bytes NAME makes the producer raise "Expected str, got bytes" -> the
    publish fails -> the whole payment path is dead. Pinned here.
    """
    with _tracer.start_as_current_span("produce"):
        headers = inject_headers()

    assert headers, "an active span must produce headers"
    for name, value in headers:
        assert isinstance(name, str), f"header name must be str, got {type(name)}"
        assert isinstance(value, bytes), f"header value must be bytes, got {type(value)}"
    names = {n for n, _ in headers}
    assert TRACEPARENT_HEADER in names


def test_inject_headers_without_active_span_is_empty():
    """No active span -> no headers (never fabricate a parent)."""
    span = _tracer.start_span("detached")
    try:
        assert inject_headers() == []
    finally:
        span.end()


def test_extract_context_roundtrip_preserves_trace_id():
    """A header set produced by inject_headers must rebuild the SAME trace id."""
    with _tracer.start_as_current_span("produce") as span:
        expected = span.get_span_context().trace_id
        headers = inject_headers()

    ctx = extract_context(headers)
    got = trace.get_current_span(ctx).get_span_context().trace_id
    assert got == expected, "the trace id must survive the Kafka round-trip"


def test_consumer_span_is_child_of_producer_span():
    """The consumer span must be a CHILD of the producer's span — that is
    what makes Tempo show ONE trace across a Kafka hop instead of two."""
    with _tracer.start_as_current_span("produce") as producer:
        producer_trace_id = producer.get_span_context().trace_id
        headers = inject_headers()

    with start_consumer_span("consume", headers, topic="t", group="g"):
        pass

    spans = _exporter.get_finished_spans()
    names = [s.name for s in spans]
    assert "produce" in names and "consume" in names
    consumer = next(s for s in spans if s.name == "consume")
    assert consumer.context.trace_id == producer_trace_id, \
        "the consumer span must live in the producer's trace"
    assert consumer.parent is not None, "the consumer span must have a parent"


def test_consumer_span_without_headers_is_a_new_root():
    """A message without trace headers must NOT be attached to a random
    parent — it simply starts its own trace."""
    with start_consumer_span("consume", [], topic="t"):
        pass

    consumer = next(s for s in _exporter.get_finished_spans() if s.name == "consume")
    assert consumer.parent is None or not consumer.parent.is_valid


def test_consumer_span_carries_messaging_attributes():
    """Tempo/Grafana need the messaging attributes to make a trace
    searchable by topic."""
    with start_consumer_span("consume", [], topic="payments.commands", group="g1"):
        pass

    consumer = next(s for s in _exporter.get_finished_spans() if s.name == "consume")
    assert consumer.attributes["messaging.system"] == "kafka"
    assert consumer.attributes["messaging.destination.name"] == "payments.commands"
    assert consumer.attributes["messaging.consumer.group.name"] == "g1"


def test_baggage_header_name_is_exported():
    """The baggage header is part of the W3C context we propagate."""
    assert BAGGAGE_HEADER == "baggage"
