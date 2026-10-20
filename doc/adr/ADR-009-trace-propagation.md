# ADR-009 — OTel → Tempo + W3C traceparent propagation over Kafka

- Status: accepted
- Date: 2026-10-20
- Deciders: CTO
- Supersedes: (none) — refines tz_v2 §4 (observability) and phase-7 spec §2.2
- Phase: 7 (`feat/08-obs-tests`)

## Context

Phase 1 wired the OpenTelemetry SDK to Tempo: every FastAPI service
instruments its app and exports spans over OTLP/gRPC
(`shared/observability.py`). The dashboard, the alert rules and the
acceptance criteria all assume that **one payment = one trace** that can be
followed from the browser down to the ledger (tz_v2 §4, phase-7 exit
criterion: "Tempo: 1 trace (5+ span) на 1 payment").

But OTel only propagates the trace context over **HTTP**, via the
`traceparent` header. Two hops in this system are not HTTP:

1. **The Kafka hops.** `payments.commands` (intake → orchestrator) and
   `ledger.commands` (orchestrator → ledger) are Kafka messages. The broker
   knows nothing about spans, so without explicit propagation the trace
   **ends at every Kafka hop** — Tempo then shows one trace per service
   instead of one trace per payment, and the acceptance check "a single
   trace_id Gateway → Ledger" (tz_v2 §7 check 3) can never pass.
   This is the phase-7 red flag #1 ("Traces — «рвётся» на Kafka-границе").

2. **The outbound HTTP calls.** The gateway calls intake with a bare
   `httpx.AsyncClient` and the orchestrator calls the fraud API with
   another one. Without HTTP client instrumentation those calls produce no
   client span and carry no `traceparent` — so even the HTTP hop breaks.

The `ledger.events` hop is a special case: those messages are produced by
**Debezium** reading the Postgres WAL (ADR-008), not by our producer, so no
header can be attached to them. That hop is CDC, not a call — it starts a
new trace, and this is accepted (see Consequences).

## Decision

1. **The trace context travels in the Kafka message HEADERS, as the
   standard W3C `traceparent` (+ `baggage`).** `shared/tracing.py` uses the
   OTel propagator, not a hand-rolled format:
   - producer: `propagate.inject(carrier)` → the carrier becomes the
     message headers;
   - consumer: `propagate.extract(carrier)` → the extracted context is the
     **parent** of the consumer span.

   The header shape is aiokafka's contract: `Sequence[tuple[str, bytes]]` —
   the NAME is a `str`, the VALUE is `bytes`. (Getting this wrong makes the
   producer raise `Expected str, got bytes` on every publish.)

2. **Every consumer opens a CONSUMER span around the handling of each
   record.** `shared/tracing.py::start_consumer_span(topic, headers, ...)`
   creates the span with `SpanKind.CONSUMER` and the `messaging.*` attributes
   (`messaging.system`, `messaging.destination.name`,
   `messaging.consumer.group.name`) so a trace is searchable by topic in
   Tempo/Grafana. All four consumers use it: orchestrator (two loops),
   ledger, profile, notification.

3. **Outbound HTTP is instrumented too.**
   `HTTPXClientInstrumentor` in `shared/observability.py` gives the gateway
   and the fraud client proper client spans and injects the `traceparent`
   header — so the browser → gateway → intake → orchestrator → fraud chain
   is one trace.

4. **A message without headers starts a new trace — it is never attached to
   a random parent.** `extract_context` on an empty header set yields the
   empty context, so the consumer span is a root. Fabricating a parent
   would be a lie; an untraced message (the CDC hop) is simply untraced.

5. **The trace id is also written into the JSON log line.** The request
   middleware puts the current span's `trace_id` in every log record, so
   Grafana can jump from a span to the logs of that request (the
   "click a span → see the logs" flow), and the E2E suite can find a
   payment's trace by reading the intake's log line.

## Consequences

**Positive:**
- One payment = one trace, from the browser to the ledger (and to the fraud
  API). The acceptance check "single trace_id Gateway → Ledger" becomes
  satisfiable, and a slow payment can be diagnosed span by span.
- The propagation format is the W3C standard, so any other OTel-based
  service can join the trace.
- The `messaging.*` attributes make a trace searchable by topic/group
  instead of only by trace id.

**Negative / accepted cost:**
- The CDC hop (`ledger.events`) is **not** linked to the write-side trace
  (Debezium produces those messages). A `traceparent` column on the
  outbox row would close the gap, at the cost of a schema change + a
  connector reload — deferred.
- Every message now carries ~2 extra headers (a few dozen bytes).
- The producer pays the cost of `propagate.inject` on every publish
  (negligible: a dict copy).
- Trace continuity is now a **cross-service contract**: a consumer that
  forgets to open a consumer span silently breaks the chain. The E2E test
  (`tests/e2e/test_e2e_chain.py::test_trace_is_continuous_across_kafka`)
  is the guard against that.

## Alternatives considered

- **Put the trace id in the message VALUE (the payload).** Rejected: the
  payload is the canonical domain event (`shared/domain/events.py`) — a
  transport concern must not leak into the event contract, and every
  producer/consumer would have to agree on the field.
- **A dedicated correlation-id field instead of W3C `traceparent`.**
  Rejected: `traceparent` is the standard, carries the span id as well as
  the trace id, and is understood by every OTel tool (Tempo, Grafana, any
  other service) without a custom mapping.
- **Trace everything through the CDC hop by adding a `traceparent` column
  to `outbox_events` now.** Deferred, not rejected — it is the natural
  follow-up if the CDC hop ever needs to be part of the same trace.
- **Instrument Kafka with a third-party OTel instrumentation library.**
  Rejected for a PoC: another dependency, and our own `shared/kafka.py` is
  the single seam through which every message flows — the propagation is
  ~40 lines and fully under our control.
