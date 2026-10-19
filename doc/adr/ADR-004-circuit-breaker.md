# ADR-004 — Circuit breaker: aiobreaker

- Status: accepted
- Date: 2026-10-19
- Deciders: CTO

## Context
The `FraudCheck` saga step calls an external (mock) service that must be protected
by a circuit breaker (`tz_v2 §3.7`). The draft used `pybreaker`, which is
**sync-only** (no `__aenter__`), so the `async with cb` pattern would not work in
our async FastAPI stack.

## Decision
Use `aiobreaker` wrapped by `shared.circuit.CircuitBreakerHttpClient`:
- entry point is `await cb.call_async(func)` (there is no `async with` on either
  pybreaker or aiobreaker — call_async is the async API);
- `fail_max=5`, half-open after `reset_timeout=30s`, httpx `timeout=3.0s` (must be
  shorter than the fraud `timeout` failure-injection of 5s, so the breaker opens);
- a fallback callback routes the payment to the DLQ instead of raising, so the
  orchestrator stays up (TЗ §3.7);
- non-2xx/5xx responses are counted as breaker failures.

## Consequences
- Native async, no thread-pool executor.
- Canonical Prometheus gauge name is `pybreaker_open` (kept for acceptance scripts
  in phases 06/07/08).
- `state()`/`is_open()` are exposed in phase 1 so the `/v1/_status` aggregator and
  UI badge can read CB state from phase 6 onward.
