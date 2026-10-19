# ADR-005 — Saga orchestration: event-driven state, watchdog-armed compensation

- Status: accepted
- Date: 2026-10-19
- Deciders: CTO
- Supersedes: (none) — refines tz_v2 §3.3/§3.4 and phase-3 spec §3/§5

## Context

Phase 3 = the saga orchestrator. The spec (phase-3 §3.5) sketched a
`DebitFailed` event on `ledger.events` driving compensation, but the Phase-2
ledger — by correct design — does **not** emit that event: a rejected
`Reserve`/`Debit` is a *business* error the ledger logs and stays up for, because
saga compensation is **not the ledger's concern** (it doesn't own the saga).
The spec also assumes `ReserveConfirmed`-style events, whereas the ledger —
through Debezium + `ExtractNewRecordState` on the outbox — emits the *outbox
row* with the event nested in a `payload` **JSON string** (a PG `JSONB` column).

Three decisions are needed to close the gap between the spec (a good sketch) and
the contract the ledger actually keeps (a Phase-2 invariant we must not break).

## Decision

1. **The orchestrator owns the lifecycle; the ledger owns the money.**
   The orchestrator drives `SagaStateMachine`, publishes lifecycle events
   (`LimitsChecked`, `FraudVerdict`, `SagaSucceeded`/`SagaFailed`, `DebitFailed`)
   to `payments.events`, and reacts to the ledger's `FundsReserved` /
   `FundsDebited` on `ledger.events`. The ledger's "business error" (insufficient
   funds, etc.) stays a log-only concern on the ledger side — the orchestrator
   never has to read the ledger DB (boundary rule, tz_v2 §2.2).

2. **Compensation is triggered by a Watchdog, not by a new cross-service event.**
   A small `asyncio` task in the orchestrator (`run_watchdog`) sweeps sagas stuck
   in an in-flight state (`Reserve`/`Fraud`/`Debit`) past a threshold
   (`ORCH_WATCHDOG_S`) and applies the same compensation the spec's
   `on_debit_failed`, `RolledBack` + `CancelReservation`. Rationale:
   * it keeps the Phase-2 ledger untouched (no new event producer there);
   * it works for **any** stall (ledger OOM, Kafka redelivery, a crashed step),
     which a single `DebitFailed` event would not;
   * it is the orchestrator's own concern (single coordination point, §1 TЗ).
   The `DebitFailed` handler (`on_debit_failed`) remains wired for the explicit
   event should a future phase add it (defense in depth / forward-compat).

3. **`event_type` is the discriminator in `ledger.events`.**
   We added `event_type` to every canonical event's `to_json()` (phase-3,
   backwards-compatible, additive). The orchestrator's consumer:
   * unwraps the Debezium record (`payload` JSON string → inner event),
   * prefers the explicit `event_type`,
   * falls back to shape-inference (`new_balance` → `FundsDebited`, `reason` →
     `CancelReservationPosted`, else `FundsReserved`) so **pre-phase-3 outbox
     rows** (already in `ledger.events` before this phase) still dispatch.
   This is the "canonical events" rule (tz_v2 §3.11.1) applied to the *wire
   format* where the ledger's Debezium output and the orchestrator's expectations
   meet.

## Consequences

- The orchestrator is a self-contained bounded context: it reads/writes only its
  own `orchestrator.sagas` table, consumes two topics, and publishes to `*.events`
  + `ledger.commands` (+ `.dlq`). It never touches the `ledger` DB.
- Compensation decisions are derived from the **persisted** saga `history`
  (`Saga.has_confirmed_reservation()`), not from in-memory flags — so they
  survive an orchestrator restart (phase-3 spec pitfall #3).
- The watchdog is off by default in tests and cheap in prod (a `SELECT ... WHERE
  state IN (...) AND updated_at < now()-30s` every 30s); it only acts on sagas
  that genuinely stalled. Its trigger is observable via `ledger.commands.dlq`.
- `python-statemachine` v3 was used via the v2-compatible `StateMachine` alias;
  multi-source failure/rollback transitions use `target.from_(*)` and guards are
  declared **before** the transition that references them (v3 evaluates `cond`
  at class-creation time).

## Alternatives considered

- **New `DebitFailed` producer in the ledger.** Rejected: it changes a Phase-2
  component for a Phase-3 concern, and a single event can't cover every stall
  type the watchdog covers.
- **Have the orchestrator read the ledger DB to detect a reject.** Rejected:
  violates the boundary rule (tz_v2 §2.2) — services must not read each other's
  DBs.
