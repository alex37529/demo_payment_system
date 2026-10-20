# ADR-012 — Saga state machine: python-statemachine, single authority

- Status: accepted
- Date: 2026-10-20
- Deciders: CTO
- Supersedes: (none) — records the phase-3 decision
- Phase: 3 (`feat/04-saga`)

## Context

A payment saga walks a fixed path (`Created → Checking → Reserve → Fraud →
Debit → Success`) and must be able to fail at any step, with a *different*
failure semantics per step:

* a failure **before** the reservation was booked → `Failed` (nothing to undo);
* a failure **after** it was booked → `RolledBack` (+ `CancelReservation`).

The transition rules are the heart of the orchestrator: if they are spread
across `if`-statements in the handler, an illegal path (e.g. Debit before
Fraud) is one forgotten check away, and the compensation owed can be
silently skipped.

## Decision

1. **The state machine is the single authority on legal transitions.**
   `services/orchestrator/domain/saga_fsm.py` uses `python-statemachine`
   (v3): the states and the transitions are declared in one place, and an
   illegal transition raises instead of corrupting the saga.

2. **The failure routing is encoded STRUCTURALLY, not by an `if`.** The
   `fail` transition is only reachable from `checking`/`reserve` (nothing
   booked → `Failed`), and `rollback` only from `fraud`/`debit` (a
   reservation is booked → `RolledBack` + compensation). An orchestrator bug
   therefore cannot skip a required compensation: the transition itself
   refuses to exist.

3. **The saga AGGREGATE records the history and the debt; it does not
   decide.** `Saga.mark_step()` appends the audit trail and
   `Saga.compensation_commands()` derives *what is owed* from the recorded
   history — so the compensation plan is derived from **what actually
   happened**, not from a separately maintained list that could drift.

4. **The state is persisted and restored.** The FSM is rebuilt from the
   stored state name (`SagaStateMachine.restore(state)`) — a fresh machine
   always starts at `created` (pitfall #2), which would silently re-run the
   saga after a restart.

5. **The compensation order is the reverse of the steps that succeeded**
   (phase-3 spec §3.2). The map lives in
   `services/orchestrator/domain/compensation.py`.

6. **Guards are explicit.** `limits_ok` must be set before the
   `enter_reserve` gate — a guard that is silently True is not a guard.

## Consequences

**Positive:**
- An illegal transition is impossible, not merely unlikely: the library
  raises.
- The compensation owed cannot be skipped by accident — it is derived from
  the recorded history.
- The saga survives a restart of the orchestrator (state + history are in
  the DB).

**Negative / accepted cost:**
- The FSM is a **synchronous** library, so it is used as a *validator*
  (transitions) and not as a workflow engine — the actual I/O stays in the
  handler. This keeps the async code readable, at the price of the FSM not
  being able to drive the flow itself.
- The FSM and the aggregate must be kept in sync (the FSM owns the legal
  transitions, the aggregate owns the history) — a mismatch shows up as a
  transition that raises.

## Alternatives considered

- **Hand-written `if`-chains in the handler.** Rejected: the illegal-path
  protection and the "compensation cannot be skipped" guarantee are exactly
  what a state machine buys, and both are structural here.
- **Temporal / Cadence / Conductor (a real workflow engine).** Rejected for
  a PoC: an extra service, an extra deployment, and a durable-workflow
  model that the PoC does not need — the saga is 5 steps.
- **A pure-Python state machine written by hand.** Rejected: it is the same
  code with no library guarantee, and no `state_change` hook for the
  observability we added in phase 7.

## References

- `services/orchestrator/domain/saga_fsm.py` (the FSM),
  `services/orchestrator/domain/saga.py` (the aggregate),
  `services/orchestrator/domain/compensation.py` (the reverse commands).
- `doc/tz_v2.md` §3.3–§3.4; `doc/phases/03_orchestrator_saga.md` §2.
