# ADR-006 — Read-Side (CQRS): denormalized copy, dual seed, SSE keepalive

- Status: accepted
- Date: 2026-10-19
- Deciders: CTO
- Supersedes: (none) — refines tz_v2 §3.8 / §3.9 / §5 and phase-5 spec §1–§4
- Phase: 5 (`feat/06-read-side`)

## Context

Phase 2–4 built the **write-side**: the `ledger` owns the balance (aggregates,
invariants, Transactional Outbox → Debezium → `ledger.events`), the `orchestrator`
owns the saga, and `intake` owns idempotent payment creation.

Phase 5 must deliver the **read-side** so the UI can show (a) the 20 seeded
users with live-updating balances and (b) a live payment feed — both fed by
*consuming* `ledger.events`, never by reading another service's database
(cross-context boundary rule, tz_v2 §2.2). The spec (`doc/phases/05_read_side_sse.md`)
sketches this but rests on a few implicit decisions that are worth making
explicit and consistent with the decisions already taken in phases 2–4:

1. **CQRS consistency model.** How does the read-side stay eventually
   consistent with the write-side, and how do we guarantee the *start* state
   (seed) is identical on both sides even though `ledger` and `profile` are
   separate Postgres databases (no cross-DB transaction is possible)?
2. **Read-side idempotency.** Kafka is at-least-once. `payment_id` idempotency
   (phase 4, `intake`) and `event_id` idempotency (read-side) are different
   concerns — need a single clear rule.
3. **Delivery of realtime updates.** SSE vs WebSocket, heartbeat, slow-client
   backpressure, and nginx buffering — choose one and document it.

## Decision

1. **The read-side is a denormalized copy refreshed by events — never a JOIN.**
   `profile.users` duplicates the balance (one row per user). The Profile
   consumer (`services/profile/kafka_consumer.py`) subscribes to `ledger.events`
   under group `profile-balance` and calls the *pure* projection
   (`services/profile/projection.py::resolve_new_balance`), which applies only
   the **final-balance** events (`FundsDebited`, i.e. those with `new_balance`).
   Delta events (`FundsReserved`, `CancelReservationPosted`) do **not** change the
   final balance, so they are recorded-but-no-op. Profile never reads the
   `ledger` DB (pitfall #4: no cross-schema JOIN, CQRS-свойство).

2. **Dual seed, deterministic, idempotent re-run.** The 20 users are seeded in
   *both* `ledger.accounts` and `profile.users` (identical ids / names /
   balances / currencies) so write- and read-sides agree at t=0 (tz_v2 §5).
   Because Postgres isolates databases, this is two `INSERT` blocks (not one
   cross-DB TX), but **deterministic** (no `random()`) and
   `ON CONFLICT (id) DO NOTHING` (re-runnable). Consistency-at-start holds
   because the two halves use the same fixed 20 rows and run in the same
   `initdb` script.

3. **Read-side idempotency = a unique `profile.processed_events(event_id)`
   guard.** One row per handled `ledger.events` `event_id`. `apply_event`
   does a *guarded* insert first; a redelivery that already has the row is a
   no-op (and any concurrent duplicate is an `IntegrityError` → rolled back,
   consumed as "already applied"). This is the exact same pattern as the
   write-side `ledger.applied_commands` (phase 2) and `intake.idempotency_keys`
   (phase 4) — a single, consistent idempotency rule across the stack.

4. **SSE (not WebSocket) with a heartbeat, fan-out, drop-on-full, and
   `X-Accel-Buffering: no`.** One-way server→client is all the feed needs
   (EventSource). The relay fans an event out to N subscriber queues; a slow
   client whose queue overflows **drops its own newest event** and never blocks
   the rest (head-of-line protection). A `: keepalive` comment is emitted on
   every heartbeat (default 15s) so proxies don't idle-close the socket.
   Cleanup (unsubscribe) is idempotent and runs in the generator `finally`,
   so a client disconnect never leaks a subscription. (pitfalls #6/#7/#8.)

## Consequences

**Positive:**
- The UI gets live balances + feed with ≤~2s end-to-end lag (event in Kafka →
  Debezium → consumer → projection/relay), with no cross-DB reads and no
  double-balance-update on redelivery.
- One consistent idempotency rule (unique-key guarded insert) across
  write-side, intake and read-side — easy to reason about and to test.
- SSE + EventSource gives free browser auto-reconnect (the phase-6 frontend
  requirement), no upgrade handshake, no binary protocol.

**Negative / accepted cost:**
- Eventual consistency: the displayed balance can lag the write-side by the
  Kafka round-trip (100–300ms). Acceptable for CQRS (tz_v2 §5); the UI must
  treat "balance refreshes after ~1s" as normal.
- Two seed INSERTs (not one cross-DB TX). Mitigated by deterministic fixed
  rows + `DO NOTHING` + same-script ordering, so a mid-initdb crash cannot
  leave one side seeded and the other not in a way that breaks the invariant.
- `RefundPosted` (currently no `new_balance` on the canonical event) is not
  applied to the read balance in phase 5; the ledger can add `new_balance` to
  it in a follow-up to enable refund display.
