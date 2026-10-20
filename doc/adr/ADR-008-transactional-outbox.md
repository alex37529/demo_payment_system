# ADR-008 — Transactional Outbox: event row in the same TX, Debezium ships the WAL

- Status: accepted
- Date: 2026-10-20
- Deciders: CTO
- Supersedes: (none) — documents the phase-2 decision, refines tz_v2 §3.2
- Phase: 2 (`feat/03-ledger`), formalized in 7 (`feat/08-obs-tests`)

## Context

The write-side has to do two things atomically: change the balance **and**
announce the change to the rest of the system. The naive way — commit the
balance, then publish to Kafka — is the **dual-write problem**: the two
writes are not atomic, so a crash between them loses the event (the balance
moved, nobody was told) or duplicates it (the event was published, the
transaction rolled back). Retrying the publish does not fix it: the broker
is not part of the transaction, and "did I already publish?" is unanswerable
across a crash.

A second constraint shapes the choice: the read-side is **not** allowed to
read another service's database (tz_v2 §2.2, no cross-context JOIN), so the
balance change MUST reach the other services somehow — through the event
stream.

The phase-2 spec sketched the outbox table; this ADR records the decision
and, importantly, the parts that were only discovered by running it
(the CDC specifics and the delivery-state problem).

## Decision

1. **The event row is written in the SAME transaction as the data change.**
   `shared/outbox.py::OutboxMixin._publish_outbox(session, topic, event)`
   appends an `outbox_events` row to the *caller's* session; the use-case
   then commits **once** (accounts + applied_commands + outbox_events +
   transactions journal — one `commit`, phase-2/ADR-007). The event is
   never published *after* the commit (pitfall #14: publish-after-commit can
   lose the event on a crash in between).

2. **Debezium ships the WAL, not our producer.** A Postgres connector
   (`kafka/connect/debezium-ledger.json`) reads the logical replication
   stream of `outbox_events` and writes every insert to the `ledger.events`
   topic. Our code never touches Kafka for the write-side events — which is
   exactly why the event cannot be lost: if the app dies after the commit,
   the row is already in the WAL and Debezium ships it later.

   Consequence: the `ledger.events` messages carry **no** trace context
   (Debezium is not our process and cannot inject a `traceparent` header) —
   the CDC hop starts a new trace. The *synchronous* call chain
   (Gateway → Intake → Orchestrator → Ledger) is fully linked, because
   those hops go through our own producer (ADR-011).

3. **The outbox row is a plain row, and the topic is data, not code.**
   `outbox_events(topic, payload, status, published_at, last_error)`. The
   `Route` transform maps the table to the topic name, so the topic is
   decided by the outbox row, not by the connector config. The payload is
   the canonical event JSON (`shared/domain/events.py`), so the wire format
   is owned by the domain, not by the transport.

4. **Delivery state is tracked, and reconciled by polling Kafka.**
   `status` ∈ `pending | published | failed` is flipped by
   `services/ledger/application/delivery_tracker.py`, which asks Kafka for
   the recently delivered `event_id`s and matches them against the pending
   rows. A row still pending after the horizon (5 min) becomes `failed`
   with a reason. Debezium's own offset is the source of truth for
   *delivery*; the column is our **audit** of it, so "what is still in the
   outbox?" is answerable with a `SELECT` instead of a WAL scan.

   The tracker reads Kafka with a **manually assigned** consumer (no group
   subscription) — a topic subscription and `assign()` are mutually
   exclusive in aiokafka (`IllegalStateError`), and `partitions_for_topic`
   / `assign` / `seek` are **synchronous** there (awaiting them raises
   `TypeError`).

5. **The read-side is idempotent on `event_id`, not on business state.**
   Kafka is at-least-once, so every consumer must tolerate a duplicate.
   `profile.processed_events(event_id)` (ADR-006) and
   `ledger.applied_commands(idem_key)` (phase 2) are the two guards.

## Consequences

**Positive:**
- The balance and its event can never disagree: one transaction, one commit.
- A crash after the commit does not lose the event — Debezium ships it from
  the WAL.
- The event contract is owned by the domain (`shared/domain/events.py`),
  the transport is an implementation detail.
- "What has not been delivered yet?" is a query, not a WAL scan.

**Negative / accepted cost:**
- **CDC latency**: the event reaches Kafka after Debezium's polling
  interval, so the read-side lags the write-side by ~100–300 ms. Accepted
  (the read-side is explicitly eventually consistent, ADR-006).
- **The write-side events are not traced across the CDC hop** (see
  consequence 2). A `traceparent` column on the outbox row would fix it
  at the cost of a schema change and a Debezium reconfiguration; deferred.
- The delivery-state reconciliation is **polling**, not push: the tracker
  runs every 5 s and can be up to one interval late.
- A `failed` outbox row is a *symptom*, not a repair: the money moved and
  the event may never reach the read-side. The PoC does not implement a
  repair path (out of scope).
- Debezium is a second moving part: it must be registered, healthy, and
  its connector config must match the table (a schema change to
  `outbox_events` needs a connector reload).

## Alternatives considered

- **Publish to Kafka inside the transaction** (2PC-style). Rejected: Kafka
  has no distributed-transaction participation, and a Kafka publish inside a
  DB transaction has the same dual-write problem, just with the crash
  window moved.
- **Change Data Capture without an outbox** (Debezium reads the
  `accounts` table directly). Rejected: the CDC stream would then contain
  *row changes*, not *business events* — every consumer would have to
  reconstruct "a debit happened" from a balance diff. The event contract
  (ADR-011's catalogue, the saga's vocabulary) would be lost.
- **Polling the DB for changes** (a "tail the accounts table" job).
  Rejected: no event semantics, and it would re-introduce a polling
  delay on top of everything else.
