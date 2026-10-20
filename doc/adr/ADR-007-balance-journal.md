# ADR-007 — Balance Journal (`ledger.transactions`): append-only audit in the write-side TX

- Status: accepted
- Date: 2026-10-19
- Deciders: CTO
- Supersedes: (none)
- Context: phase 2 (`ledger` write-side); follows ADR-006 (read-side)

## Context

After phase 5 the write-side (`ledger`) stores the **current** balance
(`accounts.balance`) and the **events** it produced (`outbox_events` →
`ledger.events`), and the read-side (`profile`) stores a derived snapshot.
But there is no place to answer, for a given account, *"what happened to my
balance over time?"* — in particular the buy-then-refund sequence
(debit −X, refund +X) that the PoC demos.

The tempting source is `ledger.outbox_events`, but it is **not** a journal:
its `status`/`published_at`/`last_error` columns are delivery-tracking state,
it carries the *whole* event payload (not a normalized amount/balance), and it
mixes in non-balance events (`FundsReserved`, …). Auditing against it is
fragile and conflates "was the event delivered to Kafka" with "what did this
do to the balance".

Requirement: a simple, queryable per-account history of balance-moving
operations (debit / refund), with each row self-describing.

## Decision

1. **Append-only table `ledger.transactions` on the write-side (DB `ledger`).**
   One row per balance-moving op, written by the ledger **in the same
   transaction** as the `accounts` mutation, the idempotency guard
   (`applied_commands`) and the outbox event — i.e. the exact single-TX
   invariant phase 2 established (update `accounts` + insert
   `applied_commands` + insert `outbox_events` + insert `transactions`, one
   `commit`). Consequence: a commit can never reflect a balance change without
   its journal row, and a rollback (insufficient funds) or a replay
   (idempotency no-op) leaves **zero** rows.

2. **Row shape = running balance.** `amount` (magnitude ≥ 0, minor units),
   `event_type` (`FundsDebited` = money left, `RefundPosted` = money back,
   i.e. direction), `balance_after` (the account balance immediately after the
   op). `payment_id` + `event_id` tie the row to the saga and the canonical
   event. Because `balance_after` is recorded on every row, the *last* row's
   `balance_after` equals the current `accounts.balance` — the whole history is
   auditable in one query and re-derivable (each `balance_after` should equal
   the previous ± `amount` by direction).

3. **`event_id` is UNIQUE and taken from the canonical event** (the same id
   that goes to `outbox_events` / Kafka). This re-uses the existing
   idempotency semantics: a redelivered command cannot produce a second journal
   row (unlike `applied_commands`, which keys on the business ref, the journal
   keys on the event, so it stays a true event-level audit log).

4. **Only balance-moving events are journaled.** `FundsReserved` and
   `CancelReservationPosted` move `reserved`, not `balance`, so they are
   excluded. (If reservation history is later wanted, it is a *separate*
   concern/column and decision — not mixed into the balance journal.)

5. **Read surface** is a ledger-local endpoint
   `GET /v1/accounts/{id}/transactions?limit&offset`, exposed to the browser
   through the gateway BFF as `GET /v1/users/{id}/transactions` (kept under the
   `/v1/users/*` façade so the UI namespace stays consistent with `profile`).

## Consequences

**Positive:**
- "What did this account do?" is a single, normalized `SELECT` against one
  table with running balances — no cross-context JOIN, no parsing event JSON.
- Consistent with the existing architecture: same write-side, same single-TX
  invariant, same idempotency rule (unique-key guard). No new service, no new
  consumer, no new database.
- Audit-grade: append-only (no UPDATE/DELETE path in the code), per-op
  `balance_after`, event/payment linkage.

**Negative / accepted cost:**
- One extra INSERT per debit/refund (negligible).
- `reserve`/`cancel` are *not* in this journal (by design); reservation
  auditing would need its own decision.
- Pre-journal history (before this change) does not exist in the table — it
  can only be reconstructed from `outbox_events` if ever needed; accepted as a
  PoC boundary.
