# ADR-011 — Idempotency: Redis `SET NX EX` + Postgres mirror, two TTLs

- Status: accepted
- Date: 2026-10-20
- Deciders: CTO
- Supersedes: (none) — records the phase-4 decision
- Phase: 4 (`feat/05-intake`)

## Context

`POST /v1/payments` must be safe to retry: a client that times out and
re-sends the same request must not create a second payment. The contract
(tz_v2 v2 §3.1) is **`Idempotency-Key` → one response**, and it must hold
across a **process restart** and across **several instances** of the intake.

That rules out the in-process answers:
* a dict in the intake's memory — lost on restart, and per-instance;
* a unique index on `payment_id` — the client does not know the payment_id
  before the first call, it only has its own key.

The key the client DOES have is the `Idempotency-Key` header. So the
answer must be a durable mapping `key → response`, and it must be
**atomic** (two concurrent requests with the same key must not both create a
payment).

## Decision

1. **Redis is the primary store, `SET key value NX EX ttl`.** The `NX` makes
   the acquisition atomic — exactly one caller wins; the losers get the
   stored answer. `EX` bounds the lifetime in Redis itself (no TTL sweep).
   Redis is already in the stack (it is the fastest correct answer, and it
   is shared by all intake instances).

2. **Two TTLs, for two different jobs** (pitfall #29a):
   * the **`pending` marker** lives **120 s** — it says "a worker is on it".
     It must be SHORT, or a worker that dies mid-flight (a crash, a pod
     eviction) would wedge the key forever and the client could never retry.
   * the **final answer** lives **24 h** — the contract: within a day a
     repeat of the same key returns the same response.

3. **A 5xx never stores an answer.** Only a successful response is
   remembered, so a failed request can be retried with the same key.

4. **Postgres mirrors the mapping** (`intake.idempotency_keys`) so the
   guarantee survives a **Redis flush** (Redis is a cache; losing it must
   not break the contract). The mirror is what makes the answer
   recoverable after a restart of the whole stack; Redis is what makes it
   fast and atomic under concurrency.

5. **The payment is created only by the winner.** The loser of the `SET NX`
   reads the stored answer and returns it verbatim (the same `payment_id`,
   the same status) — it never touches the payment pipeline.

## Consequences

**Positive:**
- A retry of the same key returns the same `payment_id` — the client can
  retry safely after a timeout (the acceptance criterion "дубль
  Idempotency-Key → 1 событие").
- The guarantee holds across restarts (Postgres mirror) and across
  instances (Redis is shared).

**Negative / accepted cost:**
- **Redis is a single point of truth for the fast path.** If Redis is down,
  the intake cannot deduplicate — the design falls back to the Postgres
  mirror (slower, and the atomicity then rests on the unique index there).
- The `pending` marker means a request that is still in flight answers
  **409 "in_progress"** rather than blocking — the client is expected to
  retry. (Blocking would hold a request open for the whole saga.)
- The 24 h answer TTL means a retry **after** a day creates a NEW payment.
  That is the documented contract, not an accident.

## Alternatives considered

- **Postgres only** (unique index on the idempotency key): atomic and
  durable, but every request pays a DB round-trip for the *hit* path, and
  the intake's hot path is the one that must not be slowed. Rejected as
  the primary store; kept as the mirror.
- **The payment_id as the idempotency key**: the client would have to
  generate it — that changes the API contract (the client would own the
  identity), and it does not remove the need for a durable mapping.
- **Locking in Redis with a long TTL and no answer**: a crash would leave
  the key locked for the whole TTL. Rejected (the two-TTL split).

## References

- `shared/idempotency.py` (the middleware), `services/intake/application/idempotency.py`
  (the Redis+PG service), `services/intake/infrastructure/` (the mirror).
- `doc/tz_v2.md` §3.1; `doc/phases/04_intake_idempotency.md`.
