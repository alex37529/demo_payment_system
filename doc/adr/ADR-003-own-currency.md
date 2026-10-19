# ADR-003 — Own-Currency Invariant

- **Status:** Accepted
- **Date:** 2026-10-19
- **Refs:** doc/tz_v2.md §3.10 (decision #5), plan §4.9
- **Alternative considered:** Cross-currency operations with implicit conversion
  rates (rejected: adds 4× the state machine complexity for a PoC scope that
  doesn't need it).
- **Alternative considered:** `Decimal` + PostgreSQL `NUMERIC` (rejected: the
  PoC's SLO is 50 TPS / P99 < 300 ms on a single node; integer minor units are
  simpler, cheaper, and the arithmetic is exact for USD/EUR minor=2).

## Decision

Ledger write-side operations enforce **two invariants** on every command
(`Debit` / `Reserve` / `CancelReservation` / `Refund`):

1. **Own-currency**: command `amount.currency` must equal `account.currency`;
   a mismatch is a hard 409 error.
2. **Sufficient available**: `balance - reserved` must be ≥ `amount`; a deficit
   is a hard 409 error.

Both checks happen **inside the aggregate** (`Account.debit/reserve/…`),
never in the use-case or controller. This is the single source of truth:

```
HTTP/Kafka -> use-case -> Account.debit(...) -> InsufficientFundsError -> 409
```

The error is mapped to a 409 response by the FastAPI exception handler and
logged by the Kafka consumer (so the orchestrator can pick up compensation in
phase 3).

## Consequences

- The PoC supports the two currencies it seeds (`USD`, `EUR`); adding a new
  one is a one-line change in `Money.__post_init__`.
- `Money` is a frozen `ValueObject` — equality by value, hashable — so it can
  be used as a dict key or in sets without custom logic.
- The use-case's `applied_commands` guard makes replay a no-op even if the
  invariant would have been satisfied on the first attempt (idempotency is
  orthogonal to correctness).

## Scope

Own-currency only. Cross-currency (FX) is out of scope for this PoC (v2 #5).
