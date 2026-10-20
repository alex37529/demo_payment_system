# Sequence diagrams

Rendered with Mermaid (renders natively on GitHub). The D2 source is in
`doc/diagrams/sequence.d2`.

## 1. Happy path — one payment, one trace

```mermaid
sequenceDiagram
    autonumber
    participant B as Browser (frontend)
    participant G as Gateway :8000
    participant I as Intake :8001
    participant K as Kafka
    participant O as Orchestrator :8002
    participant F as mock-fraud :8007
    participant L as Ledger :8003
    participant P as Postgres (ledger)
    participant R as Profile :8004

    B->>G: POST /v1/payments (Idempotency-Key)
    Note over B,G: span: gateway server
    G->>I: POST /v1/payments<br/>(traceparent header)
    Note over G,I: span: gateway client -> intake server
    I->>I: SETNX idem:key (Redis)
    I->>K: payments.commands {cmd:Create}
    Note over I,K: traceparent in the message HEADERS
    K-->>O: payments.commands
    Note over O: span: consume payments.commands
    O->>L: (via Kafka) ledger.commands {cmd:Reserve}
    L->>P: TX: reserved += X
    L->>P: TX: outbox_events row
    L->>P: COMMIT
    Note over L,P: Debezium reads the WAL
    P-->>K: ledger.events {FundsReserved}
    K-->>O: ledger.events
    O->>F: POST /fraud/check
    Note over O,F: span: fraud client -> fraud server
    F-->>O: approve
    O->>K: ledger.commands {cmd:Debit}
    L->>P: TX: balance -= X, journal row
    L->>P: COMMIT
    P-->>K: ledger.events {FundsDebited, new_balance}
    K-->>R: ledger.events
    R->>R: upsert profile.users.balance
    K-->>O: ledger.events
    O->>K: payments.events {SagaSucceeded}
    K-->>B: (SSE) ledger.events
    B->>B: live balance + feed update
```

**Where the money is checked**

| Step | Invariant |
| --- | --- |
| Reserve | `available = balance - reserved >= amount` |
| Debit | `available >= amount`, currency must match the account (ADR-003) |
| Both | the balance change + the outbox row are ONE transaction (ADR-008) |
| Read-side | applied `event_id` is inserted once — a redelivery is a no-op (ADR-006) |

## 2. Fraud failure — the circuit breaker opens

```mermaid
sequenceDiagram
    autonumber
    participant I as Intake
    participant K as Kafka
    participant O as Orchestrator
    participant F as mock-fraud
    participant L as Ledger
    participant D as DLQ (ledger.commands.dlq)

    I->>K: payments.commands {cmd:Create}
    K-->>O: payments.commands
    O->>L: ledger.commands {cmd:Reserve}
    L-->>K: ledger.events {FundsReserved}
    K-->>O: FundsReserved
    Note over O: reservation confirmed
    O->>F: POST /fraud/check
    F-->>O: 500 (injected "errors" mode)
    Note over O: failure 1/2/3 -> the breaker counts it
    O->>F: POST /fraud/check
    F-->>O: 500
    O->>F: POST /fraud/check
    F-->>O: 500
    Note over O,fraud: fail_max reached -> breaker OPEN
    O->>O: pybreaker_open = 1 (Prometheus)
    O->>D: {payment, reason} — the payment is NOT lost
    O->>L: ledger.commands {cmd:CancelReservation}
    L->>L: reserved -= X (the hold is released)
    Note over O: saga -> RolledBack
    O->>K: payments.events {SagaFailed}
```

**What the demo shows**

- The breaker opens after `fail_max` failures; the *next* call is rejected
  immediately (no call to fraud at all) — the fallback returns
  `route_to_dlq`.
- The failed payment lands in the DLQ, so nothing is lost, and the valid
  stream keeps flowing.
- `pybreaker_open` goes 0 → 1 in Prometheus; the Grafana badge turns red;
  the `CircuitBreakerOpen` alert fires (severity: critical).
- The reservation is released by the compensation — a failed payment leaves
  no hold on the account.

## 3. Ledger unavailable — the payment is not lost

```mermaid
sequenceDiagram
    autonumber
    participant I as Intake
    participant R as Redis
    participant K as Kafka
    participant O as Orchestrator
    participant L as Ledger

    I->>R: SETNX idem:key "pending" (TTL 120s)
    I->>K: payments.commands {cmd:Create}
    K-->>O: payments.commands
    O->>L: ledger.commands {cmd:Reserve}
    L--xO: connection refused
    Note over O: the ledger is down -> the command is retried
    O->>K: payments.commands.dlq (after the retry budget)
    Note over O: the saga -> Failed, the payment is in the DLQ
    O->>I: (no answer — the client already got 202)
    Note over I,R: the "pending" marker expires (120s)<br/>-> the client may retry with the same key
```

**Why the pending marker is short-lived**

`Idempotency-Key` uses two TTLs: the `pending` marker is **120 s** (so a
worker that dies mid-flight does not wedge the key forever) and the final
answer is **24 h** (the contract). A 5xx never stores an answer.
