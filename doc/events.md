# Event Contracts

The wire contract between the services. **The canonical catalogue is
`shared/domain/events.py`** — the dataclasses there are the single source of
truth (tz_v2 §3.11.1); this document describes how they travel.

- Every payload is **JSON**; every message carries an `event_id` (UUID) that
  is the idempotency key of the event.
- Every event has an `event_type` discriminator (the class name) so a
  consumer can route on it without guessing from the field set.
- Money is always `{"value": <int minor units>, "currency": "<ISO-4217>"}` —
  never a float (pitfall #3).

## Topics

| Topic | Producer | Consumers | Kind |
| --- | --- | --- | --- |
| `payments.commands` | `intake` | `orchestrator` | command |
| `ledger.commands` | `orchestrator` | `ledger` | command |
| `payments.events` | `orchestrator` | (inspection; the read-side uses `ledger.events`) | event |
| `ledger.events` | `ledger` → **Debezium** (CDC of the outbox table) | `orchestrator`, `profile`, `notification` | event |
| `payments.commands.dlq` | `intake` (producer fallback) | inspection | dead letter |
| `ledger.commands.dlq` | `orchestrator` (fraud failures, bad commands) | inspection | dead letter |
| `payments.events.dlq` | `orchestrator` (producer fallback) | inspection | dead letter |

Topics are created by `kafka/topics.sh` (3 partitions, RF 1). Each topic
has a **`.dlq`** twin: aiokafka has no DLQ, so after the retry budget the
producer publishes to `<topic>.dlq` and raises (ADR context §3.8).

## Commands

### `payments.commands` — intake → orchestrator

```json
{
  "cmd": "Create",
  "payment_id": "P-1f2e...",
  "account_id": "U001",
  "amount": { "value": 5000, "currency": "USD" },
  "idem_key": "e2e-6b1c...",
  "saga_id": ""
}
```

| Field | Meaning |
| --- | --- |
| `cmd` | always `"Create"` (the only command on this topic) |
| `payment_id` | the payment's identity — the saga's unique key |
| `account_id` | the account to charge |
| `amount` | `{value, currency}`; the currency must match the account (ADR-003) |
| `idem_key` | the client's `Idempotency-Key` (echo, for tracing) |

Produced by `intake` **after** the idempotency key is acquired. A replay
with the same key returns the *same* `payment_id` and publishes nothing new.

### `ledger.commands` — orchestrator → ledger

```json
{
  "cmd": "Reserve" | "Debit" | "CancelReservation" | "Refund",
  "saga_id": "…",
  "payment_id": "P-1f2e...",
  "account_id": "U001",
  "amount": { "value": 5000, "currency": "USD" },
  "reason": ""
}
```

| `cmd` | Effect on the account | Emits |
| --- | --- | --- |
| `Reserve` | `reserved += amount` (holds the money, `balance` untouched) | `FundsReserved` |
| `Debit` | `balance -= amount` | `FundsDebited` (with `new_balance`) |
| `CancelReservation` | `reserved -= amount` (releases the hold) | `CancelReservationPosted` |
| `Refund` | `balance += amount` | `RefundPosted` |

The `reason` is carried for the failure path (why a compensation was
issued). `CancelReservation` is the **compensation** command — the saga
issues it when a step fails after the reservation was confirmed (ADR-005).

## Events

### `ledger.events` — ledger → read-side (via Debezium)

Produced by **Debezium** from the `outbox_events` table, so the message
value is the **outbox row**, and the domain event is nested inside its
`payload` column (a JSON **string**, because the column is `JSONB` and
Debezium renders it as a string):

```json
{
  "id": 42,
  "event_id": "82d66e04-0259-4fa9-90e6-cd934980a217",
  "topic": "ledger.events",
  "payload": "{\"ts\": \"…\", \"event_type\": \"FundsDebited\", \"event_id\": \"…\", \"account_id\": \"U001\", \"payment_id\": \"P-1f2e…\", \"amount\": {\"value\": 5000, \"currency\": \"USD\"}, \"new_balance\": {\"value\": 615000, \"currency\": \"USD\"}}",
  "status": "pending",
  "published_at": null,
  "last_error": null,
  "created_at": "…",
  "updated_at": "…"
}
```

Consumers unwrap it with `shared/kafka.py::unwrap_event(record)`, which
returns the inner event dict.

| Event | When | Carries |
| --- | --- | --- |
| `FundsReserved` | the hold was booked | `account_id`, `payment_id`, `amount` |
| `FundsDebited` | the money moved | + `new_balance` (the **final** balance) |
| `CancelReservationPosted` | the hold was released | `account_id`, `payment_id`, `amount`, `reason` |
| `RefundPosted` | the money was returned | `account_id`, `payment_id`, `amount`, `reason` |

**Only `FundsDebited` carries `new_balance`.** The read-side applies the
final balance on that event alone; the delta events (reserved / cancelled)
change no balance (`services/profile/projection.py::resolve_new_balance`).

### `payments.events` — orchestrator → inspection

```json
{
  "event_type": "SagaSucceeded" | "SagaFailed" | "FraudVerdict" | "LimitsChecked" | "DebitFailed",
  "event_id": "…",
  "ts": "2026-10-20T09:00:00+00:00",
  "saga_id": "…",
  "payment_id": "P-1f2e...",
  "reason": "",
  "failed_step": "",
  "decision": "approve",
  "score": 0.42
}
```

The saga's own lifecycle. `SagaSucceeded` / `SagaFailed` are the terminal
events; `FraudVerdict` records the fraud decision, `DebitFailed` the
rejected debit.

## Dead letters

```json
{
  "original_topic": "ledger.commands",
  "value": { "…": "the original payload" },
  "error": "…",
  "attempts": 5
}
```

Two producers of DLQ messages:
- the **producer fallback** (`shared/kafka.py`): the publish failed after the
  retry budget → the message goes to `<topic>.dlq` and the publish raises;
- the **orchestrator** (`services/orchestrator/application/saga_handler.py`):
  a payment that failed the fraud check (or a malformed command) is routed to
  `ledger.commands.dlq` **deliberately** — it is not lost, it is parked.

## Trace context

Command and event messages produced by our own services carry the W3C
`traceparent` (and `baggage`) in their **Kafka headers** — see ADR-009 and
`shared/tracing.py`. The `ledger.events` messages are produced by Debezium
and therefore carry no trace context (ADR-008, consequence 2).
