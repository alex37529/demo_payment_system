# C4 — Context & Container view

Rendered with Mermaid (renders natively on GitHub). The D2 source of the same
picture is in `doc/diagrams/c4-container.d2` (render with `d2`).

## System context

```mermaid
flowchart LR
    customer([Customer])
    operator([Operator])

    subgraph fintech["fintech_demo — event-driven payments PoC"]
        direction TB
        core["saga + outbox + CQRS"]
    end

    kafka[["Kafka (KRaft)"]]
    pg[["PostgreSQL — 4 DBs"]]
    redis[["Redis"]]
    fraud[/"Fraud API"/]
    acquirer[/"Acquirer"/]

    customer -->|"creates payments, watches balance"| core
    operator -->|"dashboards, failure injection"| core
    core --> kafka
    core --> pg
    core --> redis
    core -->|"fraud check (circuit breaker)"| fraud
    acquirer -->|"creates payments (load)"| core
```

## Container view

```mermaid
flowchart TB
    customer([Customer])

    subgraph edge["Edge"]
        frontend["Frontend<br/>React 18 + Vite<br/>users table, live feed, CB badge"]
        gateway["Gateway / BFF<br/>FastAPI<br/>routes /v1/*, terminates SSE,<br/>aggregates CB status"]
    end

    subgraph core["Core services"]
        intake["Intake<br/>FastAPI<br/>idempotent payment creation<br/>-> payments.commands"]
        orchestrator["Orchestrator<br/>FastAPI<br/>saga: Check-Reserve-Fraud-Debit<br/>compensation, DLQ"]
        ledger["Ledger<br/>FastAPI<br/>write-side: balance aggregates<br/>Transactional Outbox"]
    end

    subgraph readside["Read-side (CQRS)"]
        profile["Profile<br/>FastAPI<br/>users + balance projection"]
        notification["Notification<br/>FastAPI<br/>payment feed + SSE"]
    end

    subgraph mocks["Mocks (external)"]
        mockfraud["mock-fraud<br/>failure injection"]
        mockacq["mock-acquirer<br/>load generator (N TPS)"]
    end

    kafka[["Kafka<br/>payments.commands / ledger.events / *.dlq"]]
    pg[["PostgreSQL<br/>intake / ledger / orchestrator / profile"]]
    tempo[["Tempo"]]
    obs[["Prometheus + Grafana + Loki"]]

    customer --> frontend
    frontend --> gateway
    gateway -->|"POST /v1/payments"| intake
    gateway -->|"GET /v1/users"| profile
    gateway -->|"GET /v1/stream (SSE)"| notification
    gateway -->|"GET /v1/circuit-breakers"| orchestrator
    mockacq -->|"POST /v1/payments"| intake

    intake -->|"payments.commands"| kafka
    intake --> pg
    orchestrator -->|"ledger.commands / payments.events / DLQ"| kafka
    orchestrator -->|"POST /fraud/check (CB)"| mockfraud
    orchestrator --> pg
    ledger -->|"ledger.commands"| kafka
    ledger --> pg
    kafka -->|"ledger.events (CDC via Debezium)"| profile
    kafka -->|"ledger.events (CDC via Debezium)"| notification

    core -.->|"spans"| tempo
    core -.->|"metrics"| obs
```

## Boundaries that matter

| Rule | Where it is enforced |
| --- | --- |
| A service never reads another service's DB | one database per bounded context (`init/01_databases.sql`); the read-side consumes events, never a JOIN |
| The event contract lives in the domain | `shared/domain/events.py` — the canonical catalogue (tz_v2 §3.11.1) |
| Every write-side mutation + its event are one transaction | `shared/outbox.py` (ADR-008) |
| One trace per payment | `shared/tracing.py` (ADR-009) |
| A saga failure never loses the payment | compensation (`CancelReservation`) + DLQ (`ledger.commands.dlq`) |
