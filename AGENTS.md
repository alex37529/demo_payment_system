# AGENTS.md — fintech_demo

Guidance for AI agents working in this repo. Read `doc/tz_v2.md` (the TЗ —
source of truth), `doc/implementation_plan.md`, and the latest
`doc/done/*.md` report **before** writing code. If they disagree with this
file, the TЗ/plan wins.

## What this is
Event-driven payments PoC. 8 FastAPI microservices + Kafka (KRaft) +
PostgreSQL (4 DBs in 1 container) + Redis 7 + Debezium (CDC/Outbox) + full
observability, all in Docker. Demonstrates Saga, Transactional Outbox, CQRS,
DDD, Circuit Breaker, Idempotency.

## Phase status
- **Phase 1 DONE** — env + `shared/` abstractions + 8 placeholder services
  (each: `/healthz`, `/metrics`, `/_status`). See `doc/done/01_environment_abstractions.md`.
- **Phase 2 NEXT** — Ledger write-side + Transactional Outbox. Spec:
  `doc/phases/02_ledger_write_side.md`, plan §4, exit:
  `POST /ledger/v1/accounts/{id}/debit` → balance−X + outbox row + `ledger.events` (`FundsDebited`).
- Phases 3–8 defined in `doc/implementation_plan.md` §5–§10. Do not build
  phase N business logic before its prerequisites are merged.

## Stack (pinned — do not swap without an ADR)
Python 3.11 · FastAPI · SQLAlchemy 2.0 (async) · Pydantic v2 · aiokafka ·
**aiobreaker** (NOT pybreaker) · **python-statemachine v3** ·
**apache/kafka:3.7.0 KRaft** (NOT bitnami) · Debezium (`debezium/connect`) ·
OpenTelemetry→Tempo · Prometheus · Loki. Deps in `pyproject.toml`.

## Layout & conventions
```
shared/        # the contract: domain (DDD + events), db, outbox, kafka,
               # circuit, state_machine, observability, idempotency
services/<name>/main.py  # thin: `app = build_app("<name>")` (+ routes in later phases)
services/main.py         # entrypoint: resolves SERVICE_NAME (dashed ok, e.g. mock-fraud)
kafka/  init/  observability/  scripts/  tests/  doc/
```
- One `pyproject.toml`, one `docker-compose.yml`, one `Dockerfile`
  (build-arg `SERVICE_NAME`) at the root. Services are placeholders until
  their phase.
- **Container naming**: our services = `fintech_<name>_service`, third-party
  = `fintech_<thing>`. Compose service keys stay short (DNS discovery).
- **Layering rule** (§3.5 TЗ): `Router → Controller → Service (UseCases) →
  Repository → Domain`. Business/domain code must NOT import FastAPI or
  SQLAlchemy. ORM only behind repository interfaces.

## Hard rules
- **Event catalog is canonical** (`doc/tz_v2.md` §3.11.1). Every
  producer/consumer MUST import event classes from `shared/domain/events.py`
  (`FundsReserved`, `FundsDebited`, `SagaFailed`, …). **Never** use `*Posted`
  synonyms (`ReservePosted`/`DebitPosted` — banned, v2 #10).
- **Port map is fixed** (`doc/tz_v2.md` §4.1): gateway 8000, intake 8001,
  orchestrator 8002, ledger 8003, profile 8004, notification 8005,
  mock-acquirer 8006, **mock-fraud 8007**, frontend 5173, grafana 3000,
  tempo 3200/4317, kafka-connect 8083, kafka ext 29092. Any drift = bug.
- **Own-currency only** (v2 #5): `debit` requires `amount.currency ==
  account.currency`. Cross-currency is out of scope.
- **Boundaries**: a service never reads another service's DB directly —
  only via Kafka/events. No cross-schema JOINs.
- **Circuit breaker** = `aiobreaker` via `shared/circuit.py`
  (`CircuitBreakerHttpClient`, `cb.call_async(func)` — no `async with`).

## How to run / verify (Windows → Git Bash; `make` needs bash)
```bash
make up          # docker compose up -d --build  (18 persistent + 2 one-shot)
make ps
make smoke       # bash scripts/smoke_env.sh  → SMOKE OK
make test        # python -m pytest -q        (22 shared tests + any you add)
make logs svc=<service-name>   # follow one service
make down        # stop, keep volumes
make clean       # down -v (remove volumes)
```
UI: Grafana `http://localhost:3000` (admin/admin, dashboard
`payments-overview`) · Tempo `:3200` · Prometheus `:9090`.

Tests: put shared/unit tests in `tests/shared/` (pytest + pytest-asyncio are
devdeps); new phase tests go under `tests/<phase>/`. Run the full suite
(`make test`) before declaring done — do not skip.

## Git workflow (protected `main`, PR-only)
- Branch per phase: `feat/0<number>-<name>` (e.g. `feat/03-ledger`).
- **Conventional Commits** with body (what / why / §TЗ / ADR). 3–8 commits
  per phase.
- No force-push, no amend on pushed commits. Open a PR → review → merge.
- After each phase: add a report to `doc/done/<NN_phase>.md`, an ADR per
  non-trivial decision in `doc/adr/ADR-0NN-*.md`.

## When in doubt
- Architecture / decisions → `doc/tz_v2.md` first, then `doc/adr/`.
- What's next / how → `doc/implementation_plan.md` (§matches the phase).
- How phase 1 was actually built (deviations!) → `doc/done/01_*.md`.
- Code search: prefer the codebase-memory index (project `fintech_demo`) —
  `search_graph` for symbols, `trace_path` for callers/impact, `search_code`
  for text. It's faster and structurally ranked.
- New dependency or stack swap → write an ADR, don't just change `pyproject.toml`.
