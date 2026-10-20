# fintech_demo — payments PoC (FinTech)

Event-driven payments demo showing best practices: **Saga orchestration**,
**Transactional Outbox** (WAL + Debezium CDC), **CQRS**, **DDD aggregates**,
**Circuit Breaker**, **Idempotency**, **Distributed tracing**. Full TЗ:
`doc/tz_v2.md`, plan: `doc/implementation_plan.md`, phase docs: `doc/phases/`.

## What it does

A payment enters at the **Intake** (idempotently), the **Orchestrator** runs
a saga (limits → reserve → fraud → debit), the **Ledger** moves the money
and publishes the event through a **Transactional Outbox** (Debezium ships
the Postgres WAL to Kafka), and the **read-side** (Profile, Notification)
projects the new balance and streams it to the browser over SSE. Failures
are contained by a **circuit breaker** and parked in a **DLQ** — never lost.

```
Browser → Gateway/BFF → Intake → Kafka → Orchestrator → Kafka → Ledger → Postgres
                                              ↓ fraud (CB)              ↓ WAL
                                          mock-fraud              Debezium → Kafka
                                                                             ↓
                                                        Profile + Notification → SSE
```

## Phases 1–7 — what is built

| Phase | What it added |
| --- | --- |
| 1 | The environment + `shared/` (DDD base, event catalogue, async DB, outbox, Kafka client, CB, state machine, observability, idempotency) |
| 2 | **Ledger** write-side: balance aggregates, Transactional Outbox, idempotency guard |
| 3 | **Orchestrator**: saga + state machine + watchdog + compensation + DLQ |
| 4 | **Intake**: idempotent payment creation (`Idempotency-Key`) |
| 5 | **Read-side**: Profile (balance projection) + Notification (SSE feed) |
| 6 | **Mocks + frontend + gateway BFF**: load generator, fraud failure injection, React UI |
| 7 | **Observability**: end-to-end traces, custom metrics, the `payments-overview` dashboard, alert rules, the E2E suite, the load harness |

## Stack
Python 3.11 · FastAPI · SQLAlchemy 2.0 (async) · Pydantic v2 · aiokafka ·
aiobreaker · python-statemachine · OpenTelemetry → Tempo · Prometheus · Loki.

## Layout
```
shared/          # DDD + db + outbox + kafka + circuit + state_machine + observability + metrics + tracing
services/<name>/ # 8 FastAPI apps (gateway, intake, orchestrator, ledger, profile, notification, 2 mocks)
docker-compose.yml
kafka/           # topics.sh + Debezium connector + register script
init/            # 01_databases.sql (4 bases + outbox + publication + seed)
observability/   # prometheus / tempo / loki / promtail / grafana / alertmanager
frontend/        # React 18 + Vite + TS
scripts/         # smoke_env.sh, acceptance.sh, load_test.py, register_connector.py
doc/             # TЗ v2, plan, phases, ADRs, diagrams, events.md, runbook.md
tests/           # unit suites + tests/e2e (the live-stack E2E)
```

### Container naming (deliberate: "ours" vs third-party)
Every container has an **explicit** `container_name` (no more `loki-1`, `gateway-1`);
the `fintech_` prefix separates them from unrelated dockers on the host.

| Group | Container names |
| --- | --- |
| **Our services** (8) | `fintech_gateway_service`, `fintech_intake_service`, `fintech_orchestrator_service`, `fintech_ledger_service`, `fintech_profile_service`, `fintech_notification_service`, `fintech_mock_acquirer_service`, `fintech_mock_fraud_service` |
| Our one-shot helpers | `fintech_kafka_topics_init`, `fintech_debezium_register` |
| Our frontend | `fintech_frontend` |
| Third-party infra | `fintech_postgres`, `fintech_redis`, `fintech_kafka`, `fintech_kafka_connect`, `fintech_prometheus`, `fintech_tempo`, `fintech_loki`, `fintech_promtail`, `fintech_alertmanager`, `fintech_grafana` |

> The compose **service keys** (DNS) keep the short names (`gateway`, `ledger`, `tempo`, …) —
> services discover each other by those; `container_name` only affects `docker ps` / `docker logs`.

## Run
```bash
# Requires Docker (Linux containerd), >= 8 GB RAM, Git Bash (Windows).
docker compose up -d --build      # or: make up
docker compose ps                 # 18/18 running
bash scripts/smoke_env.sh         # or: make smoke  (healthz x8 + topics + PG + redis + Debezium + Prometheus)
python -m pytest -q               # or: make test   (the hermetic unit suite)
```

### One command: the whole demo + the acceptance
```bash
make demo
# → clean stack → up → smoke → seed → acceptance (7 criteria / 27 checks)
# → "ACCEPTANCE: 27 passed, 0 failed" + "ALL ACCEPTANCE CRITERIA MET"
```
The stack is left running afterwards (UI on :5173, Grafana on :3000).
`make acceptance` runs the same checks against the stack that is already up.

### Quick checks
```bash
curl http://localhost:8000/healthz            # {"status":"ok","service":"gateway"}
curl -s http://localhost:8000/metrics | grep http_request
curl -s http://localhost:8083/connectors/ledger-outbox/status   # ... "state":"RUNNING"
# UI: Grafana  http://localhost:3000  — dashboard "payments-overview"
# Tempo:       http://localhost:3200   ·   Prometheus: http://localhost:9090
```

> **Tempo (:3200) и Loki (:3100) отдают 404 на `/` — это нормально:** у них нет
> собственного UI. Трейсы и логи смотрят в Grafana → Explore → Tempo / Loki.
> Alertmanager (:9093) показывает только **срабатывающие** алерты — список пуст,
> пока ни одно правило не в статусе `firing`.

### Grafana — пароль не придётся менять заново

**Открывается без логина:** <http://localhost:3000> — анонимный доступ
включён (стек слушает только localhost). Если предпочитаешь вход —
`admin` / `admin`.

Пароль **детерминирован**: он берётся из `.env`
(`GRAFANA_ADMIN_PASSWORD`), а не из UI. Поэтому:

| Событие | Что с паролем |
| --- | --- |
| `make up` (ребилд) | сохраняется (volume цел) |
| `make clean` (volume снесён) | **тот же**, что в `.env` — пересоздаётся с ним |
| смена в UI | переживает ребилд, но **не** переживёт `make clean` |

Менять его правильно — **здесь**, а не в UI:

```bash
make setup                                # создаёт .env из .env.example
make grafana-password PASSWORD=my-pass    # пишет в .env И меняет в живой Grafana
```

Скрипт меняет пароль в обоих местах (через `grafana cli admin
reset-admin-password` — текущий пароль знать не нужно), так что `.env` и
живая Grafana никогда не расходятся.

> Brute-force защита Grafana **отключена** (`GF_SECURITY_DISABLE_BRUTE_FORCE_LOGIN_PROTECTION`):
> по умолчанию 5 неудачных попыток за 5 минут **блокируют** аккаунт на 5 минут,
> и пока он заблокирован — даже **правильный** пароль отклоняется («Invalid username
> or password»). На локальном демо это чистый грабли.

### The demo in 4 commands
```bash
make load                                  # 50 TPS / 60 s -> the UI feed flies
curl -X POST http://localhost:8007/fraud/admin/fail -d '{"mode":"errors"}'
curl http://localhost:8000/v1/_status      # {"circuit_breakers":{"fraud":{"state":"OPEN"}}}
curl -X POST http://localhost:8007/fraud/admin/fail -d '{"mode":"off"}'
```

More in **[doc/runbook.md](doc/runbook.md)**.

### Ports (see `doc/tz_v2.md §4.1`)
| svc | host | | infra | host |
| --- | --- | | --- | --- |
| gateway | 8000 | | postgres | 5432 |
| intake | 8001 | | redis | 6379 |
| orchestrator | 8002 | | kafka (external) | 29092 |
| ledger | 8003 | | kafka-connect | 8083 |
| profile | 8004 | | prometheus | 9090 |
| notification | 8005 | | alertmanager | 9093 |
| mock-acquirer | 8006 | | tempo | 3200 / 4317 |
| mock-fraud | 8007 | | loki | 3100 |
| frontend | 5173 | | **grafana** | 3000 |

## Teardown
```bash
docker compose down            # make down
docker compose down -v         # make clean
```

## Limits (the PoC boundaries)

| Ограничение | Почему | Что вместо |
| --- | --- | --- |
| KRaft single-node | PoC | HA — infra-задача |
| In-memory state (режим fraud, sagas) | сбрасывается при restart | Redis / БД |
| Own-currency (кросс-валюта вне скоупа) | tz_v2 §3.10, ADR-003 | FX-модель — v0.2 |
| 24 h TTL идемпотентности | контракт §3.1 | длиннее — v0.2 |
| Логи/трейсы: retention 72 h (Loki) / 3 d (Tempo) | PoC | больше — v0.2 |
| Нет AuthN/AuthZ (Grafana — анонимный Admin) | local | OAuth2/RBAC — v1.0 |
| Chaos-тесты = kill контейнера | PoC | toxiproxy — v0.2 |

## Docs
| Document | What it is |
| --- | --- |
| [`doc/tz_v2.md`](doc/tz_v2.md) | the TЗ (the requirements) |
| [`doc/implementation_plan.md`](doc/implementation_plan.md) | the phase plan |
| [`doc/phases/`](doc/phases) | the per-phase specs |
| [`doc/done/`](doc/done) | the per-phase delivery reports |
| [`doc/adr/`](doc/adr) | the architecture decision records |
| [`doc/events.md`](doc/events.md) | the event contracts (topics, payloads) |
| [`doc/runbook.md`](doc/runbook.md) | operations: logs, metrics, traces, failure injection, DLQ |
| [`doc/diagrams/`](doc/diagrams) | C4 + sequence diagrams |
