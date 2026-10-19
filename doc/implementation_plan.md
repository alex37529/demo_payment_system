# План реализации FinTech PoC (по `tz_v2.md`)

> **Роль:** CTO
> **База:** `doc/tz_v2.md`
> **Старт:** чистый репозиторий (`doc/` + `.idea`)
> **Цель:** PoC из 8 сервисов + Kafka(KRaft) + PG×4 + Redis + Observability — полностью поднятый в Docker, покрытый тестами и acceptance-критериями §7 TЗ v2.

---

## 0. Принятые допущения (зафиксированы)

| Параметр | Значение |
| --- | --- |
| Платформа | Docker Desktop (Linux containerd), min 8 GB RAM, 20 GB disk |
| Monorepo | `fintech_demo/` как корень; один `pyproject.toml` (workspace: `shared` + `services/*`); один `docker-compose.yml`; один `Makefile` |
| Python | 3.11-slim (единый base image) + **`curl`** (apt-get в Dockerfile) — для healthcheck и smoke-скриптов |
| Kafka | **Oфициальный KRaft single-node** (1 контейнер, образ `apache/kafka:3.7.0`) + Kafka Connect (Confluent `cp-kafka-connect:7.6`) + Debezium |
| State machine | **python-statemachine** (зафиксирован, ADR-006) |
| Circuit breaker | **aiobreaker** (async-нативный; обёртка `CircuitBreakerHttpClient` в `shared/circuit.py`; ADR-004) |
| Frontend | **React 18 + Vite + TypeScript** (порт **5173**, не 3000 — т.к. 3000 = Grafana) |
| Scope | Own-currency, 20 юзеров, 1 кластер Kafka, 1 PG, 1 Redis |
| Git | Conventional Commits, `main` защищён, PR-only |
| Карта портов | Зафиксирована в `tz_v2.md §4.1` (v2 decisions #12) |
| Event catalog | Канон в `tz_v2.md §3.11.1` (v2 decisions #10) |

---

## 1. Модель работы с git

```
main                 (protected, PR-only, tags v*)
  ├─ feat/01-infra         ← Этап 1 (инвайрмент + shared + placeholders)
  ├─ feat/03-ledger        ← Этап 2 (write-side)
  ├─ feat/04-saga          ← Этап 3 (оркестратор / saga)
  ├─ feat/05-intake        ← Этап 4 (приём + idempotency)
  ├─ feat/06-read-side     ← Этап 5 (read-side + SSE)
  ├─ feat/07-mocks-fe      ← Этап 6 (mocks + UI)
  ├─ feat/08-obs-tests     ← Этап 7 (observability + tests)
  └─ release/01            ← Этап 8 (tag + acceptance demo)
```

Правила:
- `main` защищён, только merge PRs.
- Ветка фазы живёт до merge; внутри — 3–8 коммитов с Conventional Commits.
- Каждый коммит: subject + body (what / why / § TЗ / ADR).
- Без force-push и `--amend` на pushed.

---

## 2. Этапы и exit-критерии

| Этап | Содержание | Ветка | Exit criterion |
| --- | --- | --- | --- |
| **1** | Инвайрмент Docker + shared-абстракции + 8 placeholder-сервисов | `feat/01-infra` | `make up` → **17/17** running (7 infra + promtail + 8 svc, см. `tz_v2.md §4.1`); `from shared.domain import AggregateRoot` OK (alias `Aggregate` в `__init__.py`); 8×`/healthz`=200 |
| **2** | Ledger (write-side) + Transactional Outbox | `feat/03-ledger` | `debit` → баланс−X, outbox+1, `ledger.events`+1 (событие `FundsDebited`, каталог в `tz_v2 §3.11.1`) |
| **3** | Orchestrator / Saga + CB (aiobreaker) + DLQ + compensation | `feat/04-saga` | happy→Success (`FundsDebited`); fraud-fail→CB open, `ledger.commands.dlq`↑, valid≥10 TPS |
| **4** | Intake + Idempotency | `feat/05-intake` | дубль `Idempotency-Key` → 1 событие в Kafka |
| **5** | Read-side + SSE | `feat/06-read-side` | UI видит обновление ≤2s; SSE auto-reconnect |
| **6** | Mocks (acquirer + fraud) + React UI | `feat/07-mocks-fe` | load@50 TPS 60s green |
| **7** | Observability (E2E) + tests + docs/ADR | `feat/08-obs-tests` | Tempo: Gateway→Ledger; Grafana dashboard; alerts |
| **8** | Acceptance + release | `release/01` | `v0.1.0`; 7 checks §7 TЗ v2 |

---

## 3. Этап 1 — Инвайрмент + абстракции (детально)

**Выход:** `make up` → 18/18 running; shared-модуль импортируется; 8 placeholder-сервисов отвечают на `/healthz`.

### 3.1. Задачи

| # | Задача |
|---|---|
| 1.1 | Init monorepo: `.gitignore`, `pyproject.toml`, `README.md` (stub), `Makefile`, структура `services/*`, `shared/*` |
| 1.2 | Docker Compose core data stack: `postgres:16` (4 базы через `init/*.sql`), `redis:7`, Kafka **`apache/kafka:3.7.0` KRaft** single-node, kafka connect (Confluent) |
| 1.3 | Debezium: image + PG connector (4 базы, `wal_level=logical`) |
| 1.4 | Observability: prometheus, alertmanager, tempo, loki, grafana (provisioning), config |
| 1.5 | Health checks + `depends_on.condition=service_healthy` |
| 1.6 | 8 placeholder-сервисов (FastAPI + `/healthz` + instrumentation) |
| 1.7 | shared/domain: `Aggregate`, `Entity`, `ValueObject`, `DomainEvent` (base DDD) |
| 1.8 | shared/domain: event catalog (`PaymentCreated`, `FundsReserved`, `FundsDebited`, `SagaFailed`, …) по §3.11 v2 |
| 1.9 | shared/db: async SQLAlchemy 2.0 base + `IAccountRepository`/`IPaymentRepository`/`IOutboxRepository` |
| 1.10 | shared/outbox: `OutboxEvent` + `OutboxMixin` (event+payload в той же TX) |
| 1.11 | shared/kafka: Producer/Consumer (retry + exp backoff + jitter + 3× DLQ) |
| 1.12 | shared/circuit: `CircuitBreakerHttpClient` (pybreaker, fallback) |
| 1.13 | shared/stats: `PaymentStateMachine` (python-statemachine, mapping §3.4 v2) |
| 1.14 | shared/observability: OTel→Tempo, `FastAPIInstrumentator`, JSON→Loki |
| 1.15 | shared/idempotency: `IdempotencyMiddleware` (Redis `SET NX EX` + PG fallback) |
| 1.16 | shared `py.typed` + smoke-тесты (import + invariants) |
| 1.17 | Makefile: `up`, `down`, `logs <svc>`, `ps`, `seed`, `test`, `load`, `acceptance`, `clean` |
| 1.18 | `scripts/smoke_env.sh` (curl `/healthz`×8, list топики, PG-базы) |
| 1.19 | ADR-001 «Monorepo layout», ADR-002 «Kafka KRaft single-node» |

### 3.2. Порядок коммитов

```
chore:      init monorepo (pyproject, makefile, .gitignore, doc/, tree)
feat(infra): core data stack — postgres(4 bases), redis7, kafka apache/kafka KRaft, kafka connect
feat(infra): Debezium PG connector (wal_level=logical)
feat(infra): observability — prometheus, tempo, loki, grafana, alertmanager
feat(infra): healthchecks + depends_on.condition=service_healthy
feat(shared): DDD base (Aggregate/Entity/ValueObject) + domain event catalog
feat(shared): async repositories + Transactional Outbox model
feat(shared): kafka client (retry + exp backoff + jitter + 3x DLQ)
feat(shared): CircuitBreaker HTTP wrapper (pybreaker, fallback)
feat(shared): PaymentStateMachine (python-statemachine, mapping §3.4)
feat(shared): observability kit (OTel->Tempo, prometheus, loki JSON)
feat(shared): IdempotencyMiddleware (Redis SETNX EX + PG fallback)
feat(services): 8 placeholder services on shared framework
chore:        make targets (up/down/logs/seed/test/load/acceptance) + smoke script
docs:         ADR-001 monorepo, ADR-002 KRaft
```

### 3.3. Явно НЕ делаю в Этапе 1
- Бизнес-логику (агрегаты/сага) — Этапы 2–3.
- Реальный фронтенд (только `/healthz`) — Этап 6.
- Load-харнес — Этап 7.

---

## 4. Этап 2 — Ledger (Write-Side)

**Выход:** `POST /ledger/v1/accounts/{id}/debit` → баланс убавлен + outbox row + `ledger.events` в Kafka.

| # | Задача |
|---|---|
| 2.1 | Domain: `Account` (Aggregate Root, currency, balance, `debit(amount)`, инвариант `balance >= amount` + currency match), `Payment` (Aggregate Root) |
| 2.2 | Domain events (импорт из `shared.domain.events`, каталог `tz_v2 §3.11.1`): `FundsReserved`, `FundsDebited`, `CancelReservationPosted`, `RefundPosted` |
| 2.3 | Application: `UseCase_Reserve/Debit/CancelReservation/Refund` (каждый в TX с outbox) |
| 2.4 | Infra: `AccountRow`, `PaymentRow`, `OutboxEventRow`, `AccountRepository`, `PaymentRepository` |
| 2.5 | API: `POST /v1/accounts/{id}/reserve/\|debit\|refund\|cancel-reservation` |
| 2.6 | Consumer `ledger.commands` → диспетчер на use cases |
| 2.7 | Debezium: `outbox_events` → `ledger.events` (payload §3.11 v2) |
| 2.8 | Unit tests: инварианты агрегатов (100% переходов + negative) |
| 2.9 | ADR-003 «Own-currency invariant» |

---

## 5. Этап 3 — Orchestrator / Saga

**Выход:** happy-path `Create→Success`; failure: `fraud/admin/fail {errors}` → CB open, DLQ↑, valid≥10 TPS.

| # | Задача |
|---|---|
| 3.1 | Domain: `Saga` (Aggregate: id, payment_id, state, history, compensation_chain), `SagaStep` |
| 3.2 | Saga state machine: `Created→Checking→Reserve→Fraud→Debit→Success` + `Failed`/`RolledBack` |
| 3.3 | Infra: `SagaRow`, `SagaRepository` (БД `orchestrator`) |
| 3.4 | Application: `SagaHandler` (consumer `payments.commands`) → команд в `ledger.commands` |
| 3.5 | Fraud step через `CircuitBreakerHttpClient` + DLQ + fallback |
| 3.6 | Compensation chain: err@Debit→`CancelReservation`→`RolledBack` |
| 3.7 | API `GET /v1/sagas/{id}` (статус) |
| 3.8 | Integration test: happy + failure (fraud fail→DLQ) с Testcontainers |

---

## 6. Этап 4 — Payment Intake

**Выход:** `POST /v1/payments` с дублем `Idempotency-Key` → 2-й кэширован, 1 событие в `payments.commands`.

| # | Задача |
|---|---|
| 4.1 | DTOs: `CreatePaymentRequest/Response` (Pydantic v2) |
| 4.2 | Idempotency: `IdempotencyMiddleware` + fallback-копия в `intake.idempotency_keys` |
| 4.3 | Producer `payments.commands` (payload §3.11 v2) |
| 4.4 | API `POST /v1/payments`, `GET /v1/payments/{id}` |
| 4.5 | Unit: идемпотентность (2 одинаковых → 1 событие в Kafka) |

---

## 7. Этап 5 — Read-Side

**Выход:** UI видит баланс-обновление и latest payment ≤2s после `POST /v1/payments`.

| # | Задача |
|---|---|
| 5.1 | Profile: `UserRow` (read model), seed 20 юзеров (миграция БД `profile`) |
| 5.2 | Consumer `ledger.events` → update `balances` |
| 5.3 | API `GET /v1/users` |
| 5.4 | Notification/Payment Log: in-memory ring (+ SQLite-опция), consumer `ledger.events` |
| 5.5 | SSE `GET /v1/stream` (EventSource, keepalive 15s, auto-reconnect) |
| 5.6 | Integration: SSE reconnect при kill Notification |

---

## 8. Этап 6 — Mocks + Frontend

**Выход:** UI live-обновляется; load@50 TPS держится 60s.

| # | Задача |
|---|---|
| 6.1 | Mock Acquirer: `POST /acquirer/generate` (валидный/невалидный) |
| 6.2 | Mock Acquirer: load mode @ 50 TPS (async loop) |
| 6.3 | Mock Fraud: `POST /fraud/check` + `POST /fraud/admin/fail {off\|errors\|timeout}` |
| 6.4 | Frontend: Vite + React 18 + TS scaffold, docker service |
| 6.5 | UI: таблица 20 юзеров (`/v1/users`) + лента платежей (`/v1/stream` SSE) |
| 6.6 | UI: CB-статус badge (poll Gateway), SSE auto-reconnect |
| 6.7 | Gateway/BFF: SSE-терминал, routing `/v1/*`, proxy к сервисам |

---

## 9. Этап 7 — Observability + Tests + Docs

**Выход:** Tempo-трейс Gateway→Ledger; Grafana dashboard `payments-overview`; Alertmanager rules.

| # | Задача |
|---|---|
| 7.1 | OTel: span propagation Gateway→Intake→Orchestrator→Ledger |
| 7.2 | Prometheus per-service: http duration, kafka lag, CB open, DLQ depth |
| 7.3 | Loki: JSON structured logs (per-svc), retention 7d |
| 7.4 | Grafana: dashboard `payments-overview` (p50/p99, RPS, DLQ, CB, lag) |
| 7.5 | Alertmanager: p99>500ms/3min, CB open>0, DLQ depth>10 |
| 7.6 | Integration: outbox→Debezium→Kafka→consumer (E2E, Testcontainers) |
| 7.7 | Load: `scripts/load_test.py` (50 TPS/60s, замер P99) |
| 7.8 | ADR-004..010 (CB lib, statemachine lib, KRaft, own-currency, Redis idem) |
| 7.9 | C4 diagram (Context + Container) |
| 7.10 | Sequence diagram: happy path + fraud-failure |
| 7.11 | `docs/events.md` — event contracts spec по §3.11 v2 |
| 7.12 | `docs/runbook.md` — up / load / fail-injection / CB / DLQ |
| 7.13 | README final (quick start, acceptance) |

---

## 10. Этап 8 — Release

**Выход:** tag `v0.1.0`; `make demo` проходит все acceptance criteria §7 v2.

| # | Задача |
|---|---|
| 8.1 | `scripts/acceptance.sh` (7 checks из §7 TЗ v2) |
| 8.2 | `make clean && make up && make smoke && make test && make load && make acceptance` |
| 8.3 | Tag `v0.1.0` + release notes |

---

## 11. Риски и митигация

| Риск | Влияние | Митигация |
|---|---|---|
| `apache/kafka` KRaft — JVM-heavy | Медленный `up`, OOM на 4GB | `KAFKA_HEAP_OPTS=-Xmx512m` (env `KAFKA_HEAP_OPTS`), single-broker+controller, 4 тофика + 3 DLQ |
| Debezium + 4 connector'а | Медленный start | 1 Debezium с 4 PG-connector'ами |
| Testcontainers на Docker Desktop (Win) | +30s первый запуск | Кэш образов в `docker-compose.test.yml` |
| 50 TPS на localhost | P99 <300ms может не пройти | Fallback: 30 TPS + зафиксировать в ADR |
| SSE over Docker → browser | Port-mapping | SSE (HTTP/1.1), не WS; browser auto-reconnect |
| `python:3.11-slim` нет `curl` | healthcheck сервисов + smoke-скрипт падают | `RUN apt-get update && apt-get install -y curl` в Dockerfile |
| Bitnami images убраны (~2025) | `bitnami/kafka` не тянется из Docker Hub | официальный `apache/kafka:3.7.0` (KRaft) + Confluent `cp-kafka-connect` |
| `pybreaker` не async | `async with cb` не существует | `aiobreaker` (нативный `async with`) |
| Кэш Vite 5173 vs 3000 | Frontend и Grafana конфликтуют по порту | Frontend = **5173** (Vite default), Grafana = **3000** — см. `tz_v2 §4.1` |
| OTel→Tempo (gRPC) | Таймауты | `OTEL exporter timeout=10s` + buffer |

---

## 12. Dev-команды

```
make up           # docker compose up -d + healthwait
make down         # docker compose down
make logs <svc>   # follow logs
make seed         # seed (profile 20 users)
make test         # unit + integration (+ --load опционально)
make load         # 50 TPS / 60s
make acceptance   # 7 checks §7 TЗ v2
make demo         # up + seed + load + fail-injection + screenshot
make clean        # down -v
```
