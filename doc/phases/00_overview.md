# Подробные документы по этапам реализации

> **Назначение:** поглотить каждую фазу проекта — цели, архитектуру, ключевой код, подводные камни, порядок коммитов, как проверить.
> **База:** `doc/tz_v2.md` (ТЗ) + `doc/implementation_plan.md` (план).
> **Аудитория:** разработчик/инженер, который должен реализовать PoC и его понимать.

## Как читать

Каждый документ самодостаточен: «что делаем → зачем → архитектура → код → коммиты → как проверить → типичные ошибки». Читай по порядку 01 → 08, это путь сборки.

## Карта документов

| Файл | Этап | Ветка | Одна фраза про результат |
| --- | --- | --- | --- |
| `01_environment_abstractions.md` | 1 | `feat/01-infra` | Docker-инвайрмент (PG×4, Redis, Kafka-KRaft, Debezium, Obs) + все shared-абстракции (DDD, outbox, Kafka, CB, state machine, idempotency) + 8 placeholder-сервисов |
| `02_ledger_write_side.md` | 2 | `feat/03-ledger` | Write-side: агрегаты `Account`/`Payment`, use-cases, Transactional Outbox → `ledger.events` через Debezium |
| `03_orchestrator_saga.md` | 3 | `feat/04-saga` | Saga-оркестратор: state machine, шаги, CircuitBreaker, DLQ, компенсации |
| `04_intake_idempotency.md` | 4 | `feat/05-intake` | Приём платежей: DTO, идемпотентность (Redis + PG), producer `payments.commands` |
| `05_read_side_sse.md` | 5 | `feat/06-read-side` | Read-side: read-модель профилей/балансов, Payment-Log ring, SSE-стрим |
| `06_mocks_frontend.md` | 6 | `feat/07-mocks-fe` | Mock Acquirer (load), Mock Fraud (fail-injection), React/Vite UI с SSE |
| `07_observability_tests.md` | 7 | `feat/08-obs-tests` | Сквозной tracing, metrics, alerting, Grafana dashboard, e2e + load-тесты, ADR/диаграммы |
| `08_release_acceptance.md` | 8 | `release/01` | Acceptance-скрипт (7 критериев §7 TЗ), tag `v0.1.0`, `make demo` |

## Путь данных (через все этапы)

```
Browser ──HTTP/SSE──▶ Gateway/BFF
   │
   ▼
Intake (idempotency) ──payments.commands──▶ Orchestrator (saga state machine)
                                              │  ledger.commands
                                              ▼
                                          Ledger (write-side, outbox)
       ┌──────────────┬──────────────────────────┘
       │              │  Debezium: outbox_events → ledger.events
       ▼              ▼
   Profile(read)   Payment-Log/SSE ──▶ Browser
```

## Сокращения

- **WAL** — Write-Ahead Log (PostgreSQL).
- **CDC** — Change Data Capture (Debezium читает WAL).
- **Outbox** — таблица событий, пишется в той же TX, что и изменение данных; Debezium «докидывает» её в Kafka.
- **CB** — Circuit Breaker.
- **DLQ** — Dead Letter Queue.
- **SSE** — Server-Sent Events.
- **BFF** — Backend-For-Frontend.
- **AGG** — Aggregate Root (DDD).

## Что даёт каждый этап следующему (зависимости)

```
01 (env+shared) ──▶ 02 (ledger) ──▶ 03 (saga)
        └───────────────────────────▶ 04 (intake)
        └── 02 ──▶ 05 (read-side) ──▶ 06 (UI) ──▶ 07 (obs/tests) ──▶ 08 (release)
```

Этап 1 — фундамент, без него ничего не собирается. Этапы 2 и 4 параллелезна, но в плане идут по очереди. 5–6 зависят от событий из 2–4. 7 и 8 — «обвязка» (наблюдаемость, тесты, релиз).
