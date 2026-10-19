# Отчёт — Этап 1. Инвайрмент + все абстракции (shared)

- Дата: 2026-10-19
- Ветка: `feat/01-infra`
- Статус: **Готов / проверен (SMOKE OK, 22 теста pass)**
- База: `doc/tz_v2.md` (ТЗ), `doc/implementation_plan.md` (план §3), `doc/phases/01_environment_abstractions.md`

---

## 1. Что сделано

`make up` / `docker compose up -d --build` поднимает полный стек. `shared/` — единый набор
абстракций, который импортируют все 8 placeholder-сервисов. Выход этапа — живой, но «пустой»
скелет: 18 сервисов отвечают `/healthz`/`/metrics`, infra-компоненты healthy.

### 1.1. Строк (20 явных контейнеров)
| Группа | Контейнер |
| --- | --- |
| Наши сервисы (8) | `fintech_gateway_service` … `fintech_mock_fraud_service` (суффикс `_service`) |
| Наши one-shot | `fintech_kafka_topics_init`, `fintech_debezium_register` |
| Третьи (10) | `fintech_postgres`, `fintech_redis`, `fintech_kafka`, `fintech_kafka_connect`, `fintech_prometheus`, `fintech_tempo`, `fintech_loki`, `fintech_promtail`, `fintech_alertmanager`, `fintech_grafana` |

> Именование: `fintech_` + (наши → `_service`); ключи compose (DNS) — короткие для discovery.

### 1.2. Shared (ядро)
| Модуль | Что внутри | Примечания |
| --- | --- | --- |
| `shared/domain/` | `ValueObject`(frozen, eq по значению), `Entity`, `AggregateRoot`(pull_events) + alias `Aggregate`; каталог из 10 событий (`FundsReserved/Debited`, `CancelReservationPosted`, `RefundPosted`, `FraudVerdict`, `DebitFailed`, `SagaSucceeded/Failed`, …) | канон `tz_v2 §3.11.1` |
| `shared/db/` | `Base`, `make_engine`, `make_session_factory`, интерфейсы `IAccount/IPayment/IOutboxRepository` | `expire_on_commit=False` |
| `shared/outbox.py` | `OutboxEvent` + `OutboxMixin._publish_outbox` | 1 TX: мутация + outbox |
| `shared/kafka.py` | `KafkaProducer/Consumer` (lazy init), retry + exp backoff + jitter, DLQ `<topic>.dlq` | aiokafka требует event loop |
| `shared/circuit.py` | `CircuitBreakerHttpClient` (aiobreaker `call_async`), fallback, `state()/is_open()` | ADR-004, `pybreaker_open` |
| `shared/state_machine.py` | `PaymentStateMachine` (Created→Validating→Processing→Success/Failed/RolledBack) + guards + `restore()` | python-statemachine v3 |
| `shared/observability.py` | OTel→Tempo (gRPC 4317), `/metrics` (Instrumentator), JSON-логи в stdout | `/healthz`, `/_status` |
| `shared/idempotency.py` | `IdempotencyMiddleware` (Redis `SET NX EX`), двойной TTL | 120s pending / 24h ответ |

### 1.3. Инфраструктура (compose)
- **PostgreSQL × 4 базы** (`ledger`, `orchestrator`, `intake`, `profile`), `wal_level=logical`,
  роль `fintech … REPLICATION`, таблица `outbox_events` + publication.
- **Kafka KRaft single-node** (`apache/kafka:3.7.0`), 4 топака + 3 DLQ (create-скрипт sidecar).
- **Debezium CDC** (`debezium/connect:2.5`): `outbox_events` → топик `ledger.events`
  (RegexRouter + `ExtractNewRecordState`).
- **Observability**: Prometheus (9 targets), Tempo (OTLP 4317), Loki+promtail, Grafana
  (dashboard `payments-overview`), Alertmanager.

### 1.4. Сервисы (8 placeholder)
Один `Dockerfile` (build-arg `SERVICE_NAME`, `curl` + `tini`, HEALTHCHECK),
`services/main.py` резолвит модуль по имени (в т.ч. `mock-acquirer`/`mock-fraud`).
Каждый: `/healthz`, `/metrics`, `/_status`, JSON-лог с `service=` + `trace_id`.

### 1.5. Тесты
`tests/shared/` — **22 pass**: domain (eq по значению, hash, freeze, pull_events), outbox,
event-catalog, circuit (open after fail_max, 5xx=failure), state machine (happy/fail/rollback,
guards, restore), infra (import, DLQ-имя, idempotency TTL).

---

## 2. Как проверяли (реально проведено)

| Критерий | Результат |
| --- | --- |
| 18 persistent контейнеров | 18 running (8 svc + 10 infra), 2 one-shot `Exited (0)` |
| 8× `/healthz` = 200 | ✓ все 8 |
| 4 топака + 3 DLQ | ✓ `kafka-topics.sh --list` |
| 4 БД + `wal_level=logical` | ✓ `psql` |
| Redis `PONG` | ✓ |
| Debezium `ledger-outbox` = **RUNNING** | ✓ |
| **CDC end-to-end**: `INSERT outbox_events` → запись в `ledger.events` | ✓ (`FundsDebited` payload) |
| Prometheus: ≥ 8 UP targets | 9 UP |
| `python -m pytest` | 22 pass |
| `bash scripts/smoke_env.sh` | **SMOKE OK** |

---

## 3. Что НЕ делали (по §3.3 плана)
- Бизнес-логику (агрегаты/сага) — Этапы 2–3.
- Реальный фронтенд (только `/healthz`) — Этап 6.
- Load-харнес — Этап 7.

---

## 4. Замечания / отклонения от фазы 01
1. **aiobreaker** не имеет `async with` (нет `__aenter__`) — используется `cb.call_async(func)`.
   Сниппет `async with cb` из фазы 01 заменён; ADR-004 зафиксирован.
2. **Kafka**: официальный `apache/kafka:3.7.0` вместо `bitnami` (ADR-002).
3. **Дебазиум**: `debezium/connect` (включает PG-коннектор + SMT). Коннектор регистрируется
   one-shot контейнером `fintech_debezium_register` (идемп.) через REST.
4. **Prometheus**: `prom/prometheus:v*` (тег `v2.53.0`), `prom/alertmanager:v0.27.0`.
5. **Tempo 2.5**: конфиг `storage.trace` (без `tempodb`), OTLP gRPC 4317.
6. Раздел «Лог-структура Loki-лейблов» и Tempo-search — по фазе 7 (фазе 1 достаточен
   «стек up + healthz + shared import» — выполнено).

---

## 5. АDR
- ADR-001 — Monorepo layout (`doc/adr/ADR-001-monorepo.md`)
- ADR-002 — Kafka KRaft single-node (`doc/adr/ADR-002-kraft.md`)
- ADR-004 — Circuit breaker `aiobreaker` (`doc/adr/ADR-004-circuit-breaker.md`)

---

## 6. Команды воспроизведения
```bash
docker compose up -d --build          # make up
docker compose ps                     # 18/18 running (2 one-shot exited 0)
bash scripts/smoke_env.sh             # make smoke  -> SMOKE OK
python -m pytest -q                   # make test   -> 22 passed
docker compose down -v                # make clean
# UI: Grafana http://localhost:3000 (admin/admin)  Tempo :3200  Prometheus :9090
```
