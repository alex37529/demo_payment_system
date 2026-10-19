# Техническое задание: Архитектура и реализация демо-системы обработки платежей (FinTech)

> **Версия:** 2.0 (supersedes v1.0)
> **Уровень:** Architect / CTO / Tech Lead
> **Контекст:** Эталонный демо-проект (PoC) — демонстрация лучших практик построения отказоустойчивых, согласованных и наблюдаемых распределённых систем в FinTech-домене.

## Изменения v2 (что зафиксировано после ревью v1)

| # | Рубрика | Решение |
| --- | --- | --- |
| 1 | Idempotency | Хранилище — **Redis 7** (добавлен в стек), `SET NX EX` |
| 2 | Топология БД | **По БД на сервис** (4 base на одном PostgreSQL) |
| 3 | Лента платежей | **Payment Log Service = Realtime Notification Service** (один сервис, consumer `ledger.events`) |
| 4 | Fraud API | **Только заглушка** + admin-эндпоинт для failure-injection |
| 5 | Валюты | **Только own-currency** (кросс-валюта — out of scope) |
| 6 | SLO | **50 TPS, P99 < 300 mс** |
| 7 | Broker | **Kafka KRaft** (ZooKeeper исключён) |
| 8 | Saga-состояния | Явный маппинг шагов саги ↔ состояний Payment |
| 9 | Event contracts | Зафиксированы producer/consumer/payload по топикам (§3.11) |
| 10 | Canonical events | Единый каталог событий зафиксирован (§3.11.1); рассинхрон `ReservePosted/DebitPosted` отменён |
| 11 | DLQ-полнота | 3 DLQ-топика по командам (`payments.commands.dlq`, `ledger.commands.dlq`, `payments.events.dlq`) |
| 12 | Карта портов | Финальная карта портов зафиксирована (§4.1); `fraud` = 8007, `frontend` = 5173, Grafana = 3000 |

---

## 1. Архитектурное видение и обоснование (CTO Level)

Главная цель — не просто «написать код», а продемонстрировать глубокое понимание границ контекстов (Bounded Contexts), гарантий доставки сообщений и жизненного цикла транзакций.

Мы отходим от монолита в пользу **Event-Driven микросервисной архитектуры** с элементами **DDD**.

| Решение | Обоснование |
| --- | --- |
| **Saga Orchestration** | Распределённые транзакции без блокировок (2PC не подходит для микросервисов). Оркестратор — единая точка контроля за State Machine платежа. |
| **Transactional Outbox через WAL** | *Exactly-Once* на уровне БД, устранение Dual Write без синхронных вызовов Kafka из приложения. |
| **CQRS** | Разделение Write (строгий Ledger с Агрегатами) и Read (быстрые проекции для UI/SSE) → независимое масштабирование. |

---

## 2. Топология системы (8 сервисов)

### 2.1. Сервисы

| # | Сервис | БД | Роль |
| --- | --- | --- | --- |
| 1 | **API Gateway & BFF** | — (Redis для idempotency) | Единая точка входа; SSE-терминал; идемпотентность; маршрутизация |
| 2 | **Payment Intake Service** | `intake` | Приём запросов, валидация DTO, генерация `Idempotency-Key`, инициация. Не знает о списании |
| 3 | **Payment Orchestrator (Saga Manager)** | `orchestrator` | State Machine платежа; координация шагов саги; хранение состояния саги |
| 4 | **Ledger & Account Service (Write-Side)** | `ledger` | Агрегаты `Account`/`Payment`; CQRS Write; Transactional Outbox |
| 5 | **User & Profile Service (Read-Side)** | `profile` | Профили 20 юзеров; read-модель балансов; подписка на `ledger.events` |
| 6 | **Realtime Notification Service / Payment Log** | — (in-memory ring) | Consumer `ledger.events`; лента последних N платежей; push SSE |
| 7 | **Mock External Acquirer** | — | Генератор валидных/невалидных платежей (ошибки, таймауты) для нагрузочного теста |
| 8 | **Mock Fraud API (заглушка)** | — | Имитация фрод-проверки; admin-эндпоинт для failure-injection |

### 2.2. Топология БД (один PostgreSQL-контейнер, 4 базы)

```
postgres:
  ├─ ledger        # write-side: accounts, payments, outbox_events
  ├─ orchestrator  # saga_state, saga_steps
  ├─ intake        # idempotency_keys (персистентная копия поверх Redis), intake_audit
  └─ profile       # read-side: users, balances (denormalized)
```

> Границы контекстов совпадают с границами БД: ни один сервис не читает чужую БД напрямую. Обмен — только через Kafka/события.

### 2.3. Mock Fraud API (заглушка, scope-ограничение)

- Реализуется как минимальный HTTP-сервис (FastAPI), **без** реальной ML/правил.
- Endpoint `POST /fraud/check` → `{"decision": "approve" | "decline", "score": float}`.
- **Admin endpoint** `POST /fraud/admin/fail` с телом `{"mode": "off" | "errors" | "timeout"}`:
  - `off` — нормальный режим;
  - `errors` — отвечает `500` (триггерит Circuit Breaker → DLQ);
  - `timeout` — задерживает ответ > порога таймаута.
- Режим переключается в рантайме одной командой — это и есть механизм «сломать фрод» в acceptance-тесте.

---

## 3. Реализация паттернов (Best Practices)

### 3.1. Gateway API (SSE, BFF, Idempotency)

- **BFF** — адаптация данных для фронтенда.
- **Idempotency** — middleware в Gateway: `GET/SETNX` в **Redis** по `Idempotency-Key`; при повторе возвращает кэшированный ответ. Персистентная копия ключа дублируется в БД `intake` для аудита.
- **SSE** — эндпоинт `/api/v1/stream` держит коннекты, пушит обновления балансов и статусов.

### 3.2. Transactional Outbox (WAL + Debezium)

- **Реализация** — событие пишется в `outbox_events` в той же транзакции, что и изменение баланса.
- **Инфраструктура** — Debezium PostgreSQL Connector читает WAL (`wal_level=logical`) и публикует в Kafka. БД и Kafka — атомарно согласованы.

### 3.3. Saga Orchestration

- **Реализация** — оркестратор хранит состояние саги в БД `orchestrator`, отправляет команды, слушает события.
- **Шаги:** `CheckLimits → Reserve → FraudCheck → Debit`.
- **Компенсации:** при ошибке на любом шаге — `CancelReservation` / `Refund` по цепочке выполненных шагов.

### 3.4. Maппинг Saga ↔ State Machine (введено в v2)

| Шаг саги (команда) | Событие | Состояние Payment |
| --- | --- | --- |
| Intake → `Created` | `PaymentCreated` | `Created` |
| `CheckLimits` | `LimitsChecked` | `Validating` |
| `Reserve` | `FundsReserved` | `Processing` (sub-state: reserved) |
| `FraudCheck` | `FraudVerdict` | `Processing` |
| `Debit` | `FundsDebited` | `Success` |
| (любой сбой) | `SagaFailed` + компенсация | `Failed` / `RolledBack` |

> `Reserved` — под-состояние `Processing` (не отдельное топ-состояние), чтобы не раздувать диаграмму. Компенсация `CancelReservation` переводит из `Processing` в `RolledBack`.

### 3.5. Service Pattern (Layered / Clean Architecture)

```
Router → Controller → Service (UseCases) → Repository → Domain/Models
```

Бизнес-логика **не импортирует** FastAPI/SQLAlchemy (зависимость только в слою Repository-интерфейсов реализаций).

### 3.6. Repository Pattern

Интерфейс `IAccountRepository` / `IPaymentRepository` скрывает ORM. Позволяет мокать БД в юнит-тестах Агрегатов.

### 3.7. Circuit Breaker

- **Обоснование** — шаг `FraudCheck` зовёт внешнюю (mock) систему.
- **Реализация** — `aiobreaker` (async-нативный CircuitBreaker; `pybreaker` sync и не поддерживается). Обёртка `CircuitBreakerHttpClient` в `shared/circuit.py`. При `errors`/`timeout` от Fraud API CB открывается → **fallback**: скип-проверки логируется, платёж идёт в DLQ (не теряется), валидные — обрабатываются. Оркестратор не ложится.

### 3.8. Retry with Exponential Backoff & Jitter

- Применяется к Kafka-consumer-персистентности и HTTP-вызовам во внешние API.
- **Формула:** `delay = base * 2^attempt + random_jitter`.
- После `N` попыток → **DLQ** (отдельный топик `*.dlq`).

### 3.9. CQRS

- **Write Side** — Ledger Service: Агрегаты, инварианты, транзакции, Outbox.
- **Read Side** — User & Profile Service: денормализованные view `users(id, name, currency, balance)` + Notification Service: ring-буфер latest payments.

### 3.10. Aggregate (DDD)

- Агрегаты: `Account` (root), `Payment` (root).
- Изменение баланса — только через метод Агрегата `debit(amount)` с проверкой инварианта `balance >= amount`.
- **Валютная изоляция:** `debit` требует `amount.currency == self.currency`. Кросс-валютные списания — **out of scope** (v2 decision #5).

### 3.11. Event Contracts (введено в v2)

Топики (Kafka KRaft, 1 кластер):

| Топик | Producer | Consumer(s) | Payload (JSON) |
| --- | --- | --- | --- |
| `payments.commands` | Intake → Orchestrator | Orchestrator | `{payment_id, cmd: "Create", amount: {value, currency}, account_id, idem_key}` |
| `payments.events` | Orchestrator | Notification, Profile | `{payment_id, state, ts, saga_id?}` |
| `ledger.commands` | Orchestrator | Ledger | `{saga_id, payment_id, cmd: "Reserve"|"Debit"|"CancelReservation"|"Refund", amount}` |
| `ledger.events` | Ledger (через Debezium) | Profile (balances), Payment Log (ring) | `{account_id, payment_id, delta, new_balance, ts}` |
| `*.dlq` (3 шт) | все consumers | (инспекция) | исходное сообщение + причина отказа + attempt_count |

> Naming: `<bounded-context>.commands` / `<bounded-context>.events`. Commands — синхронно между сервисами в саге; events — async broadcast.

#### 3.11.1. Канонический каталог Domain Events (v2 decisions #10)

> **Правило:** имена ниже — канон. Любой сервис, который публикует/консомирует событие, обязан импортить класс из `shared/domain/events.py`. Любые отклонения (в т.ч. `ReservePosted`/`DebitPosted` из v1-черновиков) — запрещены.

| Класс | Топик (publish) | Producer | Key fields | Semantics |
| --- | --- | --- | --- | --- |
| `PaymentCreated` | `payments.events` | Intake | `payment_id, amount{value,currency}, account_id, idem_key, ts` | платёж принят, валидация DTO пройдена |
| `LimitsChecked` | `payments.events` | Orchestrator | `payment_id, saga_id, ts` | шаг `CheckLimits` завершён (успех) |
| `FundsReserved` | `ledger.events` (Debezium) | Ledger | `account_id, payment_id, saga_id, amount, ts` | средства зарезервированы (delta-only) |
| `FraudVerdict` | `payments.events` | Orchestrator | `payment_id, saga_id, decision: approve/decline/route_to_dlq, score, ts` | вердикт fraud (в т.ч. `route_to_dlq`) |
| `FundsDebited` | `ledger.events` (Debezium) | Ledger | `account_id, payment_id, saga_id, amount, new_balance, ts` | средства списаны (final) |
| `CancelReservationPosted` | `ledger.events` (Debezium) | Ledger | `account_id, payment_id, saga_id, amount, reason, ts` | резерв снят (компенсация) |
| `RefundPosted` | `ledger.events` (Debezium) | Ledger | `account_id, payment_id, saga_id, amount, reason, ts` | возврат средств (после Debit) |
| `DebitFailed` | `payments.events` | Orchestrator | `payment_id, saga_id, step: debit, reason, ts` | сбой на шаге Debit → компенсация |
| `SagaFailed` | `payments.events` | Orchestrator | `saga_id, payment_id, failed_step, reason, ts` | любой сбой саги |
| `SagaSucceeded` | `payments.events` | Orchestrator | `saga_id, payment_id, ts` | сага завершена до Success |

> **Почему имена `Funds*` / `*Posted`** (а не `ReservePosted`/`DebitPosted`):
> - `FundsReserved` / `FundsDebited` — события в `ledger.events` — «факт движения денег на ledger».
> - `CancelReservationPosted` / `RefundPosted` — факт записи компенсации в ledger (аналогичная форма).
> - `*Failed` / `Saga*Succeeded` — события оркестратора по жизни саги.
>
> Это устраняет рассинхрон plan §4 `2.2` ↔ tz §3.4 ↔ фазы 02/03/05, который был на входе в Этап 1.

---

## 4. Технологический стек и инфраструктура

### 4.1. Карта портов (v2 decisions #12)

> **Правило:** одна строка — один порт. Любое отклонение в доках/коде — баг. `EXPOSE` в Dockerfile и `ports:` в compose обязаны совпадать с таблицей.

| Компонент | Порт (host→container) | Назначение |
| --- | --- | --- |
| **gateway** | `8000:8080` | BFF, SSE `/api/v1/stream`, маршрутизация |
| **intake** | `8001:8080` | `POST /v1/payments`, идемпотентность |
| **orchestrator** | `8002:8080` | saga state machine, `GET /v1/sagas/{id}` |
| **ledger** | `8003:8080` | write-side, `POST /v1/accounts/{id}/…` |
| **profile** | `8004:8080` | `GET /v1/users`, consumer `ledger.events` |
| **notification** | `8005:8080` | ring-буфер последних платежей, SSE |
| **mock-acquirer** | `8006:8080` | load-генератор (`/acquirer/burst`) |
| **mock-fraud** | **`8007:8080`** | `/fraud/check` + `/fraud/admin/fail` |
| **frontend** | **`5173:80`** | Vite dev / nginx prod; **НЕ 3000** (3000 занят Grafana) |
| **postgres** | `5432:5432` | 4 базы |
| **redis** | `6379:6379` | идемпотентность |
| **kafka (external)** | `29092:29092` | EXTERNAL listener для хоста |
| **kafka-connect** | `8083:8083` | REST API Debezium |
| **prometheus** | `9090:9090` | metrics |
| **alertmanager** | `9093:9093` | алерты |
| **tempo** | `3200:3200`, `4317:4317` | traces (HTTP + OTel gRPC) |
| **loki** | `3100:3100` | логи |
| **grafana** | `3000:3000` | дашборды |

> **Почему frontend = 5173:** Vite default 5173, Grafana default 3000 — оба нужны одновременно. Vite proxy на dev: `{"/v1": "http://localhost:8000"}` (gateway). В prod — nginx gateway проксирует, CORS не нужен.
>
> **Почему fraud = 8007:** согласовано в фазы 03/06/07/08; ссылка `localhost:5005` из приложения B отменена (v2 #12).

| Составляющая | Стек |
| --- | --- |
| **Backend** | Python 3.11+, FastAPI, SQLAlchemy 2.0 (async), Pydantic v2 |
| **Message Broker** | **Apache Kafka (KRaft mode)**: официальный образ `apache/kafka:3.7.0` + 4 основных топака + 3 DLQ |
| **Database** | PostgreSQL 15+ (`wal_level=logical`), 4 базы |
| **Cache / Idempotency** | **Redis 7** (`SET NX EX`, key-TTL 24h) |
| **CDC** | Debezium PostgreSQL Connector + Kafka Connect |
| **Circuit Breaker** | `aiobreaker` (async, нативный `async with`; обёртка в `shared/circuit.py`) |
| **State Machine** | `python-statemachine` (зафиксирован в v2, ADR-006) |

### Observability (Must Have)

- **Metrics** — Prometheus (`prometheus-fastapi-instrumentator`).
- **Dashboards** — Grafana (latency, RPS, ошибки, CB-статус).
- **Tracing** — OpenTelemetry → Tempo (Gateway → Ledger сквозной).
- **Logging** — Loki (структурированный JSON).
- **Alerting** — Alertmanager: `p99_latency > 500ms` за 3 мин ИЛИ `cb_open > 0` для Fraud CB.

### Frontend

- React (или Vue.js).
- `EventSource` для SSE (`/api/v1/stream`).
- UI: таблица 20 юзеров (balance, currency), лента latest payments, badge статуса CB.

### SLO / нагрузочные цели (введено в v2)

| Метрика | Цель |
| --- | --- |
| Throughput | **≥ 50 платежей/сек** (Mock Acquirer как loader) |
| Latency | **P99 < 300 mс** (Gateway → 202 Accepted) |
| Delivery | **0 потерянных / 0 дублированных** событий (Exactly-Once через Outbox) |
| CB | При `mode=errors` — ≥ 90% фрод-проверок уходят в DLQ, но ≥ 10 TPS остаются валидными |

> Порог алерта 500ms — запас в ~1.5× от цели P99 (300ms).

### Infrastructure

Docker & Docker Compose: Kafka(KRaft) + Connect + Debezium + PostgreSQL + Redis + 8 сервисов + Prometheus + Grafana + Tempo + Loki + Alertmanager + Frontend.

---

## 5. Требования к тестовым данным (Seeding)

`init-db`/миграции наполняют:

- **20 пользователей** — Имя, Фамилия, Email, валюта (USD/EUR).
- **Балансы** — случайные реалистичные ($100 – $10 000).
- **Consistency** — Write- и Read-стороны синхронны на момент старта (seed пишется в `ledger` и `profile` атомарно через миграцию ledger + событие).
- Mock Acquirer — отдельный контейнер, не в БД.

---

## 6. Ожидаемые артефакты (Deliverables)

1. **Исходный код** — monorepo, структура `services/<name>/{api,domain,application,infrastructure}`.
2. **Документация:**
   - C4 Model (Context + Container).
   - **Sequence Diagram** платежа (Saga + Outbox + CB + DLQ).
   - **Event Contracts spec** (§3.11) в `docs/events.md`.
   - **ADR** по каждому решению v2 (§Изменения).
3. **Docker Compose** — `docker compose up` поднимает полный стек (~16 контейнеров).
4. **Тесты:**
   - Unit — Агрегаты, State Machine (покрытие 100% переходов).
   - Integration — Testcontainers: Outbox → Debezium → Kafka → consumer; Saga с fallback и компенсацией.
   - Load — Mock Acquirer @ 50 TPS, замер P99 в Grafana.
5. **Grafana dashboard** (JSON) — latency, RPS, CB status, DLQ depth.
6. **Runbook** — как переключить Fraud API в `errors`/`timeout` и убедиться, что CB открыт + платёж в DLQ.

---

## 7. Критерии оценки (Acceptance Criteria)

| Критерий | Что проверяем | Как |
| --- | --- | --- |
| **Согласованность** | Сбой Ledger в момент записи → Kafka-событие не теряется, не дублируется | Kill Ledger-контейнер, проверить `ledger.events` |
| **Отказоустойчивость** | `POST /fraud/admin/fail {mode: errors}` → CB открыт, фрод-проверки в DLQ, валидные ≥10 TPS | Admin-эндпоинт + Grafana |
| **Наблюдаемость** | Один и тот же `trace_id` от Gateway до Ledger | Tempo, поиск по `payment_id` |
| **Качество кода** | SOLID, чистые границы; ни один сервис не читает чужую БД | Code review + grep на cross-schema JOIN |
| **UI/UX** | Обрыв SSE → авто-reconnect; баланс обновляется live | Kill Notification, наблюдать reconnect |
| **SLO** | ≥ 50 TPS, P99 < 300ms при нагрузочном тесте | Grafana + `docker compose up` + Mock Acquirer |

---

## Приложения

### A. Диаграммы (заполнить в `docs/`)

- C4-Context.png — внешние акторы (Browser, Mock Acquirer, Mock Fraud) ↔ Gateway.
- C4-Container.png — 8 сервисов + Kafka + PG + Redis + observability cluster.
- Sequence-payment-happy-path.png.
- Sequence-fraud-failure.png (CB open, DLQ, валидные продолжаются).

### B. Команды быстрого старта (заполнить в README при реализации)

```
docker compose up -d --build
curl -X POST http://localhost:8000/api/v1/payments -H 'Idempotency-Key: abc' -d '{...}'
curl -X POST http://localhost:8007/fraud/admin/fail -d '{"mode":"errors"}'
# Grafana: http://localhost:3000 → dashboard "payments-overview"
```
