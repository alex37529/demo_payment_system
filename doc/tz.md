# Техническое задание: Архитектура и реализация демо-системы обработки платежей (FinTech)

> **Уровень:** Architect / CTO / Tech Lead
>
> **Контекст:** Разработка эталонного демо-проекта (Proof of Concept) для демонстрации лучших практик построения отказоустойчивых, согласованных и наблюдаемых распределенных систем в FinTech-домене.

---

## 1. Архитектурное видение и обоснование (CTO Level)

Главная цель проекта — не просто «написать код», а продемонстрировать глубокое понимание границ контекстов (Bounded Contexts), гарантий доставки сообщений и жизненного цикла транзакций.

Мы отходим от монолитного подхода в пользу **Event-Driven микросервисной архитектуры** с элементами **DDD**.

### Почему такие решения?

| Решение | Обоснование |
| --- | --- |
| **Saga Orchestration** | Распределённые транзакции без блокировок (2PC не подходит для микросервисов). Оркестратор даёт единую точку контроля за State Machine платежа. |
| **Transactional Outbox через WAL** | Гарантирует *Exactly-Once* семантику на уровне БД и избегает проблемы двойной записи (Dual Write), не нагружая приложение синхронными вызовами Kafka. |
| **CQRS** | Разделение моделей записи (строгий Ledger с Агрегатами) и чтения (быстрые проекции для UI и SSE) позволяет масштабировать чтение независимо от записи. |

---

## 2. Топология системы (составные части)

Исходные компоненты были реорганизованы согласно принципам DDD и микросервисной декомпозиции.

| Сервис | Роль |
| --- | --- |
| **API Gateway & BFF** (Backend-For-Frontend) | Единая точка входа. Терминирует SSE для фронтенда. Обеспечивает идемпотентность (проверка `Idempotency-Key` в заголовках). Маршрутизирует запросы. |
| **Payment Intake Service** (приём платежей) | Принимает запросы, валидирует DTO, генерирует идемпотентные ключи, инициирует процесс. Не знает о бизнес-логике списания. |
| **Payment Orchestrator** (Saga Manager) | Управляет State Machine платежа. Координирует шаги: Проверка лимитов → Резерв → Фрод-мониторинг → Списание. |
| **Ledger & Account Service** (ядро / Write-Side) | Хранит Агрегаты (Счета, Платежи). Реализует CQRS (Write side). Использует Transactional Outbox. |
| **User & Profile Service** (Read-Side / CQRS) | Хранит профили 20 тестовых пользователей. Предоставляет read-модели для BFF и фронтенда. Подписывается на события из Ledger для актуализации балансов. |
| **Realtime Notification Service** (SSE Pusher) | Потребляет события из Kafka (или читает из Read-модели), формирует потоки SSE для дашборда. |
| **Mock External Acquirer** (генератор) | Имитирует внешние шлюзы/мерчантов. Генерирует валидные и невалидные (с ошибками, таймаутами) платежи для стресс-теста системы. |

---

## 3. Реализация паттернов (Best Practices)

Ниже описано, где и как каждый паттерн должен быть применён.

### 3.1. Gateway API (SSE, BFF, Idempotency)

- **BFF** — адаптирует данные для фронтенда.
- **Idempotency** — middleware в Gateway проверяет Redis/БД на наличие `Idempotency-Key`. Если ключ уже обработан — возвращает кэшированный ответ.
- **SSE** — эндпоинт `/api/v1/stream` держит коннекты и пушит обновления балансов и статусов платежей.

### 3.2. Transactional Outbox (WAL + Kafka Connect)

- **Реализация** — приложение пишет событие в таблицу `outbox_events` в той же транзакции, что и изменение баланса.
- **Инфраструктура** — Debezium читает WAL (Write-Ahead Log) PostgreSQL и публикует изменения в Kafka. Это гарантирует, что БД и Kafka всегда синхронизированы.

### 3.3. Saga Orchestration

- **Реализация** — оркестратор хранит состояние саги, отправляет команды (`CheckBalance`, `DebitFunds`) и слушает события (`BalanceChecked`, `FundsDebited`).
- **Компенсации** — при ошибке на шаге «Списание» (например, нет денег) оркестратор отправляет команду `CancelReservation` или `Refund`.

### 3.4. Service Pattern (Layered / Clean Architecture)

- **Реализация** — строгое разделение слоёв внутри каждого сервиса:

```
Router → Controller → Service (UseCases) → Repository → Domain/Models
```

- Бизнес-логика не должна зависеть от FastAPI или SQLAlchemy.

### 3.5. Repository Pattern

- **Реализация** — абстракция над доступом к данным. Интерфейс `IPaymentRepository` скрывает детали ORM (SQLAlchemy/SQLModel). Позволяет легко мокать БД для юнит-тестов.

### 3.6. Circuit Breaker

- **Обоснование** — в Сагу добавлен шаг внешней проверки на фрод (Mock Fraud API).
- **Реализация** — использование `pybreaker` или `tenacity` (с CB). Если Mock Fraud API падает, Circuit Breaker открывается и система идёт по «fallback»-сценарию (пропускаем проверку или кладём в DLQ), чтобы не обрушить Оркестратор.

### 3.7. Retry with Exponential Backoff & Jitter

- **Реализация** — настройка Kafka Consumer (например, в `aiokafka` или `confluent-kafka`) или HTTP-клиента для вызова внешних API.
- **Формула:**

```
delay = base * (2^attempt) + random_jitter
```

- Обязательно наличие **Dead Letter Queue (DLQ)** после исчерпания попыток.

### 3.8. CQRS (Command Query Responsibility Segregation)

- **Write Side** — Ledger Service. Строгие проверки инвариантов, транзакции, Агрегаты.
- **Read Side** — User & Profile Service. Денормализованные таблицы (или materialized views) для быстрого чтения балансов и списка пользователей.

### 3.9. Aggregate (DDD)

- **Реализация** — в Ledger Service сущности `Account` и `Payment` являются Агрегатами. У них есть корень (Aggregate Root). Изменение баланса возможно только через методы Агрегата, которые проверяют бизнес-инварианты (например, `balance >= amount`).

### 3.10. State Machine

- **Реализация** — жизненный цикл Payment:

```
Created → Validating → Processing → Success / Failed / RolledBack
```

- **Инструмент** — библиотека `transitions` или `python-statemachine`. Переходы триггерятся событиями из Kafka.

---

## 4. Технологический стек и инфраструктура

| Составляющая | Стек |
| --- | --- |
| **Backend** | Python 3.11+, FastAPI, SQLAlchemy 2.0 (async), Pydantic v2 |
| **Message Broker** | Apache Kafka (топики: `payments.commands`, `payments.events`, `ledger.events`) |
| **Database** | PostgreSQL 15+ (с включённым `wal_level=logical` для Debezium) |
| **CDC** | Debezium PostgreSQL Connector + Kafka Connect |

### Observability (Must Have)

- **Metrics** — Prometheus (экспорт из FastAPI через `prometheus-fastapi-instrumentator`).
- **Dashboards** — Grafana (дашборды latency, RPS, ошибок).
- **Tracing** — OpenTelemetry → Tempo (сквозной трейсинг от Gateway до Ledger).
- **Logging** — Loki (структурированные JSON-логи).
- **Alerting** — Alertmanager (алерт на рост latency > 500 mс или ошибки Circuit Breaker).

### Frontend

- React / Vue.js (на выбор).
- Обязательна поддержка `EventSource` для SSE.
- Простой UI с таблицей 20 пользователей и лентой платежей.

### Infrastructure

- Docker & Docker Compose (весь стек, включая Kafka, Zookeeper/KRaft, Debezium, Observability).

---

## 5. Требования к тестовым данным (Seeding)

При старте системы (через `init-db`-скрипт или миграции) необходимо заполнить БД:

- **20 реальных пользователей** — Имя, Фамилия, Email, Валюта счёта (USD/EUR).
- **Балансы** — случайные, но реалистичные значения (от $100 до $10 000).
- **Важно:** данные должны быть консистентны между Write- и Read-сторонами на момент старта.

---

## 6. Ожидаемые артефакты (Deliverables)

Кандидат/архитектор должен предоставить:

1. **Исходный код** — репозиторий с чёткой структурой (monorepo или polyrepo).
2. **Архитектурная документация** (ADR / README.md):
   - C4 Model (Context- и Container-диаграммы).
   - Sequence Diagram для процесса проведения платежа (с учётом Saga и Outbox).
   - Обоснование выбора паттернов (почему именно так).
3. **Docker Compose** — команда `docker-compose up` должна поднимать всю систему, включая Debezium, Kafka, БД, Observability и Frontend.
4. **Тесты:**
   - Unit-тесты для Агрегатов и State Machine.
   - Integration-тесты (с использованием Testcontainers) для проверки Outbox и Saga.
5. **Дашборд Grafana** — экспортированный JSON-дашборд, показывающий latency обработки платежа и статус Circuit Breaker.

---

## 7. Критерии оценки (Acceptance Criteria)

| Критерий | Описание |
| --- | --- |
| **Согласованность** | При сбое сервиса в момент записи в БД сообщение в Kafka не теряется и не дублируется (Outbox + Debezium). |
| **Отказоустойчивость** | При «падении» Mock Fraud API система не ложится (работает Circuit Breaker), часть платежей уходит в DLQ, но валидные продолжают обрабатываться. |
| **Наблюдаемость** | По трейсам в Tempo можно пройти путь платежа от Gateway до Ledger. В Grafana видны пики latency. |
| **Качество кода** | Отсутствие «God objects», соблюдение SOLID, чистые границы сервисов. |
| **UI/UX** | Фронтенд корректно переподключается при обрыве SSE, балансы обновляются в реальном времени без перезагрузки страницы. |
