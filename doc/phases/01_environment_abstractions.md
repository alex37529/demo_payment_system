# Этап 1. Инвайрмент + все абстракции (shared)

> **Ветка:** `feat/01-infra`
> **Результат:** `make up` поднимает полный Docker-стек (**18 контейнеров**: 9 data/obs infra + promtail + 8 сервисов); `from shared.domain import Aggregate, AggregateRoot, ValueObject, Entity` работает; 8 placeholder-сервисов отвечают `200` на `/healthz`. Карта портов: `tz_v2.md §4.1` (v2 #12). Event-каталог: `tz_v2.md §3.11.1` (v2 #10).
> **Чему учимся здесь:** как устроен весь инвайрмент «с низа вверх» — БД, Kafka, CDC, observability — и какие Python-абстракции (DDD, outbox, Kafka, circuit breaker, state machine, идемпотентность) мы дадим каждому сервису, чтобы на этапах 2–6 писать только бизнес-логику.

---

## 1. Что такое «инвайрмент» и зачем он нужен

**Инвайрмент** — это готовая среда, где все инфраструктурные компоненты уже работают: PostgreSQL, Redis, Kafka, CDC, мониторинг. Мы поднимаем его **первым**, ещё до написания любой бизнес-логики, по трём причинам:

1. **Обнаруживаем проблемы ранно.** Если Kafka/Debezium не стартуют — лучше узнать сейчас, а не на этапе 7, когда 8 сервисов уже от него зависят.
2. **Задаём контракт для всех.** shared-модуль (`shared/`) становится единой точкой: все 8 сервисов импортируют одни и те же классы Kafka, outbox, circuit breaker. Никаких 8 разных реализаций.
3. **Плейсхолдеры.** 8 сервисов пока отвечают только на `/healthz` — это позволяет проверить, что сети, health checks, observability, порты — всё работает end-to-end, до реального кода.

> **Смысл этапа:** на выходе у нас есть «пустой, но живой» скелет системы. Дальше этапы 2–6 просто наполняют каждый сервис логикой по очереди.

---

## 2. Цели этапа (что должно быть на конце)

| Цель | Как проверить |
| 18 контейнеров в `running` (4 data + 5 obs + promtail + 8 svc) | `docker compose ps` |
| 4 БД в PostgreSQL | `psql -l` |
| 4 Kafka-топака + 3 DLQ | `docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list` |
| Redis отвечает | `redis-cli ping` → `PONG` |
| Debezium подключён | `GET /connectors` (Kafka Connect) |
| Prometheus + Grafana + Tempo + Loki up | HTTP на порты |
| 8 сервисов `/healthz` → 200 | `make smoke` |
| `shared` импортируется и тестируется | `make test` |

---

## 3. Структура репозитория после этапа

```
fintech_demo/
├── docker-compose.yml          # весь стек
├── pyproject.toml              # workspace: shared + services/*
├── Makefile                    # up/down/logs/seed/test/load/acceptance
├── .gitignore
├── README.md                   # stub (заполнится в Этапе 7)
├── doc/                        # ТЗ + план + этапы (уже есть)
├── init/
│   └── 01_databases.sql        # создать 4 базы + wal_level
├── kafka/
│   ├── connect/
│   │   └── debezium-ledger.json
│   └── topics.sh               # создать 6 топиков + 3 DLQ
├── observability/
│   ├── prometheus/prometheus.yml
│   ├── grafana/provisioning/…
│   ├── loki/loki.conf
│   ├── tempo/tempo.yml
│   └── alertmanager/alertmanager.yml
├── shared/                     # ★ ЯДРО этапа: абстракции
│   ├── __init__.py
│   ├── py.typed
│   ├── domain/
│   │   ├── __init__.py
│   │   ├── base.py             # Aggregate, Entity, ValueObject
│   │   └── events.py           # каталог DomainEvents
│   ├── db/
│   │   ├── __init__.py
│   │   ├── base.py             # AsyncSession, Base
│   │   └── repository.py       # IAccount/IPayment/IOutbox interfaces
│   ├── outbox.py               # OutboxEvent + OutboxMixin
│   ├── kafka.py                # Producer/Consumer + backoff + DLQ
│   ├── circuit.py              # CircuitBreakerHttpClient (aiobreaker, async with)
│   ├── state_machine.py        # PaymentStateMachine
│   ├── observability.py        # OTel + prom + loki-json
│   └── idempotency.py          # IdempotencyMiddleware
├── services/
│   ├── _template/              # шаблон FastAPI-сервиса (только код, без pyproject — workspace в корне)
│   │   ├── Dockerfile
│   │   └── main.py             # app factory + /healthz
│   ├── gateway/                # BFF + SSE-терминал
│   ├── intake/
│   ├── orchestrator/
│   ├── ledger/
│   ├── profile/
│   ├── notification/
│   ├── mock-acquirer/
│   └── mock-fraud/
├── scripts/
│   └── smoke_env.sh            # curl /healthz×8 + topics + pg
└── tests/
    └── shared/                 # smoke-тесты shared
```

---

## 4. Инфраструктура (docker-compose) — разбирать по блокам

### 4.1. PostgreSQL × 4 базы

**Зачем 4 базы:** границы контекстов = границы БД (§2.2 TЗ v2). Ни один сервис не читает чужую базу — только через события.

```
postgres → база ledger       (write-side: accounts, payments, outbox_events)
       → база orchestrator  (saga_state, saga_steps)
       → база intake        (idempotency_keys, intake_audit)
       → база profile       (read-side: users, balances)
```

```yaml
postgres:
  image: postgres:16
  environment:
    POSTGRES_PASSWORD: fintech
    # wAL обязательно для Debezium (CDC)
  command: >
    postgres -c wal_level=logical
             -c max_replication_slots=10
             -c max_wal_size=2GB
  volumes:
    - pgdata:/var/lib/postgresql/data
    - ./init/01_databases.sql:/docker-entrypoint-initdb.d/01.sql:ro
  healthcheck:
    test: ["CMD-SHELL", "pg_isready -U postgres"]
    interval: 5s
    timeout: 5s
  # 4 базы создаём в 01_databases.sql:
  #   CREATE DATABASE ledger; ... и т.д.
  #   CREATE ROLE fintech LOGIN PASSWORD 'fintech';
  ports: ["5432:5432"]
```

> **Подводный камень №1:** `wal_level=logical` **обязателен**. Без него Debezium PostgreSQL-коннектор не сможет читать изменения. Проверка: `SHOW wal_level;` → `logical`.
>
> **Почему не 4 отдельных postgres?** Один postgres = 4 схемы/базы = 1 контейнер, проще. 4 отдельных контейнера — только если нужно изолировать I/O, в PoC не нужно.

### 4.2. Redis 7 — идемпотентность

**Зачем:** `GET/SETNX` по `Idempotency-Key` быстрее, чем запись в БД (§1.1 TЗ v2, решение #1).

```yaml
redis:
  image: redis:7-alpine
  command: redis-server --appendonly yes
  healthcheck:
    test: ["CMD", "redis-cli", "ping"]
  ports: ["6379:6379"]
```

> **Подводный камень №2:** TTL на ключи идемпотентности = **24 часа**. Ключ дольше — Redis разрастётся; короче — клиент может повторить запрос и получить «already processed» раньше, чем он реально обработан. 24h — разумный баланс. Дубликат ключа **также** пишется в БД `intake.idempotency_keys` как персистентная копия (на случай сбоя Redis).

### 4.3. Kafka **KRaft** single-node (официальный Apache)

**Зачем KRaft, а не ZooKeeper:** современный стандарт, меньше контейнеров (§2 decisions TЗ v2, ADR-002).

**Почему `apache/kafka:3.7.0` и не `bitnami/kafka`:** Bitnami ~авг.2025 ограничил бесплатные образы в Docker Hub (нужен bitnami-registry) — не тянется на чистом `docker pull`. Официальный образ `apache/kafka` — тот же KRaft, без license-drift. Это зафиксировано в `implementation_plan.md §0` и ADR-002.

```yaml
kafka:
  image: apache/kafka:3.7.0
  container_name: kafka
  environment:
    # KRaft single-node: controller + broker в одном процессе
    CLUSTER_ID: "fintech-demo-cluster-0001"
    KAFKA_NODE_ID: "0"
    KAFKA_PROCESS_ROLES: "controller,broker"
    KAFKA_CONTROLLER_QUORUM_VOTERS: "0@kafka:9093"
    KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
    KAFKA_LISTENERS: "PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:29092"
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT"
    KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka:9092,EXTERNAL://localhost:29092"
    KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1"
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1"
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1"
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    # RAM экономия (single-node PoC)
    KAFKA_HEAP_OPTS: "-Xmx512m -Xms256m"
    KAFKA_LOG_HEAP_OPTS: "-Xmx256m"
    KAFKA_NUM_PARTITIONS: "3"
    KAFKA_LOG_RETENTION_BYTES: "1073741824"
    KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS: "300000"
  healthcheck:
    test: ["CMD-SHELL", "kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1"]
    interval: 10s
    timeout: 10s
    retries: 20
    start_period: 30s
  ports:
    - "29092:29092"   # EXTERNAL для хоста (host-side smoke-скрипт, testcontainers)
  volumes:
    - kafka-data:/var/lib/kafka/data
```

> **Подводный камень №3:** KRaft требует **отдельного controller-порту** (9093) и `KAFKA_PROCESS_ROLES=controller,broker`. Забыть controller → broker не стартует и молчит в логах.
>
> **Подводный камень №4 (RAM):** JVM-heavy — дефолтный heap ~1GB+. Ставим `KAFKA_HEAP_OPTS=-Xmx512m -Xms256m`; иначе на машине с 8GB OOM.
>
> **Подводный камень №4a (images):** `bitnami/kafka:3.7` — не тянется из Docker Hub (Bitnami ~2025 перешёл на bitnami-registry). Используем официальный `apache/kafka:3.7.0`.
>
> **Подводный камень №4b (CLUSTER_ID):** нужен **стабильный** CLUSTER_ID — при смене он «сломает» consumer offsets. Для PoC — строка. Генерация: `kafka-storage.sh random-uuid`.

**Топика** (создаём `kafka/topics.sh`, запускается после up):
```
payments.commands     # Intake → Orchestrator (producer)
payments.events       # Orchestrator → (Profile, Notification)
ledger.commands       # Orchestrator → Ledger (producer)
ledger.events         # Ledger(Debezium) → (Profile, Notification)
payments.commands.dlq # DLQ×1   (Intake publish-fail → сюда)
ledger.commands.dlq   # DLQ×2   (Orchestrator publish-fail → сюда; fraud-fail → сюда)
payments.events.dlq   # DLQ×3   (Orchestrator event publish-fail → сюда)
```

> **Почему ровно 3 DLQ, а не 4.** DLQ нужен **на producer** (кто публикует — тот и «бросает в DLQ при отказе»). Producers: Intake (`payments.commands`), Orchestrator (`payments.events`, `ledger.commands`). Consumer'ы событий (`ledger.events` у Profile/Notification) в PoC «не падают» (кольцо/БД в том же контейнер-группе). Если позже появится «упадает consumer» — добавим 4-й DLQ по событию. Зафиксировано в `tz_v2 §3.11` (v2 #11).

### 4.4. Kafka Connect + Debezium (CDC)

**Zачем:** Outbox-паттерн. Приложение пишет событие в таблицу `outbox_events` **в той же транзакции**, что и изменение баланса. Debezium читает WAL Postgres и публикует изменения в Kafka. БД и Kafka атомарно согласованы (§3.2 TЗ v2). Это **убивает Dual Write problem**: нельзя «записать в БД, но не в Kafka» и наоборот.

```yaml
kafka-connect:
  image: confluentinc/cp-kafka-connect:7.6.0
  depends_on: { kafka: { condition: service_healthy } }
  environment:
    BOOTSTRAP_SERVERS: kafka:9092
    CONNECT_CONTACT_INFO: kafka-connect:8083
    GROUP_ID: 1
    CONNECT_PLUGIN_PATH: /usr/share/java
    # JSON-конверторы (без Debezium Avro → нет Schema Registry)
    CONNECT_KEY_CONVERTER: org.apache.kafka.connect.json.JsonConverter
    CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE: "false"
    CONNECT_VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
    CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE: "false"
  ports: ["8083:8083"]   # Rest API Connect
  # SMT RegexRouter — задаётся НЕ в compose, а в JSON-конфиге коннектора ниже
```

> **Важно по конвертерам:** старое значение `CONNECT_VALUE_CONVERTER: io.debezium.transforms.EnumMapper` в v1-черновике было **ошибкой**: `EnumMapper` — это SMT (Single Message Transform), а не value-converter. Converter отвечает за сериализацию value, SMT — за трансформацию сообщения. Правильно: converter = `JsonConverter`, SMT = `RegexRouter` (в конфиге коннектора).

Debezium подключается REST-вызовом `POST http://localhost:8083/connectors` (см. `make up`), конфиг `kafka/connect/debezium-ledger.json`:
```json
{
  "name": "ledger-outbox",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "fintech",
    "database.password": "fintech",
    "database.dbname": "ledger",
    "database.schema": "public",
    "publication.name": "outbox_publication",
    "plugin.name": "pgoutput",
    "table.include.list": "public.outbox_events",
    "topic.prefix": "outbox",
    "schema.name": "ledger",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "transforms": "Route",
    "transforms.Route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.Route.regex": "^outbox\\.ledger\\.public\\.outbox_events$",
    "transforms.Route.replacement": "ledger.events"
  }
}
```

> **Почему так:** `topic.prefix=outbox` + `database=ledger` + `schema=name=ledger` + `table=outbox_events` → «сырой» топик Debezium = `outbox.ledger.public.outbox_events`. SMT `RegexRouter` (`transforms`) переименует его в целевой `ledger.events` (контракт TЗ v2 §3.11). SMT ≠ value-converter: конвертер сериализует value в JSON, SMT трансформирует само сообщение (regex-match → replacement).

> **Подводный камень №5:** Debezium-коннектор **подключается POST-вызовом** в Kafka Connect REST API, а не через volumes. Значит, `make up` должен делать: `up → wait health → POST connector → wait running`. Иначе коннектор «висит» и события не идут.
>
> **Подводный камень №6 (SMT):** `topic.prefix` образует «сырой» топик (напр. `outbox.ledger.public.outbox_events`), а **SMT `RegexRouter`** переименует его в целевой. Finaльный топик в TЗ v2 = **`ledger.events`**. SMT ≠ converter: конвертер сериализует value, SMT трансформирует сообщение (regex → замена).

### 4.5. Observability stack (5 контейнеров)

| Контейнер | Порт | Что |
| --- | --- | --- |
| **prometheus** | 9090 | сбор метрик; scraping `*/metrics` от 8 сервисов |
| **tempo** | 3200 | трассировки (OTel gRPC) |
| **loki** | 3100 | логи (от сервисов через promtail/sidecar) |
| **grafana** | 3000 | дашборды (E2E latency, RPS, CB, DLQ) |
| **alertmanager** | 9093 | алерты (p99>500ms, CB open) |

```yaml
prometheus:
  image: prom/prometheus:latest
  volumes: [ ./observability/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro ]
  command: ["--config.file=/etc/prometheus/prometheus.yml"]
  ports: ["9090:9090"]
# scrape_configs: все services/* на :8080/metrics + kafka + postgres + redis
```

> **Подводный камень №7:** Grafana — **last to start** (нужен Prometheus + Loki + Tempo). Ставим `depends_on` с `condition: service_started`, а не `healthy` (Grafana self-heals, если datasource временно недоступен).
>
> **Подводный камень №8 (локи):** Сервисы пишут JSON-логи в stdout. Loki собирает через **Grafana Alloy / promtail** (additional контейнер) ИЛИ через `docker logs` pipeline. В PoC проще: **promtail** sidecar на каждый сервис — но это +8 контейнеров. Компромисс: promtail **один** на host, читает `/var/lib/docker/containers/*/*.log` через `docker inspect`. Это +1 контейнер, не +8.

### 4.6. Health checks и порядок старта

Правильный порядок:
```
postgres + redis (base)
   → kafka (KRaft)
      → kafka-connect + debezium connector
         → prometheus + tempo + loki + grafana + alertmanager
            → services/* (8 шт) — каждый только если база healthy
```
В compose — `depends_on.condition=service_healthy` (postgres, redis, kafka) + `healthcheck` на каждого сервиса.

> **Подводный камень №9:** `service_healthy` ждёт **прохождения healthcheck**. На postgres это `pg_isready`, на kafka — `kafka-topics --list`, на каждом сервисе — `curl /healthz`. Если healthcheck не прописан — `depends_on` ждёт по 30s и падает. **Красный флаг — сервисы стартуют, но «видят» только часть стека.**

### 4.7. Placeholder-сервисы (шаблон + 8 копий)

Каждый сервис на этом этапе — минимальный FastAPI:

```python
# services/<name>/main.py
from fastapi import FastAPI
from shared.observability import setup       # §5.8: setup(app, service, otel_endpoint)

def create_app() -> FastAPI:
    app = FastAPI(title="<name>")
    setup(app, service="<name>")   # OTel, prometheus /metrics, /healthz, JSON-logs
    return app

app = create_app()
```

> **`/healthz`** добавляется внутри `shared.observability.setup()` (см. §5.8) — дублировать его в каждом сервисе не нужно.

Dockerfile (единый, shared для всех 8 сервисов — workspace, один `pyproject.toml` в корне):
```dockerfile
FROM python:3.11-slim
# curl + tini нужны: curl — HEALTHCHECK (compose) и smoke-скрипт §5.10; tini — PID1/signal
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl tini \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN pip install --no-cache-dir -e '.[all-services]'   # workspace: shared + services/*
COPY shared ./shared
COPY services ./services
ENV PYTHONUNBUFFERED=1
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=5s --start-period=10s \
  CMD curl -fsS http://localhost:8080/healthz || exit 1
ENTRYPOINT ["tini","--"]
CMD ["uvicorn","services.<name>.main:app","--host=0.0.0.0","--port=8080"]
```

Счёт контейнеров (exit `18/18`):

| Группа | Число | Состав |
| --- | --- | --- |
| Data | 4 | postgres, redis, kafka, kafka-connect |
| Observability | 5 | prometheus, tempo, loki, grafana, alertmanager |
| Log-shipper | 1 | promtail |
| Сервисы | 8 | gateway, intake, orchestrator, ledger, profile, notification, mock-acquirer, mock-fraud |
| **Итого** | **18** | |

> **Подводный камень №10:** placeholder **обязан** импортировать `shared.observability.setup(app, service=...)`. Если один сервис без него — он не попадёт в Prometheus и не будет виден в Grafana. Это проверится smoke-скриптом.
>
> **Подводный камень №10a (curl):** образ `python:3.11-slim` **не содержит** `curl` (только busybox). Без `apt-get install curl` `HEALTHCHECK` в compose и `scripts/smoke_env.sh` (§5.10) не работают. Установка в Dockerfile обязательна.
>
> **Подводный камень №10b (workspace):** в корне **один** `pyproject.toml` (uv/hatch workspace: `shared` + `services/*` + extras `[all-services]`). Пер-сервисные `pyproject.toml` **внутри** `services/<name>/` — не создаём (противоречит §0 плана). `uv.lock` обязателен для детерминированного build.

---

## 5. Shared — ядро этапа (по файлам)

> Каждый блок ниже объясняет: (a) цель, (b) ключевой код, (c) подводные камни.

### 5.1. DDD-базы: `shared/domain/base.py`

**Зачем:** Aggregate/Entity/ValueObject — стандарт DDD. Агрегат — граница транзакции и инвариантов: «изменение только через методы агрегата».

```python
# shared/domain/base.py
from __future__ import annotations
import uuid
from dataclasses import dataclass, field
from typing import Generic, TypeVar

@dataclass
class ValueObject:
    """Immutable. __eq__ по значению."""
    def __hash__(self):
        return hash(tuple(getattr(self, f) for f in self.__dataclass_fields__))
    def __eq__(self, other):
        return type(self) is type(other) and self.__dict__ == other.__dict__

class Entity:
    id: uuid.UUID
    def __init__(self, id: uuid.UUID | None = None):
        self.id = id or uuid.uuid4()
    def __eq__(self, other):
        return isinstance(other, Entity) and self.id == other.id

class AggregateRoot(Entity):
    _events: list = field(default_factory=list, init=False)
    def add_event(self, e): self._events.append(e)
    def pull_events(self) -> list:
        out, self._events = self._events, []
        return out
```

> **Подводный камень №11:** `ValueObject.__eq__` по значению — **обязателен** для `Money`, `Currency` и т.п. Если оставить `__eq__` по ссылке — `Money(100,USD) != Money(100,USD)` и весь код ломается на сравнениях.
>
> **Подводный камень №11a (alias):** exit-criterion (plan §2, таблица этапа 1) требует `from shared.domain import Aggregate`. В коде мы назвали класс `AggregateRoot` (стандарт DDD). Чтобы оба имени работали, в `shared/domain/__init__.py` объявляем:
> ```python
> from .base import AggregateRoot, Entity, ValueObject
> Aggregate = AggregateRoot          # backward-compat alias, зафиксировано в v2
> __all__ = ["Aggregate", "AggregateRoot", "Entity", "ValueObject"]
> ```
> Запрещено создавать второй класс `Aggregate` — это alias, а не дубль.

### 5.2. Domain Event Catalog: `shared/domain/events.py`

**Зачем:** единый каталог событий (§3.11 / §3.11.1 TЗ v2). Каждый сервис импортирует отсюда — нет «каждый описывает свой payload».

> **Канон (v2 #10):** имена ниже — единственно разрешённые. Источник: `tz_v2.md §3.11.1`. Имена `ReservePosted` / `DebitPosted` (v1-черновик) **отменены** и заменены на `FundsReserved` / `FundsDebited`.

```python
# shared/domain/events.py
from dataclasses import dataclass, field
from datetime import datetime, timezone
import uuid

@dataclass(frozen=True)
class DomainEvent:
    event_id: uuid.UUID = field(default_factory=uuid.uuid4)
    ts: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    occurred_by: str = ""          # сервис, создавший

    def to_json(self) -> dict:
        return {k: (v.isoformat() if hasattr(v,"isoformat") else v)
                for k, v in self.__dict__.items()}

# === payments (produced: Intake, Orchestrator) ===
@dataclass(frozen=True)
class PaymentCreated(DomainEvent):
    payment_id: str; amount: dict; account_id: str; idem_key: str = ""

@dataclass(frozen=True)
class LimitsChecked(DomainEvent):
    payment_id: str; saga_id: str

@dataclass(frozen=True)
class FraudVerdict(DomainEvent):
    payment_id: str; saga_id: str
    decision: str                  # "approve" | "decline" | "route_to_dlq"
    score: float = 0.0

@dataclass(frozen=True)
class DebitFailed(DomainEvent):
    payment_id: str; saga_id: str; step: str; reason: str = ""

# === ledger (produced: Ledger — через outbox → Debezium → ledger.events) ===
@dataclass(frozen=True)
class FundsReserved(DomainEvent):
    """Delta-only: резерв, НЕ финальный баланс (см. phase 05)."""
    account_id: str; payment_id: str; saga_id: str; amount: dict

@dataclass(frozen=True)
class FundsDebited(DomainEvent):
    """Final: средства списаны, new_balance = актуальный."""
    account_id: str; payment_id: str; saga_id: str; amount: dict; new_balance: dict

@dataclass(frozen=True)
class CancelReservationPosted(DomainEvent):
    account_id: str; payment_id: str; saga_id: str; amount: dict; reason: str = ""

@dataclass(frozen=True)
class RefundPosted(DomainEvent):
    account_id: str; payment_id: str; saga_id: str; amount: dict; reason: str = ""

# === saga (produced: Orchestrator) ===
@dataclass(frozen=True)
class SagaFailed(DomainEvent):
    saga_id: str; payment_id: str; failed_step: str; reason: str = ""

@dataclass(frozen=True)
class SagaSucceeded(DomainEvent):
    saga_id: str; payment_id: str

__all__ = [
    "DomainEvent",
    "PaymentCreated", "LimitsChecked", "FraudVerdict", "DebitFailed",
    "FundsReserved", "FundsDebited", "CancelReservationPosted", "RefundPosted",
    "SagaFailed", "SagaSucceeded",
]
```

> **Почему 10, а не 4 события** (как в v1-черновике §5.2): v1 не содержал `LimitsChecked`, `FraudVerdict`, `DebitFailed`, `RefundPosted`, `SagaSucceeded` — это ломало фазы 03 (consumer `ledger.events` реагирует на `FundsReserved`/`FundsDebited`, оркестратор публикует `FraudVerdict`/`DebitFailed`) и 05 (Consumer `ledger.events` различает «delta» vs «final» по классу). Полный список канонизирован в `tz_v2 §3.11.1`.

> **Подводный камень №12:** События **frozen=True** (immutable). Мутабельные события — классическая причина «событие пришло в Kafka, а в памяти у отправителя его поменяли». Frozen + `pull_events()` в AggregateRoot — стандарт.

### 5.3. async DB base + repository interfaces: `shared/db/`

**Зачем:** async SQLAlchemy 2.0 — единый слой. Интерфейсы позволяют мокать БД в юнит-тестах (§3.5 TЗ v2).

```python
# shared/db/base.py
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

class Base(DeclarativeBase): pass

def make_engine(dsn: str):
    return create_async_engine(dsn, echo=False, pool_size=10, max_overflow=20)

def make_session(engine):
    factory = async_sessionmaker(engine, expire_on_commit=False)
    return factory()
```

```python
# shared/db/repository.py
from abc import ABC, abstractmethod
from typing import Any, Optional

class IAccountRepository(ABC):
    @abstractmethod async def get(self, account_id: str) -> Any: ...
    @abstractmethod async def save(self, account: Any) -> None: ...
    @abstractmethod async def debit(self, account_id: str, amount: int, currency: str) -> Any: ...
    @abstractmethod async def reserve(self, account_id: str, amount: int, currency: str) -> Any: ...
    @abstractmethod async def cancel_reservation(self, account_id: str, ref: str) -> None: ...
    @abstractmethod async def refund(self, account_id: str, ref: str, amount: int) -> None: ...

class IPaymentRepository(ABC):
    @abstractmethod async def get(self, payment_id: str) -> Any: ...
    @abstractmethod async def save(self, payment: Any) -> None: ...
    @abstractmethod async def set_state(self, payment_id: str, state: str) -> None: ...

class IOutboxRepository(ABC):
    @abstractmethod async def publish(self, event: Any) -> None:
        """Пишет event в outbox в текущей TX. Debezium подхватит."""
```

> **Подводный камень №13:** `expire_on_commit=False` **обязателен** с async. Без него — `DetachedInstanceError` на любом атрибуте объекта после commit.

### 5.4. Transactional Outbox: `shared/outbox.py`

**Зачем:** это **ключевой паттерн PoC** (§3.2 TЗ v2). Изменение данных и запись события — в **одной транзакции**. Debezium доставляет событие в Kafka. Атомарность: нельзя «записал баланс, но событие утеряно».

```python
# shared/outbox.py
import json, uuid
from datetime import datetime
from sqlalchemy import Column, String, JSON, DateTime, BigInteger
from shared.db.base import Base

class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    id        = Column(BigInteger, primary_key=True, autoincrement=True)
    event_id  = Column(String(36), unique=True, index=True)
    topic     = Column(String(128), index=True)     # ledger.events / payments.events
    payload   = Column(JSON)
    created_at= Column(DateTime(timezone=True), default=datetime.utcnow, index=True)

class OutboxMixin:
    """Миксин для use-cases. Пишет событие в TX."""
    def _publish_outbox(self, session, topic: str, event) -> None:
        row = OutboxEvent(
            event_id=str(event.event_id),
            topic=topic,
            payload=event.to_json(),
        )
        session.add(row)   # commit произойдёт outside — в use-case
```

```python
# Пример use-case (Этап 2), показывает, как используется mixin
async def debit_use_case(repo: IAccountRepository, session, account_id, amount, currency):
    account = await repo.get(account_id)
    account.debit(Money(amount, currency))        # инвариант внутри агрегата
    await session.flush()
    self._publish_outbox(session, "ledger.events", FundsDebited(
        account_id=account_id, payment_id=payment_id, amount=amount.to_dict(),
        new_balance=account.balance.to_dict()))
    await session.commit()   # ★ одна TX: изменение баланса + outbox строка
    # Debezium увидит строку в outbox_events → Kafka
```

> **Подводный камень №14:** **Нельзя «записать событие» после commit** (иначе событие потеряно при крахе между commit'ом и записью). Порядок внутри TX: `flush() → add outbox → commit()`. Если commit падает — и изменение, и outbox откатываются (атомарность).
>
> **Подводный камень №15 (Debezium + outbox):** Debezium публикует **insert** в `outbox_events`. Это означает: **событие в Kafka = insert row**. Уникальный `event_id` позволяет consumer'ам идемпотентно игнорировать дубликаты (если Debezium повторит).

### 5.5. Kafka клиент: `shared/kafka.py`

**Зачем:** единый Producer/Consumer с retry + exponential backoff + jitter + DLQ (§3.8 TЗ v2).

```python
# shared/kafka.py
import asyncio, random, time, json
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

class KafkaProducer:
    def __init__(self, brokers: str, dlq_prefix: str = ""):
        self._p = AIOKafkaProducer(bootstrap_servers=brokers)
        self.dlq_prefix = dlq_prefix

    async def start(self): await self._p.start()
    async def stop(self):  await self._p.stop()

    async def publish(self, topic: str, key: str, value: dict, *,
                     max_retries: int = 5, base_delay: float = 0.2) -> None:
        last_err = None
        for attempt in range(max_retries):
            try:
                await self._p.send_and_wait(topic, key=key.encode(),
                                            value=json.dumps(value).encode())
                return
            except Exception as e:
                last_err = e
                # Exponential backoff + jitter (§3.8 TЗ v2)
                delay = base_delay * (2 ** attempt)
                await asyncio.sleep(delay + random.uniform(0, delay * 0.5))
        # DLQ
        dlq = f"{self.dlq_prefix or topic}.dlq"
        await self._send_nowait(dlq, key, {"original_topic": topic, "value": value,
                                           "error": str(last_err), "attempts": max_retries})
        raise RuntimeError(f"publish failed after {max_retries} retries → {dlq}") from last_err

    async def _send_nowait(self, topic, key, value):
        await self._p.send_and_wait(topic, key=key.encode(),
                                    value=json.dumps(value).encode())
```

```python
class KafkaConsumer:
    def __init__(self, brokers: str, group: str, topics: list[str]):
        self._c = AIOKafkaConsumer(*topics, bootstrap_servers=brokers,
                                   group_id=group,
                                   enable_auto_commit=True,
                                   auto_offset_reset="earliest")

    async def start(self): await self._c.start()
    async def stop(self):  await self._c.stop()

    async def stream(self):
        async for msg in self._c:
            yield msg.key, json.loads(msg.value)
```

> **Подводный камень №16:** AIokafka (Python) **не** имеет встроенным DLQ «из коробки». Мы пишем свой (`_send_nowait` в `.dlq`). Это норм — DLQ = «ещё один топик».
>
> **Подводный камень №17:** `auto_offset_reset="earliest"` — в PoC это удобно (видим старое). В проде — `latest` + commit per msg.
>
> **Подводный камень №18:** `group_id` — **разные группы** на каждый consumer (ledger-события читают Profile И Notification, обе получают → разные group_id).

### 5.6. Circuit Breaker: `shared/circuit.py`

**Зачем:** External Fraud API может падать. CB «открывает» Circuit и идёт в fallback, чтобы не обрушить Оркестратор (§3.7 TЗ v2).

**Библиотека: `aiobreaker`** (зафиксировано в v2 #12 / ADR-004). `pybreaker` — **sync-only**: у него нет `__aenter__`/`__aexit__`, т.е. `async with self.cb:` в старом сниппете не сработает (AttributeError). Для async-миров нужно либо обёртка через `loop.run_in_executor` (блокирует event loop), либо нативный async-CB. `aiobreaker` — нативный (API совместим с pybreaker, + `async with`).

```python
# shared/circuit.py
import httpx, logging
from aiobreaker import CircuitBreaker, CircuitBreakerError
log = logging.getLogger(__name__)

class CircuitBreakerHttpClient:
    """Async CB + httpx. Нативный `async with circuit` (aiobreaker)."""
    def __init__(self, fallback=None, *,
                 max_failures: int = 5, reset_timeout: float = 30.0,
                 timeout: float = 3.0):
        self.cb = CircuitBreaker(max_failures=max_failures,
                                 reset_timeout=reset_timeout)
        self.client = httpx.AsyncClient(timeout=timeout)
        self.fallback = fallback  # async Callable[[str, dict], Awaitable[Any]] | None

    async def post(self, url: str, json: dict, *, tags: dict | None = None):
        try:
            async with self.cb:                       # aiobreaker: нативный async
                r = await self.client.post(url, json=json)
                r.raise_for_status()
                return r.json()
        except CircuitBreakerError:
            log.warning("CB OPEN for %s -> fallback", url, extra=tags)
            if self.fallback: return await self.fallback(url, json)
            raise
        except httpx.HTTPError as e:
            log.warning("HTTP error %s: %s -> fallback", url, e, extra=tags)
            if self.fallback: return await self.fallback(url, json)
            raise

    async def state(self) -> str:
        """Для Prometheus gauge / UI. Возвращает 'CLOSED' | 'OPEN' | 'HALF_OPEN'."""
        return self.cb.current_state

    async def close(self):
        await self.client.aclose()
```

```python
# Пример использования (Этап 3, fraud step)
async def fraud_fallback(url: str, payload: dict) -> dict:
    # Платёж не теряется: Orchestrator увидит "route_to_dlq" и опубликует в ledger.commands.dlq
    log.error("fraud unavailable, payment -> DLQ",
              extra={"payment_id": payload.get("payment_id")})
    return {"decision": "route_to_dlq"}

client = CircuitBreakerHttpClient(fallback=fraud_fallback,
                                   max_failures=5, reset_timeout=30, timeout=3.0)
result = await client.post("http://mock-fraud:8007/fraud/check", {...})   # port 8007 — tz_v2 §4.1
```

> **Подводный камень №19 (синхронность):** `pybreaker.CircuitBreaker.call()` — sync. `async with pybreaker.CircuitBreaker(...)` **падает** (`__aenter__` нет). В v1-черновике сниппет `async with self.cb:` не скомпилируется. `aiobreaker` даёт нативный `async with` без executor'а.
>
> **Подводный камень №20:** Пороги `max_failures=5` — при 5 подряд ошибках CB `OPEN`. `reset_timeout=30` — через 30s `HALF_OPEN`, один пробный запрос. Uспех → `CLOSED`, fail → снова `OPEN`. Это **не** «навсегда закрыт».
>
> **Подводный камень №21 (metrics):** CB-статус должны видеть в Prometheus — gauge **`pybreaker_open`** (имя каноническое, `tz_v2`/фазы 03/06/07/08 используют его везде; `1=open, 0=closed`, label `service`). Да, имя метрики сохранилось от pybreaker — это **не** ошибка: это конвенция, зафиксированная в ADR-004/ADR-006, менять на `aiobreaker_open` не будем (ломает acceptance). `client.state()` уже есть в §5.6; экспорт в prometheus — Этап 7.
>
> **Подводный камень №21a (порт fraud):** в сниппете выше `mock-fraud:8007` — канон по `tz_v2 §4.1`. Если где-то встретят `:8000` (gateway) — это баг.

### 5.7. State Machine: `shared/state_machine.py`

**Зачем:** жизненный цикл платежа (§3.10 TЗ v2). Триггерится событиями из Kafka.

```python
# shared/state_machine.py
from statemachine import StateMachine, State

class PaymentStateMachine(StateMachine):
    created    = State("Created", initial=True)
    validating = State("Validating")
    processing = State("Processing")   # sub-state: reserved
    success    = State("Success", final=True)
    failed     = State("Failed", final=True)
    rolledback = State("RolledBack", final=True)

    # Transitions (mapping §3.4 TЗ v2)
    validate   = created.to(validating)
    start      = validating.to(processing)
    succeed    = processing.to(success, conditions=["has_debit_ok"])
    fail       = processing.to(failed,   conditions=["has_error"])
    rollback   = processing.to(rolledback, conditions=["reservation_made"])

    # Guard (устанавливается вызывающим кодом)
    has_debit_ok = lambda: True
    has_error    = lambda: False
    reservation_made = lambda: False
```

```python
# Использование
sm = PaymentStateMachine()
sm.validate(); sm.start()
sm._PaymentStateMachine__has_debit_ok = lambda: True   # context-dependent
sm.succeed()
assert sm.current == "Success"
```

> **Подводный камень №22:** python-statemachine **не держит state** между экземплярами. `sm2 = PaymentStateMachine()` — всегда `created`. Для «восстановления» из БД нужно `PaymentStateMachine(starting_state=row.state)`. Это обязательно в Оркестраторе (Этап 3).
>
> **Подводный камень №23 (guards):** Guards по умолчанию `None` — transition всегда доступен. Если забудешь поставить guard — transition вызовется всегда. Рекомендую явно `conditions=[...]`.

### 5.8. Observability kit: `shared/observability.py`

**Зачем:** единый init OTel + Prometheus + structured logs (§4 TЗ v2).

```python
# shared/observability.py
import logging, json, time
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from prometheus_fastapi_instrumentator import Instrumentator
from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware

class JsonLogMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        start = time.perf_counter()
        response = await call_next(request)
        log = {"ts": time.time(), "method": request.method, "path": str(request.url.path),
               "status": response.status_code,
               "duration_ms": int((time.perf_counter() - start) * 1000),
               "trace_id": trace.get_current_span().get_span_context().trace_id.hex}
        print(json.dumps(log, default=str), flush=True)   # stdout → Loki
        return response

def setup(app: FastAPI, service: str, otel_endpoint: str = "tempo:4317"):
    exporter = OTLPSpanExporter(endpoint=otel_endpoint, insecure=True)
    # ... TracerProvider setup (опущено для краткости)
    Instrumentator().instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)
    app.add_middleware(JsonLogMiddleware)

    @app.get("/healthz")
    def healthz(): return {"status": "ok", "service": service}

    @app.get("/_status")
    def status():   # CB-status и т.п. — для UI/тестов
        return {"service": service, "up": True}
```

> **Подводный камень №24:** **stdout logs** — это то, что читает Loki (через promtail). **Не пишите в файл** — иначе Loki не увидит. Формат JSON обязателен (Loki → Grafana Logs parser).
>
> **Подводный камень №25:** `trace.get_current_span().get_span_context().trace_id.hex` — если OTEL не инициирован, вернёт ноль-ид. Убедитесь, что OTel init идёт **до** startup'а приложения (в `setup()`).
>
> **Подводный камень №26 (Prometheus):** `Instrumentator().expose(app, endpoint="/metrics")` — путь должен быть именно `/metrics` (Prometheus default scrape path). Не `/metrics2`.

### 5.9. Idempotency Middleware: `shared/idempotency.py`

**Зачем:** один и тот же `Idempotency-Key` → один и тот же ответ (§3.1 TЗ v2).

```python
# shared/idempotency.py
import redis.asyncio as aioredis
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
import json

class IdempotencyMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, redis_url: str):
        super().__init__(app)
        self.redis = aioredis.from_url(redis_url, decode_responses=True)

    PENDING_TTL = 120        # сек: «в обработке» — короткий, чтобы 5xx не «застревал»
    ANSWER_TTL  = 86400      # сек: финальный ответ — 24h (контракт TЗ v2, solution #1)

    async def dispatch(self, request, call_next):
        key = request.headers.get("Idempotency-Key")
        if not key:
            return await call_next(request)
        # SET NX EX — атомарно: только один запрос «занимает» ключ
        acquired = await self.redis.set(f"idem:{key}", "pending", nx=True, ex=self.PENDING_TTL)
        if not acquired:
            cached = await self.redis.get(f"idem:{key}")
            if cached and cached != "pending":
                return JSONResponse(json.loads(cached))
            # cached == "pending" → первый ещё в обработке (race) → 409, не дублируем
            return JSONResponse({"error":"in_progress"}, status_code=409)
        response = await call_next(request)
        # Кэшируем ответ (только 2xx/4xx) под ДЛИТЕЛЬНЫЙ TTL
        if response.status_code < 500:
            body = b"".join([c async for c in response.body_iterator])
            await self.redis.set(f"idem:{key}", body.decode(), ex=self.ANSWER_TTL)
            return JSONResponse(json.loads(body), status_code=response.status_code,
                                headers=dict(response.headers))
        # 5xx: НЕ кэшируем — TTL pending (120s) отработал, повторный запрос пройдёт заново
        return response
```

> **Подводный камень №27:** `SET NX EX` — **атомарная** проверка+запись. Если просто `GET → SET` — две параллельные запроса с одним ключом **обе** пройдут (race condition). `SET NX` — только один.
>
> **Подводный камень №28 (fallback в PG):** Если Redis не доступен — идемпотентность всё ещё нужна (БД). Fallback: `intake.idempotency_keys` таблица. Это **Этап 4** (Intake), здесь только Redis-шаг.
>
> **Подводный камень №29 (body_iterator):** `response.body_iterator` — генератор. Прочитав его один раз, повторный вызов вернёт пустое. **Нельзя** читать body 2 раза — только один раз и кэшировать.
>
> **Подводный камень №29a (двойной TTL):** метка `pending` и финальный ответ имеют **разные TTL**. `pending` = 120s (заканчивается, если worker 5xx/убит — клиент может повторить), финальный ответ = 24h (контракт TЗ v2 solution #1). Если бы и тот и другой были 24h — один сбой навсегда «застревал бы» ключ.

### 5.10. Smoke-скрипт: `scripts/smoke_env.sh`

> **Подводный камень №30 (curl + kafka-topics на хосте):** `curl` есть в host (мы его ставим в Dockerfile сервисов, и в host-окружении — он есть как системный пакет на dev-машине; если нет — `apt-get install curl`). `kafka-topics.sh` **НЕ** есть на хосте (это JVM-kafka-скрипт внутри контейнера). В smoke-скрипте его вызывать через `docker compose exec kafka`.

```bash
#!/usr/bin/env bash
set -euo pipefail

# 1. Health: 8 сервисов
declare -A SERVICES=(
  [gateway]=8000 [intake]=8001 [orchestrator]=8002 [ledger]=8003
  [profile]=8004 [notification]=8005 [mock-acquirer]=8006 [mock-fraud]=8007
)
for svc in "${!SERVICES[@]}"; do
  port=${SERVICES[$svc]}
  code=$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:${port}/healthz")
  echo "  $svc (:$port): HTTP $code"
  [[ "$code" == "200" ]] || exit 1
done

# 2. Kafka topics (через контейнер, НЕ хост)
TOPICS=$(docker compose exec -T kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --list)
for t in payments.commands payments.events ledger.commands ledger.events \
         payments.commands.dlq ledger.commands.dlq payments.events.dlq; do
  echo "$TOPICS" | grep -qw "$t" || { echo "MISSING TOPIC: $t"; exit 1; }
done

# 3. PG: 4 базы + wal_level
DBS=$(docker compose exec -T postgres psql -U fintech -tAc \
  "SELECT count(*) FROM pg_database WHERE datname IN ('ledger','orchestrator','intake','profile')")
[[ "$DBS" == "4" ]] || { echo "PG: найдено баз = $DBS (нужно 4)"; exit 1; }
WAL=$(docker compose exec -T postgres psql -U fintech -tAc "SHOW wal_level")
[[ "$WAL" == "logical" ]] || { echo "PG: wal_level=$WAL (нужен logical)"; exit 1; }

# 4. Redis ping
[[ "$(docker compose exec -T redis redis-cli ping)" == "PONG" ]] || exit 1

# 5. Kafka Connect: коннектор должен быть RUNNING
STATUS=$(curl -fsS http://localhost:8083/connectors/ledger-outbox/status | grep -oE '"state":"[a-zA-Z]+"' | cut -d'"' -f4)
[[ "$STATUS" == "RUNNING" ]] || { echo "Debezium connector: $STATUS (нужен RUNNING)"; exit 1; }

# 6. Prometheus: все сервисы UP
UP=$(curl -fsS http://localhost:9090/api/v1/targets | grep -c '"health":"UP"')
[[ "$UP" -ge 8 ]] || { echo "Prometheus: $UP/8 UP (нужно >=8)"; exit 1; }

echo "smoke OK — all checks passed"
```

---

## 6. Порядок коммитов (по `implementation_plan.md` §3.2)

| # | Тип | Команды | Что делает |
|---|---|---|---|
| 1 | `chore` | init monorepo | pyproject, Makefile, .gitignore, tree, doc/ |
| 2 | `feat(infra)` | core data stack | postgres(4 bases), redis, kafka-krft, connect |
| 3 | `feat(infra)` | debezium | PG connector |
| 4 | `feat(infra)` | observability | prom, tempo, loki, grafana, alertmanager |
| 5 | `feat(infra)` | healthchecks | `depends_on.condition=service_healthy` |
| 6 | `feat(shared)` | DDD base | Aggregate/Entity/ValueObject + events |
| 7 | `feat(shared)` | db + outbox | async repos + OutboxEvent/Mixin |
| 8 | `feat(shared)` | kafka | Producer/Consumer + backoff + DLQ |
| 9 | `feat(shared)` | circuit | aiobreaker wrapper (async with) |
| 10 | `feat(shared)` | state machine | PaymentStateMachine |
| 11 | `feat(shared)` | observability | OTel + prom + loki-JSON |
| 12 | `feat(shared)` | idempotency | Middleware (Redis) |
| 13 | `feat(services)` | placeholders | 8 сервисов на shared |
| 14 | `chore` | make + smoke | targets + smoke_env.sh |
| 15 | `docs` | ADR | ADR-001 monorepo, ADR-002 KRaft |

**Commit body формат (пример):**
```
feat(shared): add Transactional Outbox model

Why:
  TЗ §3.2 — Outbox через WAL гарантирует Exactly-Once на уровне БД
  без Dual Write problem.

What:
  - OutboxEvent (id, event_id, topic, payload, created_at)
  - OutboxMixin._publish_outbox() — пишет в текущей TX
  - Debezium подхватит insert → ledger.events

Refs: doc/tz_v2.md §3.2, ADR-001
```

---

## 7. Как проверить (exit criteria)

```
make up                                 # 18/18 running
docker compose ps                       # проверить status, не 'restarting'
make smoke                              # 8×/healthz=200 + topics + PG-bases + Redis + Debezium + Prometheus
make test                               # shared smoke-тесты pass
# вручную:
docker compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -E 'commands|events|dlq'
docker compose exec -T redis redis-cli ping                                        # PONG
docker compose exec -T postgres psql -U fintech -tAc "SHOW wal_level"             # logical
curl -fsS http://localhost:8083/connectors/ledger-outbox/status | grep RUNNING   # Debezium
curl -fsS http://localhost:9090/api/v1/targets | grep -c '"health":"UP"'          # >= 8
```

**Красные флаги (если НЕ так — не двигаться дальше):**
- [ ] `docker compose ps` показывает `restarting` или unhealthy.
- [ ] `kafka-topics.sh --list` — нет 4 топов (`*.commands`/`*.events`) + 3 DLQ.
- [ ] `psql ... WHERE datname IN (…)` — не 4 базы.
- [ ] `SHOW wal_level` != `logical`.
- [ ] `curl /healthz` — любой сервис !=200.
- [ ] `from shared.kafka import KafkaProducer` — ImportError.
- [ ] `from shared.domain import Aggregate, AggregateRoot` — ImportError (alias §5.1a).
- [ ] `from shared.domain.events import FundsReserved, FundsDebited, RefundPosted, SagaSucceeded` — ImportError (каталог §5.2).
- [ ] `await circuit.state()` — AttributeError (aiobreaker, §5.6).
- [ ] Debezium connector — state != RUNNING.
- [ ] Prometheus targets — не все UP.

---

## 8. Типичные ошибки (на что реально спотыкаются)

| Ошибка | Симптом | Как избежать |
| --- | --- | --- |
| `wal_level=replica` | Debezium «no logical decoding» | `command: postgres -c wal_level=logical` |
| Забыл `KAFKA_CFG_PROCESS_ROLES` | Kafka молчит, `kafka-topics` — «cluster has no active controller» | KRaft: broker+controller в одном контейнере |
| `expire_on_commit=True` | `DetachedInstanceError` после commit | `async_sessionmaker(..., expire_on_commit=False)` |
| `SET` вместо `SET NX` | race: оба запроса прошли | только `SET NX EX` |
| `print()` вместо JSON | Loki не парсит | `print(json.dumps(log), flush=True)` |
| Забыл `setup(app)` | `/healthz` 404, OTel нет | Каждый сервис должен звать `setup(app)` |
| `pybreaker` в async | `AttributeError: __aenter__` (sync-only, нет `async with`) | `aiobreaker` (нативный `async with`), §5.6 |
| `bitnami/kafka:3.7` | `docker pull` падает (Bitnami ~2025 → bitnami-registry) | официальный `apache/kafka:3.7.0`, §4.3 |
| `python:3.11-slim` без `curl` | HEALTHCHECK + smoke-скрипт падают | `apt-get install curl tini` в Dockerfile, §4.7 |
| Пер-сервисный `pyproject.toml` | workspace дублируется, lock-файлы расходуются | один `pyproject.toml` + `uv.lock` в корне, §4.7 |
| `EnumMapper` как value-converter | Соединение `kafka_connect` не стартует | `JsonConverter` + SMT `RegexRouter` (§4.4) |
| `pending` + финальный ответ с одним TTL | 1 сбой = ключ «навсегда pending» | двойной TTL: pending=120s, ответ=24h, §5.9 |
| `kafka-topics.sh` на хосте | smoke падал (нет JVM на хосте) | `docker compose exec kafka kafka-topics.sh …`, §5.10 |
| Разные `group_id` на 1 consumer | Потеря сообщений | 1 group_id на 1 consumer |
| `depends_on` без `condition` | Service стартует до postgres | `condition: service_healthy` |

---

## 9. Чек-лист перед merge Этапа 1

- [ ] `make down && make up && make smoke` проходит.
- [ ] `docker compose ps` — **18/18 running**, не `restarting`/unhealthy.
- [ ] 8 сервисов отвечают `/healthz` → 200.
- [ ] Kafka: **4 топака** (`payments.commands`, `payments.events`, `ledger.commands`, `ledger.events`) + **3 DLQ** (`*.dlq`).
- [ ] PG: **4 базы** в `pg_database` + `SHOW wal_level` = `logical`.
- [ ] Redis: `redis-cli set test 1` → OK.
- [ ] Debezium connector: `GET :8083/connectors/ledger-outbox/status` → `RUNNING`.
- [ ] Prometheus: 8 сервисов в `/api/v1/targets` (UP).
- [ ] `from shared.domain import Aggregate, AggregateRoot, Entity, ValueObject` — OK (alias §5.1a).
- [ ] `from shared.domain.events import FundsReserved, FundsDebited, RefundPosted, SagaSucceeded, DebitFailed` — OK (каталог §5.2, канон `tz_v2 §3.11.1`).
- [ ] `from shared.circuit import CircuitBreakerHttpClient` — OK (aiobreaker, §5.6).
- [ ] `from shared.kafka import KafkaProducer, KafkaConsumer` — OK.
- [ ] `from shared.outbox import OutboxEvent, OutboxMixin` — OK.
- [ ] `from shared.idempotency import IdempotencyMiddleware` — OK.
- [ ] `from shared.state_machine import PaymentStateMachine` — OK.
- [ ] `from shared.observability import setup` — OK.
- [ ] 15 коммитов в `feat/01-infra`, каждый с body+ADR-ссылкой.
- [ ] ADR-001 monorepo, ADR-002 KRaft (`apache/kafka`, не bitnami).
- [ ] `git log --oneline` — чистая история.

> **После merge в `main`** — открывать Этап 2 (`02_ledger_write_side.md`).
