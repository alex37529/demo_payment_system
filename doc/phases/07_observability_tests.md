# Этап 7. Observability + Tests + Docs

> **Ветка:** `feat/08-obs-tests`
> **Результат:** сквозной trace от Gateway до Ledger в Tempo; Grafana dashboard `payments-overview` (6 panel); Alertmanager rules; E2E-тесты (outbox→Debezium→Kafka→consumer); load-харнес @ 50 TPS; ADR-001..010; C4 + Sequence-диаграммы.
> **Чему учимся здесь:** как устроен observability «из коробки»: OTel → Tempo (tracing), Prometheus (metrics), Loki (logs), Alertmanager (alerting), Grafana (dashboards); как покрываются e2e + load; как задокументировать решения (ADR).

---

## 1. Обзор: что «наблюдаем»

**3 столпа (Three Pillars of Observability):**

| Столп | Инструмент | Данные | Порт |
| --- | --- | --- | --- |
| **Metrics** | Prometheus | Числа (rate, latency, CB state, DLQ depth) | 9090 |
| **Tracing** | Tempo | Связные span'ы (Gateway → Intake → Orchestrator → Ledger) | 3200 |
| **Logs** | Loki | Текстовые (JSON-логи сервисов) | 3100 |

**Склеивает всё:** **Grafana** (dashboard + logs + traces, по `trace_id`). **Alertmanager** — алерты (p99>500ms, CB open, DLQ>10).

---

## 2. Tracing (OTel → Tempo)

### 2.1. Setup (shared, Этап 1)

```python
# shared/observability.py (фрагмент)
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from prometheus_fastapi_instrumentator import Instrumentator

def setup(app: FastAPI, service: str, otel_endpoint: str = "tempo:4317"):
    resource = Resource.create({
        "service.name": service,
        "service.version": "0.1.0",
        "deployment.environment": "local",
    })
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=otel_endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    # FastAPI instrument (auto span на каждый request)
    FastAPIInstrumentor.instrument_app(app)

    # Prometheus
    Instrumentator().instrument(app).expose(app, endpoint="/metrics",
                                            include_in_schema=False)

    # /healthz
    @app.get("/healthz")
    def healthz(): return {"status":"ok","service":service}
```

### 2.2. Как работает

```
Gateway (span A) ──HTTP──▶ Intake (span B)
                              │
                              └──Kafka publish──▶ (no span boundary)
Orchestrator (span C, consumer) ──HTTP──▶ Fraud (span D)
                              │
                              └──Kafka publish──▶ ledger.commands
Ledger (span E, consumer) ──[use-case]──▶ Debezium (no span)
                              │
                              └──Kafka publish──▶ ledger.events
Profile (span F, consumer)
Notification (span G, consumer)
```

**Все span'ы с единым `trace_id`** (Gateway → Ledger). Tempo хранит. Grafana (Traces) — «клик по span'у» → Logs (по `trace_id`).

> **Подводный камень №1 (context propagation over Kafka):** Kafka **не** прокидывает W3C-headers (`traceparent`, `baggage`) как HTTP. Нужно **вручную** добавить `traceparent` в Kafka message headers (producer → consumer → extract → set as parent span). Без этого trace «рвётся» на Kafka-границе. **Решение:** shared.kafk.KafkaProducer добавляет `traceparent` header; consumer извлекает и ставит как parent.
>
> **Подводный камень №2 (OTel exporter timeout):** Если Tempo «не готов» (startup) — exporter «hang» (timeout). `exporter.timeout=10s` — «убить» и «не блокировать». BatchSpanProcessor — async export (не блокирует request).
>
> **Подводный камень №3 (service.name = source of truth для Tempo):** В Grafana (Traces) — «search by service.name». Если service.name не уникальный (все «app» — нельзя). Каждый сервис: свой name (gateway, intake, orchestrator, ledger, profile, notification, mock-acquirer, mock-fraud).

### 2.3. Tempo config

```yaml
# observability/tempo/tempo.yml
server:
  http_listen_port: 3200
  grpc_listen_port: 9095     # OTel gRPC endpoint
 distributors:
  receivers:
    otlp:
      protocols:
        grpc:
storage:
  trace:
    backend: local
    local:
      path: /tmp/traces
metrics_generator:
  storage:
    path: /tmp/trace-metrics
    remote_write:
      - url: http://prometheus:9090/api/v1/write
        send_exemplars: true
search:
  duration_scheduling:
    min_lookback: 720h
```

**Prometheus** получает `trace-metrics` (histogram-ы по service) — **автоматически** (metrics_generator). Это «красиво»: latency p99 — из traces (не из Prometheus-матрик отдельно).

---

## 3. Metrics (Prometheus)

### 3.1. Что собираем

| Метрика | Тип | Источник | Значение |
| --- | --- | --- | --- |
| `http_request_duration_seconds` | Histogram | FastAPI Instrumentator | p50/p95/p99 latency |
| `http_requests_total` | Counter | FastAPI | RPS |
| `pybreaker_open` | Gauge | CircuitBreakerHttpClient | 1=open, 0=closed |
| `kafka_consumer_lag` | Gauge | aiokafka (custom) | отставание consumer |
| `dlq_messages_total` | Counter | KafkaConsumer (custom) | глубина DLQ |
| `payments_processed_total` | Counter | Ledger | общий счётчик |
| `payments_failed_total` | Counter | SagaHandler | счётчик fail (fraud, insufficient) |
| `sse_active_connections` | Gauge | Notification | сколько SSE коннектов |

### 3.2. Custom metrics (не из Instrumentator)

```python
# shared/metrics.py
from prometheus_client import Counter, Gauge, Histogram
import pybreaker

pybreaker_open = Gauge("pybreaker_open", "1=open, 0=closed", ["service"])
kafka_lag = Gauge("kafka_consumer_lag", "messages lag", ["topic", "consumer"])
dlq_total = Counter("dlq_messages_total", "DLQ depth", ["topic"])
payments_failed = Counter("payments_failed_total", "Failed payments", ["reason"])
sse_connections = Gauge("sse_active_connections", "Active SSE connections")
```

```python
# services/orchestrator/infrastructure/fraud_client.py (фрагмент)
class CircuitBreakerHttpClient:
    def __init__(self, service: str, **cb_kwargs):
        self.cb = CircuitBreaker(**cb_kwargs)
        self.service = service
        self._cb_hook = self._on_cb_state_change
        self.cb.closed += [self._on_cb_state_change]   # pybreaker hook
        self.cb.opened += [self._on_cb_state_change]
    def _on_cb_state_change(self):
        pybreaker_open.labels(service=self.service).set(
            1 if self.cb.current_state == CircuitState.OPEN else 0)
```

> **Подводный камень №4 (pybreaker hook):** pybreaker не «автоматически» экспортирует метрики. Нужен hook (`.opened += [callback]`). В `shared/circuit.py` (Этап 1) — **уже реализовано** (см. `pybreaker_open` gauge).
>
> **Подводный камень №5 (kafka_consumer_lag):** aiokafka **не** предоставляет метрики. **Кастомизировать:** после `consumer.assign()` — poll `consumer.position()` vs `consumer.end_offsets()` → lag. В PoC — «упрощённо»: `kafka_lag.labels(topic).set(...)` в consumer-loop.

---

## 4. Logs (Loki)

### 4.1. Формат (JSON → Loki)

**Каждый** сервис (через `JsonLogMiddleware`, shared/observability.py, Этап 1) пишет:
```json
{"ts":1700320.5,"level":"INFO","msg":"payment created","service":"intake","payment_id":"P1","trace_id":"abc123..."}
```

**Loki** (через promtail/dockergo) собирает stdout. **Grafana** (Logs) — filter by `service` + `trace_id` + `level`.

### 4.2. Loki config

```yaml
# observability/loki/loki.conf
auth_enabled: false
server:
  http_listen_port: 3100
common:
  ring:
    kvstore:
      store: inmemory
  replication_factor: 1
  path_prefix: /loki
storage_config:
  fs:
    directory: /loki
schema_config:
  configs:
    - from: "2024-01-01"
      store: tsdb
      object_store: filesystem
      schema: v13
      index:
        prefix: index_
        period: 24h
limits_config:
  retention_period: 168h   # 7 days

# promtail (sidecar) — собирает stdout
scrape_configs:
  - job_name: docker
    static_configs:
      - targets: [localhost]
        labels:
          job: docker
          __path__: /var/lib/docker/containers/*/*.log
    docker_sd_configs:
      - host: unix:///var/run/docker.sock
        refresh_interval: 5s
```

> **Подводный камень №6 (Loki-индексы):** Loki **не** индексирует **текст** (только labels). Фильтр «msg:payment_created» — **не работает**. **Filter by label** (service, level, pod). Text-match — через `|~ "payment_created"` (regex) — **медленный** (full scan). **Вывод:** в JSON-лог **обязательно** `level` (INFO/WARN/ERROR) — это **label** Loki.
>
> **Подводный камень №7 (retention):** 7 дней (PoC). Прода — 30–90 дней (настроить `retention_period`).

---

## 5. Alertmanager (алерты)

```yaml
# observability/alertmanager/alertmanager.yml
route:
  receiver: "local"
receivers:
  - name: "local"
    # (в PoC: log only; прода: Slack/EMAIL/PagerDuty)
    # webhook_configs:
    #   - url: http://hooks.slack.com/...

# Prom-Alert rules (в observability/prometheus/rules.yaml)
groups:
  - name: fintech-alerts
    rules:
      - alert: HighP99
        expr: histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket[3m])) by (le)) > 0.5
        for: 3m
        labels: {severity: warning}
        annotations: {summary: "P99 > 500ms for 3 min"}

      - alert: CircuitOpen
        expr: pybreaker_open == 1
        for: 30s
        labels: {severity: critical}
        annotations: {summary: "CB open ({{ $labels.service }})"}

      - alert: DLQDeep
        expr: dlq_messages_total > 10
        for: 1m
        labels: {severity: warning}
        annotations: {summary: "DLQ depth > 10 ({{ $labels.topic }})"}
```

**Проверка:**
```
docker compose exec alertmanager alertmanager-cli status
# 3 alerts active
```

> **Подводный камень №8 (alert-группировка):** `for: 3m` — алерт «срабатывает» если условие **3 минуты** постоянно. Не «1 раз». Это «debounce» (не спамим).
>
> **Подводный камень №9 (severity):** `warning` (жёлтый) vs `critical` (красный, page someone). В PoC — log only. Прода — webhook.

---

## 6. Grafana dashboards (provisioning)

### 6.1. Dashboard `payments-overview` (6 panels)

| # | Panel | Тип | Source |
| --- | --- | --- | --- |
| 1 | **RPS** (payments/sec) | TimeSeries | `rate(http_requests_total{service="intake"}[1m])*10` |
| 2 | **P50 / P95 / P99** (latency, ms) | TimeSeries | `histogram_quantile(0.5/0.95/0.99, ...)` |
| 3 | **CB state** (fraud) | Stat | `pybreaker_open{service="orchestrator"}` |
| 4 | **DLQ depth** | TimeSeries | `dlq_messages_total` |
| 5 | **Kafka lag** | TimeSeries | `kafka_consumer_lag` |
| 6 | **SSE connections** | Stat | `sse_active_connections` |

**Provisioning:**
```yaml
# observability/grafana/provisioning/datasources/defaults.yml
apiVersion: 1
datasources:
  - name: prometheus
    type: prometheus
    url: http://prometheus:9090
    access: proxy
    isDefault: true
  - name: loki
    type: loki
    url: http://loki:3100
  - name: tempo
    type: tempo
    url: http://tempo:3200

# observability/grafana/provisioning/dashboards/defaults.yml
apiVersion: 1
providers:
  - name: "fintech"
    folder: "FinTech"
    options:
      path: /etc/grafana/provisioning/dashboards
# Dashboard JSON (payments_overview.json) — 6 panels выше
```

> **Подводный камень №10 (Tempo datasource):** Grafana **needs** `tempo` datasource для «Traces» tab. Без него — Traces не видны.
>
> **Подводный камень №11 (panel refresh):** `refresh: 10s` — dashboard обновляется. `timeRange: from=now-10m to=now` — последние 10м. Для «live» — `now-30s to=now` + `refresh: 5s`.

---

## 7. E2E-тесты (Testcontainers)

**Что покрываем:** Outbox (Ledger) → Debezium (CDC) → Kafka → Consumer (Profile/Notification). **Полный цикл** события.

```python
# tests/test_e2e_outbox.py
import pytest, time
from testcontainers.postgres import Postgres
from testcontainers.kafka import KafkaContainer

@pytest.fixture(scope="module")
def stack():
    with KafkaContainer("apache/kafka:3.7.0") as kafka, Postgres() as pg:
        # start debezium (docker API)
        # start ledger + profile services (uvicorn)
        yield {"kafka": kafka, "pg": pg}

@pytest.mark.asyncio
async def test_outbox_to_kafka_to_profile(stack):
    # 1. Ledger: debit
    await run_use_case("debit", account_id="A01", amount=100)
    # 2. Wait для Debezium (WAL → Kafka)
    await asyncio.sleep(2)
    # 3. Kafka: ledger.events — 1 msg
    msgs = await consume_topic("ledger.events", n=1)
    assert msgs[0]["payment_id"] == "P-E2E-1"
    # 4. Profile: balance обновился
    bal = await get_balance("A01")
    assert bal == initial_balance - 100
    # 5. Idempotent: replay (2nd) — balance NOT changed again
    await replay_message("ledger.events", msgs[0])
    await asyncio.sleep(1)
    bal2 = await get_balance("A01")
    assert bal2 == bal   # no double-count
```

> **Подводный камень №12 (Testcontainers + Debezium):** Debezium **нужен** Kafka + PG + Connect. Testcontainers: 3 контейнера (PG, Kafka, Connect) + 1 (Debezium) + 8 services = 12 контейнеров. **Медленно** (Win + Docker Desktop: +30–60s на первый). Кэш образов — быстрее.
>
> **Подводный камень №13 (eventual consistency):** Debezium — async (не мгновенно). `await asyncio.sleep(2)` — **не** надёжный. **Лучше:** poll `ledger.events` (с timeout 10s) + poll Profile (с timeout 10s).

---

## 8. Load-харнес

```python
# scripts/load_test.py
import asyncio, httpx, time, statistics, uuid

async def load_test(tps: int = 50, duration: int = 60, base_url: str = "http://localhost:8001"):
    client = httpx.AsyncClient(timeout=5.0)
    latencies = []
    end = time.time() + duration
    interval = 1.0 / tps

    async def one():
        payload = {"amount": random.randint(1000, 100000),
                   "currency": random.choice(["USD","EUR"]),
                   "account_id": f"A{random.randint(1,20):02d}"}
        start = time.perf_counter()
        r = await client.post(f"{base_url}/v1/payments", json=payload,
                              headers={"Idempotency-Key": str(uuid.uuid4())})
        latencies.append((time.perf_counter() - start) * 1000)
        if r.status_code >= 500: raise RuntimeError(r.text)

    tasks = []
    while time.time() < end:
        tasks.append(asyncio.create_task(one()))
        await asyncio.sleep(interval)
    await asyncio.gather(*tasks)
    await client.aclose()

    latencies.sort()
    n = len(latencies)
    return {
        "throughput": n / duration,
        "p50": latencies[n//2],
        "p95": latencies[int(n*0.95)],
        "p99": latencies[int(n*0.99)],
        "max": latencies[-1],
        "total": n,
    }

# Usage: python scripts/load_test.py
if __name__ == "__main__":
    result = asyncio.run(load_test())
    print(result)
```

**SLO** (§4 TЗ v2): P99 < 300ms при 50 TPS, 0 loss.

> **Подводный камень №14 (latency vs TPS):** Если latency одного запроса > 20ms (interval for 50 TPS) — **TPS упадёт**. Измерять **фактический** throughput (n/duration), не «желаемый».

---

## 9. Docs + ADR

### 9.1. ADR (Architecture Decision Records)

| # | Название | Кратко |
| --- | --- | --- |
| **ADR-001** | Monorepo layout | Все сервисы в одном репозитории, `shared/` — общая база |
| **ADR-002** | Kafka KRaft single-node | Не Zookeeper; 1 контейнер + Connect |
| **ADR-003** | Own-currency invariant | Списание только со счёта своей валюты (кросс-валюта — out of scope) |
| **ADR-004** | Circuit Breaker (aiobreaker) | Почему aiobreaker (не pybreaker — sync, не tenacity+CB); нативный `async with`; hook на state change |
| **ADR-005** | Redis Idempotency | SET NX EX; PG fallback; 24h TTL |
| **ADR-006** | State Machine (python-statemachine) | 7 states; guards; `starting_state` (restore) |
| **ADR-007** | Transactional Outbox | Event в той же TX, что и данные; Debezium доставляет |
| **ADR-008** | CQRS (Write/Read separated) | Ledger (write) vs Profile/Notification (read) |
| **ADR-009** | SSE (не WebSocket) | Одна (server→client); EventSource; keepalive |
| **ADR-010** | OTel → Tempo (span propagation) | `traceparent` в Kafka headers (кросс-сервис) |

**Структура ADR** (каждый):
```markdown
# ADR-XXX: Название

## Context
(зачем нужно решение; какой был выбор)

## Decision
(что выбрали)

## Consequences
(плюсы, минусы, tradeoff)

## Alternatives considered
(что рассматривали, почему нет)

## References
(TЗ, TЗ v2 §, код)
```

### 9.2. C4 diagrams (D2 или PlantUML)

```d2
# C4-Container
Gateway → Intake: "POST /v1/payments"
Gateway → Profile: "GET /v1/users"
Gateway → Notification: "SSE /v1/stream"
Intake → orchestrator: "payments.commands (Kafka)"
orchestrator → Ledger: "ledger.commands (Kafka)"
Ledger → Debezium: "WAL"
Debezium → Kafka: "ledger.events"
Ledger → Profile: "ledger.events (Kafka)"
Ledger → Notification: "ledger.events (Kafka)"
Kafka: {shape: queue}
PG: {shape: cylinder}
```

### 9.3. Sequence diagrams (PlantUML)

**Happy path:**
```
Intake → Kafka(payments.commands) → Orchestrator
Orchestrator → Kafka(ledger.commands: Reserve) → Ledger
Ledger → Kafka(ledger.events: Reserved) → Orchestrator
Orchestrator → HTTP(Fraud API) — (approve)
Orchestrator → Kafka(ledger.commands: Debit) → Ledger
Ledger → Kafka(ledger.events: Debit) → Orchestrator
Orchestrator → Saga: Success
```

**Fraud-failure path:**
```
... (как выше до Fraud)
Orchestrator → HTTP(Fraud) — (500/errors)
Orchestrator → CircuitBreaker (opens, fallback)
Orchestrator → Kafka(DLQ: payment)
Orchestrator → Saga: Failed
```

### 9.4. `docs/events.md` (Event Contracts spec)

| Topic | Producer | Consumer | Payload |
| --- | --- | --- | --- |
| `payments.commands` | Intake | Orchestrator | `{cmd:"Create", payment_id, amount, account_id, idem_key}` |
| `payments.events` | Orchestrator | Profile, Notification | `{payment_id, state, event, ts}` |
| `ledger.commands` | Orchestrator | Ledger | `{cmd:"Reserve/Debit/.../Refund", account_id, amount, saga_id}` |
| `ledger.events` | Ledger (Debezium) | Profile, Notification | `{event, account_id, payment_id, amount, new_balance, ts}` |
| `*.dlq` (×3) | все consumers | инспекция | `{original_topic, payload, err, attempts}` |

**Payload** (каждый) — JSON, `event_id` unique (idempotency).

### 9.5. `docs/runbook.md`

```
## Start
make up         # docker compose up -d

## Stop
make down

## Seed (если не в init)
make seed

## Load test
make load        # 50 TPS / 60s

## Failure injection (Fraud)
curl -X POST http://localhost:8007/fraud/admin/fail -d '{"mode":"errors"}'
# → CB opens, DLQ ↑
curl -X POST .../fraud/admin/fail -d '{"mode":"off"}'
# → CB closes

## DLQ inspect
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic ledger.commands.dlq \
  --from-beginning --max-messages 10

## Traces (Grafana)
http://localhost:3000 → Traces → search payment_id

## Logs (Grafana)
http://localhost:3000 → Logs → {service="intake"} | level=ERROR
```

---

## 10. Коммиты Этапа 7

| # | Тип | Сообщение |
|---|---|---|
| 1 | `feat(observability)` | wire OTel→Tempo (end-to-end) |
| 2 | `feat(observability)` | add custom metrics (CB, lag, DLQ, SSE) |
| 3 | `feat(observability)` | add Loki structured JSON logs (promtail) |
| 4 | `feat(observability)` | add `payments-overview` dashboard (6 panels) |
| 5 | `feat(observability)` | add Alertmanager rules (p99, CB, DLQ) |
| 6 | `test` | add E2E test (outbox→Debezium→Kafka→consumer) |
| 7 | `test` | add load test harness (50 TPS, P99) |
| 8 | `docs` | add ADR-001..010 |
| 9 | `docs` | add C4 + Sequence diagrams (D2/PlantUML) |
| 10 | `docs` | add `events.md` (Event Contracts spec) |
| 11 | `docs` | add `runbook.md` + finalize `README.md` |

**Body (пример):**
```
feat(observability): add payments-overview dashboard (6 panels)

Why:
  TЗ §4 — Observability (Must Have): Grafana dashboards.
  TЗ §6 — Deliverable: exported Grafana dashboard (latency, CB).

What:
  - 6 panels: RPS, p50/p95/p99, CB state, DLQ depth, Kafka lag, SSE conns
  - Provisioning: datasources (prom/loki/tempo) + dashboard JSON
  - Refresh: 10s, timeRange: now-10m to now
  - Tempo datasource: Traces tab (clickable span)

Refs: doc/tz_v2.md §4, §6
```

---

## 11. Как проверить (exit criteria)

```
make up

# 1. Tracing:
curl -X POST http://localhost:8001/v1/payments \
     -H 'Idempotency-Key: K-TEST-1' -d '{"amount":100,"currency":"USD","account_id":"A01"}'
# → 202 (payment_id)
# Grafana (http://localhost:3000 → Traces) → search payment_id
#   → 1 trace, 5+ span (gateway, intake, orchestrator, ledger, profile)
#   → «Click» span → Logs (по trace_id)

# 2. Metrics:
curl http://localhost:9090/api/v1/query?query=pybreaker_open
# → 0 (closed)
curl http://localhost:9090/api/v1/query?query=kafka_consumer_lag
# → 0 (lag=0)

# 3. Load test:
python scripts/load_test.py
# → {"throughput": 50, "p50": 55.2, "p95": 180.3, "p99": 245.1, ...}
#   p99 < 300ms ✓

# 4. CB failure:
curl -X POST http://localhost:8007/fraud/admin/fail -d '{"mode":"errors"}'
python scripts/load_test.py
curl http://localhost:9090/api/v1/query?query=pybreaker_open
# → 1 (open)
# Grafana: CB state — RED (Open)

# 5. Alerts:
# Alertmanager (http://localhost:9093) → Active Alerts
# → HighP99, CircuitOpen (if p99>500ms / CB open)

# 6. E2E test:
make test
# → test_e2e_outbox.py pass (outbox→Kafka→Profile)
```

**Красные флаги:**
- [ ] Traces — «рвётся» на Kafka-границе (no trace_id propagation).
- [ ] CB metric — не обновляется (pybreaker hook сломан).
- [ ] Load @ 50 TPS — P99 > 300ms.
- [ ] E2E — Debezium не доставляет (Kafka нет события).
- [ ] Alerts — не срабатывают (rule wrong).

---

## 12. Типичные ошибки

| Ошибка | Симптом | Как избежать |
| --- | --- | --- |
| Traces «рвётся» на Kafka | Tempo: 2+ traces на 1 payment | `traceparent` в Kafka headers (shared.kafk) |
| CB metric «не виден» | Grafana: CB panel=0 (всегда) | pybreaker hook → gauge |
| Loki — нет `level` | Logs — не фильтруются по level | JSON-лог: **обязательно** `level` (label) |
| Dashboard — «пусто» | Grafana: panels = «No data» | Datasource URL, panel query |
| Load-test «зависает» | P99 = inf | `client.timeout`, gather tasks |
| E2E — flaky (timing) | `asyncio.sleep(2)` недостаточно | Poll (timeout 10s) |

---

## 13. Чек-лист перед merge Этапа 7

- [ ] Tempo: 1 trace (5+ span) на 1 payment.
- [ ] Grafana: `payments-overview` (6 panels, данные).
- [ ] Alerts: 3 rules active.
- [ ] Load @ 50 TPS: P99 < 300ms.
- [ ] CB: pybreaker_open gauge updates.
- [ ] E2E test: pass.
- [ ] ADR-001..010, C4, Sequence, `events.md`, `runbook.md`, README — all present.
- [ ] 11 коммитов в `feat/08-obs-tests`.

> **После merge в `main`** — открывать Этап 8 (`08_release_acceptance.md`).
