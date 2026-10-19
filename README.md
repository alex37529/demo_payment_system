# fintech_demo — payments PoC (FinTech)

Event-driven payments demo showing best practices: **Saga orchestration**,
**Transactional Outbox** (WAL + Debezium CDC), **CQRS**, **DDD aggregates**,
**Circuit Breaker**, **Idempotency**. Full TЗ: `doc/tz_v2.md`, plan:
`doc/implementation_plan.md`, phase docs: `doc/phases/`.

## Phase 1 (this) — environment + shared abstractions + 8 placeholders

`make up` brings up **18 containers**: Postgres (4 bases) · Redis 7 · Kafka
(KRaft) · Kafka Connect + Debezium · Prometheus/Grafana/Tempo/Loki/Alertmanager ·
promtail · 8 FastAPI placeholder services. `shared/` provides DDD base, event
catalog, async DB + outbox, Kafka client (retry + DLQ), circuit breaker, state
machine, observability, and idempotency — the contract for phases 2–6.

## Stack
Python 3.11 · FastAPI · SQLAlchemy 2.0 (async) · Pydantic v2 · aiokafka ·
aiobreaker · python-statemachine · OpenTelemetry → Tempo · Prometheus · Loki.

## Layout
```
shared/          # DDD + db + outbox + kafka + circuit + state_machine + observability + idempotency
services/<name>/ # 8 placeholder FastAPI apps (each: POST /healthz, /metrics, /_status)
docker-compose.yml
kafka/           # topics.sh + Debezium connector + register script
init/            # 01_databases.sql (4 bases + outbox + publication)
observability/   # prometheus / tempo / loki / promtail / grafana / alertmanager
scripts/         # smoke_env.sh, acceptance.sh, register_connector.py
doc/             # TЗ v2, plan, phases, ADRs
```

### Container naming (deliberate: "ours" vs third-party)
Every container has an **explicit** `container_name` (no more `loki-1`, `gateway-1`);
the `fintech_` prefix separates them from unrelated dockers on the host.

| Group | Container names |
| --- | --- |
| **Our services** (8) | `fintech_gateway_service`, `fintech_intake_service`, `fintech_orchestrator_service`, `fintech_ledger_service`, `fintech_profile_service`, `fintech_notification_service`, `fintech_mock_acquirer_service`, `fintech_mock_fraud_service` |
| Our one-shot helpers | `fintech_kafka_topics_init`, `fintech_debezium_register` |
| Third-party infra | `fintech_postgres`, `fintech_redis`, `fintech_kafka`, `fintech_kafka_connect`, `fintech_prometheus`, `fintech_tempo`, `fintech_loki`, `fintech_promtail`, `fintech_alertmanager`, `fintech_grafana` |

> The compose **service keys** (DNS) keep the short names (`gateway`, `ledger`, `tempo`, …) —
> services discover each other by those; `container_name` only affects `docker ps` / `docker logs`.

## Run (phase 1)
```bash
# Requires Docker (Linux containerd), >= 8 GB RAM, Git Bash (Windows).
docker compose up -d --build      # or: make up
docker compose ps                 # 18/18 running
bash scripts/smoke_env.sh         # or: make smoke  (healthz x8 + topics + PG + redis + Debezium + Prometheus)
python -m pytest -q               # or: make test   (22 shared tests)
```

### Quick checks
```bash
curl http://localhost:8000/healthz            # {"status":"ok","service":"gateway"}
curl -s http://localhost:8000/metrics | grep http_request
curl -s http://localhost:8083/connectors/ledger-outbox/status   # ... "state":"RUNNING"
# UI: Grafana  http://localhost:3000  (admin/admin)  — dashboard "payments-overview"
# Tempo:       http://localhost:3200   ·   Prometheus: http://localhost:9090
```

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
| | | | **grafana** | 3000 |

## Teardown
```bash
docker compose down            # make down
docker compose down -v         # make clean
```

> **Next:** phase 2 — `doc/phases/02_ledger_write_side.md` (Ledger write-side + Outbox).
