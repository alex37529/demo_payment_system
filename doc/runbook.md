# Runbook

Operational notes for `fintech_demo`. Every command assumes the repo root
and a running stack (`make up`).

## Start / stop

```bash
make up            # docker compose up -d --build   (18 containers + 2 one-shot)
make ps            # expect 18/18 "healthy"
make smoke         # healthz x8 + topics + PG + redis + Debezium + Prometheus
make down          # stop, keep the volumes
make clean         # stop + delete the volumes (fresh seed on the next `up`)
```

> The seed (`init/01_databases.sql`) runs **only on a fresh volume**. After
> `make clean` the 20 users are re-seeded; on an existing volume the
> balances are whatever the last run left them at.

## Logs

All services write **one JSON object per request** to stdout; promtail
ships them to Loki and promotes `service` + `level` to labels.

```bash
docker compose logs -f intake            # follow one service
# Loki (labels are indexed — filter on them, not on the text):
curl -sG http://localhost:3100/loki/api/v1/query_range \
  --data-urlencode 'query={service="intake"}' \
  --data-urlencode 'limit=20' --data-urlencode 'direction=backward'
# Grafana -> Logs: {service="intake"} | level="ERROR"
```

> **Pitfall:** Loki indexes **labels only** — a text filter like
> `msg:payment_created` does not work. `service` and `level` are labels;
> `trace_id` is in the line body (search it with `|= "..."`).

## Metrics & dashboards

```bash
curl -s 'http://localhost:9090/api/v1/query?query=pybreaker_open'   # CB state (1=open)
curl -s 'http://localhost:9090/api/v1/query?query=kafka_consumer_lag'
curl -s 'http://localhost:9090/api/v1/query?query=dlq_messages_total'
# Grafana http://localhost:3000 (admin/admin) -> "FinTech / payments-overview" (7 panels)
```

## Traces

```bash
# Tempo search: which traces exist?
curl -s 'http://localhost:3200/api/search?limit=10' | jq '.traces[] | {traceID, rootServiceName, durationMs}'
# One trace in full:
curl -s 'http://localhost:3200/api/traces/<traceID>' | jq '.batches'
# Grafana -> Explore -> Tempo: search by service, then click a span -> Logs
```

A single payment produces **one** trace across Gateway → Intake →
Orchestrator → Ledger (ADR-009). The `ledger.events` hop is produced by
Debezium and starts a new trace (ADR-008).

## Failure injection

```bash
# Break the fraud API -> the circuit breaker opens
curl -X POST http://localhost:8007/fraud/admin/fail \
  -H 'Content-Type: application/json' -d '{"mode":"errors"}'   # 500 on every check
curl -X POST http://localhost:8007/fraud/admin/fail \
  -H 'Content-Type: application/json' -d '{"mode":"timeout"}'   # sleep 5s (> CB timeout 3s)
# Recover
curl -X POST http://localhost:8007/fraud/admin/fail \
  -H 'Content-Type: application/json' -d '{"mode":"off"}'
curl http://localhost:8007/fraud/status     # current mode
```

Watch it happen: `pybreaker_open` → 1, the Grafana badge turns red, the
`CircuitBreakerOpen` alert fires, and the failed payments land in
`ledger.commands.dlq`.

## Load test

```bash
make load                     # mock-acquirer burst (50 TPS / 60 s) + status poll
make load-slo                 # the harness: percentiles + a p99 SLO gate
python scripts/load_test.py --tps 100 --duration 30 --url http://localhost:8001/v1/payments
```

`load-slo` exits **1** when the p99 exceeds 300 ms (the SLO, tz_v2 §4) or a
5xx is seen. Measured on this machine: **50.02 TPS, p50 52 ms, p95 58 ms,
p99 60 ms, 0 errors**.

> The reported throughput is the **achieved** one, not the requested one
> (spec pitfall #14): a request slower than the 20 ms slot makes the
> achieved rate drop below the requested one — that is a fact about the
> system.

## DLQ inspection

```bash
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic ledger.commands.dlq \
  --from-beginning --max-messages 10
```

The DLQ messages are the *original* payload plus `error` and `attempts`.
`payments.commands.dlq` = the producer's retry budget was exhausted;
`ledger.commands.dlq` = a payment failed the fraud check (deliberate).

## Tests

```bash
make test            # the hermetic unit suite (no Docker needed)
make e2e             # the E2E suite against the LIVE stack (needs `make up`)
```

The E2E suite covers the whole chain (outbox → Debezium → Kafka → the
read-side), the idempotent replay, the own-currency invariant, the Loki
pipeline and the trace continuity.

## Acceptance (phase 7 → 8)

```bash
make acceptance      # the 7 checks of tz_v2 §7
```

## When something is wrong

| Symptom | Likely cause | What to do |
| --- | --- | --- |
| The intake returns 503 `kafka_unavailable` | Kafka is down / not reachable | `make ps`; `docker compose logs kafka` |
| The balance does not move | the saga failed (fraud / funds) | `docker compose logs orchestrator`; check `ledger.commands.dlq` |
| Loki is empty | the log driver is not `json-file` | `docker inspect -f '{{.HostConfig.LogConfig.Type}}' fintech_intake_service` must print `json-file` |
| No spans in Tempo | the service has no `OTEL_EXPORTER_OTLP_ENDPOINT` | `docker compose ps`; the env var is set per service in `docker-compose.yml` |
| `pybreaker_open` is missing | the orchestrator did not start | `docker compose logs orchestrator` |
| The Debezium connector is not RUNNING | it was not registered | `docker compose logs debezium`; `curl -s http://localhost:8083/connectors` |
| The outbox has pending rows forever | the delivery tracker cannot reach Kafka | `docker compose logs ledger` |

## Rebuilding after a code change

```bash
make up              # rebuilds the images
# or a single service:
docker compose up -d --build intake
```

The observability containers mount their configs from `observability/` — a
config change needs a **restart** (Prometheus/Alertmanager cache their
config file):

```bash
docker compose restart prometheus alertmanager
```

The `logs` (Loki) pipeline needs a restart too, because the JSON→label
extraction is a promtail pipeline stage:

```bash
docker compose restart promtail
```
