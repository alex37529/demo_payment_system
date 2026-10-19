#!/usr/bin/env bash
# Phase 1 smoke (01_environment_abstractions §5.10).
# Checks: 8x /healthz, 4 topics + 3 DLQ, 4 PG bases + wal_level, Redis PONG,
# Debezium ledger-outbox RUNNING, Prometheus >=8 UP targets.
set -euo pipefail

fail() { echo "SMOKE FAIL: $*" >&2; exit 1; }

# 1. Health: 8 services
declare -A SERVICES=(
  [gateway]=8000 [intake]=8001 [orchestrator]=8002 [ledger]=8003
  [profile]=8004 [notification]=8005 [mock-acquirer]=8006 [mock-fraud]=8007
)
for svc in "${!SERVICES[@]}"; do
  port=${SERVICES[$svc]}
  code=$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:${port}/healthz")
  echo "  $svc (:$port): HTTP $code"
  [[ "$code" == "200" ]] || fail "$svc /healthz != 200 (got $code)"
done
# /metrics reachable on at least one service
curl -fsS http://localhost:8000/metrics | grep -q "http_request" || fail "gateway /metrics"

# 2. Kafka topics (4 + 3 DLQ)
# Note: `bash -lc` avoids MSYS (Git for Windows) mangling the leading-/ path.
RAW_TOPICS=$(docker compose exec -T kafka bash -lc '/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list' 2>/dev/null)
# Normalize to a single space-separated list (strip newlines/CR).
TOPICS=$(echo "$RAW_TOPICS" | tr -d '\r' | tr '\n' ' ')
have_topic() {
  local needle="$1" hay
  for hay in $TOPICS; do
    [[ "$hay" == "$needle" ]] && return 0
  done
  return 1
}
for t in payments.commands payments.events ledger.commands ledger.events \
         payments.commands.dlq ledger.commands.dlq payments.events.dlq; do
  have_topic "$t" || fail "kafka topic missing: $t"
done
echo "  kafka: 4 topics + 3 DLQ present"

# 3. PG: 4 bases + wal_level
# Note: connect to an existing DB (ledger); listing pg_database is fine.
N=$(docker compose exec -T postgres psql -U fintech -d ledger -tAc \
  "SELECT count(*) FROM pg_database WHERE datname IN ('ledger','orchestrator','intake','profile')")
[[ "$N" == "4" ]] || fail "PG bases found $N (need 4)"
WAL=$(docker compose exec -T postgres psql -U fintech -d ledger -tAc "SHOW wal_level")
[[ "$WAL" == "logical" ]] || fail "wal_level=$WAL (need logical)"
echo "  postgres: 4 bases + wal_level=logical"

# 4. Redis
[[ "$(docker compose exec -T redis redis-cli ping)" == "PONG" ]] || fail "redis ping"
echo "  redis: PONG"

# 5. Debezium connector RUNNING
STATE=$(curl -fsS http://localhost:8083/connectors/ledger-outbox/status 2>/dev/null \
  | grep -oE '"connector"[^{]*\{[^}]*\}' | grep -oE '"state":"[A-Z]+"' | head -1 | cut -d'"' -f4)
[[ "$STATE" == "RUNNING" ]] || fail "debezium ledger-outbox state=${STATE:-MISSING} (need RUNNING)"
echo "  debezium: ledger-outbox RUNNING"

# 6. Prometheus targets UP (prometheus reports "health":"up")
UP=$(curl -fsS http://localhost:9090/api/v1/targets | grep -oiE '"health":"up"' | wc -l | tr -d ' ')
[[ "$UP" -ge 8 ]] || fail "prometheus UP targets=$UP (need >=8)"
echo "  prometheus: $UP targets up"

echo "SMOKE OK — all phase-1 checks passed."
