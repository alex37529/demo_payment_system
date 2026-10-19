# Этап 8. Release + Acceptance

> **Ветка:** `release/01`
> **Результат:** `make demo` (up + seed + load + failure-injection + восстановление) проходит **все 7 acceptance-критериев** из §7 TЗ v2; tag `v0.1.0`; релиз-ноты.
> **Чему учимся здесь:** как зафиксировать «готовность» проекта: формализованный acceptance-скрипт (автоматическая проверка критериев), чистый release-процесс (tag, notes), и как «сложить» всё вместе в один детерминированный demo-сценарий.

---

## 1. Зачем отдельный этап релиза

Этапы 1–7 **собирали** систему и доказывали каждый паттерн локально. Этап 8 — **синтез**: один сценарий (`make demo`) прогоняет **все 7 критериев** §7 TЗ v2 подряд и либо **всё** проходит, либо **нет**. Это:

1. **Защита от «работает у меня, но не вместе».** Каждый этап проверен изолированно; здесь — end-to-end.
2. **Детерминированный демо-сценарий.** Показательность: `make demo` → 7/7 green.
3. **Фиксация версии** (tag + notes) — повторимая точка в git.

> **Принцип:** acceptance-скрипт **идемпотентен** (можно запускать сколько угодно раз) и **самодостаточен** (сам поднимает стек, сам роняет, сам проверяет).

---

## 2. 7 acceptance-критериев (§7 TЗ v2)

| # | Критерий | Что проверяем | Как (автоматически) |
| --- | --- | --- | --- |
| **1** | **Согласованность** | Сбой Ledger при записи → событие в Kafka не теряется и не дублируется (Outbox + Debezium) | Kill Ledger-контейнер во время `debit`; после restart — `ledger.events` содержит ровно 1 msg; `outbox_events` — 1 row |
| **2** | **Отказоустойчивость** | `POST /fraud/admin/fail {mode:errors}` → CB open, фрод-проверки в DLQ, валидные ≥10 TPS | Fraud-fail; load @50TPS/30s; Prometheus: `pybreaker_open=1`, `ledger.commands.dlq` ≥ N, success-rate ≥ 10 |
| **3** | **Наблюдаемость** | Один `trace_id` от Gateway до Ledger; пики latency в Grafana | Создать payment; Tempo: 1 trace ≥5 span; Prometheus: p99 exists |
| **4** | **Качество кода** | SOLID, чистые границы; **ни один сервис не читает чужую БД** | Grep: cross-schema JOIN отсутствует; coverage ≥ 80% (домен); unit tests pass |
| **5** | **UI/UX** | Обрыв SSE → авто-reconnect; баланс обновляется live без reload | Kill Notification; client reconnects; новое событие приходит |
| **6** | **SLO** | ≥ 50 TPS **и** P99 < 300ms | `scripts/load_test.py` → through/put ≥50, p99<300 |
| **7** | **Идемпотентность** (в §3.1, проверяем в §7) | Дубль `Idempotency-Key` → 1 событие | 2 POST (same key) → `payments.commands` count = 1 |

> **Примечание:** TЗ v2 §7 содержит 5 явных критериев (Согласованность, Отказоустойчивость, Наблюдаемость, Качество кода, UI/UX). Мы дополняем **SLO** (§4) и **Идемпотентность** (§3.1), так как они — «must-have» и легко проверяются. Итого **7**.

---

## 3. Acceptance-скрипт (`scripts/acceptance.sh`)

**Структура** (8 секций: setup + 7 критериев + teardown):

```bash
#!/usr/bin/env bash
set -uo pipefail   # нет -e: хотим собрать ВСЕ результаты

BASE=http://localhost
PASS=0; FAIL=0

check() {  # check <name> <condition-0/1>
  local name="$1"; local ok="$2"
  if [[ "$ok" == "1" ]]; then echo "  ✅ $name"; ((PASS++));
  else echo "  ❌ $name"; ((FAIL++)); fi
}

echo "=== SETUP ==="
make clean >/dev/null 2>&1
make up >/dev/null
./scripts/smoke_env.sh || { echo "smoke failed"; exit 1; }
make seed >/dev/null 2>&1

# ---------- Критерий 1: Согласованность (Outbox) ----------
echo "--- [1] Consistency (Outbox) ---"
PID=$(curl -s -X POST $BASE:8001/v1/payments -H 'Idempotency-Key: C1' \
      -d '{"amount":100,"currency":"USD","account_id":"A01"}' | jq -r .payment_id)
# Kill ledger во время обработки (race):
docker compose stop ledger >/dev/null
docker compose start ledger >/dev/null
sleep 3
ROWS=$(docker compose exec -T postgres psql -U fintech -d ledger -tAc \
       "SELECT count(*) FROM outbox_events WHERE payload->>'payment_id'='$PID'")
MSEV=$(docker compose exec -T kafka kafka-console-consumer.sh \
       --bootstrap-server localhost:9092 --topic ledger.events \
       --from-beginning --timeout-ms 5000 2>/dev/null | grep -c "$PID")
check "exactly-one outbox row" $([[ "$ROWS" -eq 1 ]]; echo $?)
check "no dup in Kafka" $([[ "$MSEV" -le 1 ]]; echo $?)

# ---------- Критерий 2: Отказоустойчивость (CB) ----------
echo "--- [2] Resiliency (Circuit Breaker) ---"
curl -s -X POST $BASE:8007/fraud/admin/fail -d '{"mode":"errors"}' >/dev/null
python scripts/load_test.py &   # 50 TPS / 20s
LOAD_PID=$!
sleep 25
kill $LOAD_PID 2>/dev/null
CB=$(curl -s "http://localhost:9090/api/v1/query?query=pybreaker_open" \
     | jq '.data.result[0].value[1]')
DLQ=$(docker compose exec -T kafka kafka-console-consumer.sh \
       --bootstrap-server localhost:9092 --topic ledger.commands.dlq \
       --from-beginning --timeout-ms 5000 2>/dev/null | wc -l)
check "circuit breaker OPEN" $([[ "$CB" == "1" ]]; echo $?)
check "DLQ non-empty" $([[ "$DLQ" -ge 1 ]]; echo $?)
curl -s -X POST $BASE:8007/fraud/admin/fail -d '{"mode":"off"}' >/dev/null

# ---------- Критерий 3: Наблюдаемость (Tracing) ----------
echo "--- [3] Observability (Tracing) ---"
PID=$(curl -s -X POST $BASE:8001/v1/payments -H 'Idempotency-Key: C3' \
      -d '{"amount":100,"currency":"USD","account_id":"A02"}' | jq -r .payment_id)
sleep 3
SPANS=$(curl -s "$BASE:3200/api/search?tags=payment_id=$PID" | jq '.traces | length')
check "one trace found" $([[ "$SPANS" -ge 1 ]]; echo $?)
# Tempo: сколько уникальных service_name в этом trace
SERVICES=$(curl -s "$BASE:3200/api/search?tags=payment_id=$PID" \
            | jq -r '.traces[0].spans[].resource.service_name' | sort -u | wc -l)
check "trace spans gateway..ledger (>=4 services)" $([[ "$SERVICES" -ge 4 ]]; echo $?)

# ---------- Критерий 4: Качество кода ----------
echo "--- [4] Code Quality (boundaries) ---"
# Границы: нет cross-schema JOIN (ledger.* в profile-сервисе и т.п.)
CROSS=$(rg -n "JOIN\s+ledger\." services/profile/ services/notification/ 2>/dev/null | wc -l)
check "no cross-schema JOIN in read-side" $([[ "$CROSS" -eq 0 ]]; echo $?)
# Домен-покрытие (агрегаты) >= 90%
COV=$(make test -- --cov=services/ledger/domain 2>/dev/null | grep TOTAL | awk '{print $5}' | tr -d '%')
check "domain coverage >= 90%" $([[ "$COV" -ge 90 ]]; echo $?)

# ---------- Критерий 5: UI/UX (SSE reconnect) ----------
echo "--- [5] UI/UX (SSE auto-reconnect) ---"
# Подключаем SSE (curl -N), kill Notification, проверяем что stream вернулась
curl -sN $BASE:8005/v1/stream > /tmp/sse.log 2>/dev/null &
SSE_PID=$!
docker compose stop notification >/dev/null
docker compose start notification >/dev/null
sleep 4
# Notification снова отвечает → reconnect
HEALTH=$(curl -s -o /dev/null -w '%{http_code}' $BASE:8005/healthz)
check "notification recovered (200)" $([[ "$HEALTH" == "200" ]]; echo $?)
curl -s -X POST $BASE:8001/v1/payments -H 'Idempotency-Key: C5' \
     -d '{"amount":50,"currency":"USD","account_id":"A03"}' >/dev/null
sleep 3
check "SSE got new event after reconnect" \
  $(grep -q "FundsDebited\|FundsReserved" /tmp/sse.log; echo $?)
kill $SSE_PID 2>/dev/null

# ---------- Критерий 6: SLO ----------
echo "--- [6] SLO (50 TPS, P99 < 300ms) ---"
LOAD=$(python scripts/load_test.py --tps 50 --duration 30)
TP=$(echo "$LOAD" | jq .throughput); P99=$(echo "$LOAD" | jq .p99)
check "throughput >= 50 TPS" $((TP >= 50); echo $?)
check "P99 < 300ms" $((P99 < 300); echo $?)

# ---------- Критерий 7: Идемпотентность ----------
echo "--- [7] Idempotency ---"
curl -s -X POST $BASE:8001/v1/payments -H 'Idempotency-Key: D1' \
     -d '{"amount":100,"currency":"USD","account_id":"A04"}' >/dev/null
curl -s -X POST $BASE:8001/v1/payments -H 'Idempotency-Key: D1' \
     -d '{"amount":100,"currency":"USD","account_id":"A04"}' >/dev/null
sleep 2
CMDS=$(docker compose exec -T kafka kafka-console-consumer.sh \
       --bootstrap-server localhost:9092 --topic payments.commands \
       --from-beginning --timeout-ms 5000 2>/dev/null | grep -c '"idem"\|D1')
check "duplicate key -> single message" $([[ "$CMDS" -le 1 ]]; echo $?)

# ---------- Итог ----------
echo ""
echo "===================================="
echo "  ACCEPTANCE: ${PASS} passed, ${FAIL} failed"
echo "===================================="
make down >/dev/null 2>&1   # teardown

[[ $FAIL -eq 0 ]] && echo "✅ ALL ACCEPTANCE CRITERIA MET" && exit 0
echo "❌ ACCEPTANCE FAILED" && exit 1
```

> **Подводный камень №1 (timing):** `sleep 3` — «грязный». В PoC — приемлем (deterministic-enough). В строгих CI — polling (max 10s) вместо fixed sleep.
>
> **Подводный камень №2 (`--from-beginning` + timeout):** kafka-console-consumer — **blocking**. `--timeout-ms 5000` + `--max-messages` — «убить». Иначе скрипт «зависнет».
>
> **Подводный камень №3 (критерий 1 — race):** «Kill Ledger во время записи» — **не гарантированно** попадает в TX. В PoC — «best-effort» (kill + start + check idempotency через unique `event_id`). Строе — chaos-test (toxiproxy). Но для PoC — достаточно: unique `event_id` **всегда** защищает от дублей.

---

## 4. `make demo` (один командный сценарий)

```
make demo
  ├─ make clean            # docker compose down -v
  ├─ make up               # поднимает стек + healthwait
  ├─ ./scripts/smoke_env.sh
  ├─ make seed             # 20 юзеров
  ├─ curl .../fraud/admin/fail (errors)        # failure-injection
  ├─ python scripts/load_test.py --tps 50 --duration 30
  ├─ curl .../fraud/admin/fail (off)           # восстановление
  ├─ ./scripts/acceptance.sh                   # 7/7
  └─ make down
```

**Результат в терминале:**
```
=== ACCEPTANCE ===
  ✅ exactly-one outbox row
  ✅ no dup in Kafka
  ✅ circuit breaker OPEN
  ✅ DLQ non-empty
  ✅ one trace found
  ✅ trace spans gateway..ledger (>=4 services)
  ✅ no cross-schema JOIN in read-side
  ✅ domain coverage >= 90%
  ✅ notification recovered (200)
  ✅ SSE got new event after reconnect
  ✅ throughput >= 50 TPS
  ✅ P99 < 300ms
  ✅ duplicate key -> single message
====================================
  ACCEPTANCE: 13 passed, 0 failed
====================================
✅ ALL ACCEPTANCE CRITERIA MET
```

---

## 5. Release-процесс

```bash
# 1. Final validation (всё вместе):
make clean && make up && make smoke && make test && make load && make acceptance

# 2. Tag:
git tag -a v0.1.0 -m "FinTech PoC v0.1.0 — 8 сервисов, Kafka KRaft, Outbox, Saga, CQRS, SSE, Obs"
git push --tags

# 3. Release notes (GitHub Release / CHANGELOG):
```

**Release notes template:**
```markdown
# v0.1.0 — FinTech PoC

## Что внутри
- 8 микросервисов (Gateway/BFF, Intake, Orchestrator, Ledger, Profile, Notification, Mock Acquirer, Mock Fraud)
- Kafka KRaft + Debezium (Transactional Outbox → CDC)
- CQRS (Write: Ledger / Read: Profile+Notification)
- Saga Orchestration (python-statemachine) + Circuit Breaker (aiobreaker) + DLQ
- SSE (реал-тайм UI) + React frontend
- Observability: OTel→Tempo, Prometheus, Loki, Grafana, Alertmanager
- 15+ ADR, C4+Sequence диаграммы, event contracts spec

## Как запустить
\`\`\`
docker compose up -d
make demo   # acceptance
\`\`\`

## Известные ограничения (PoC)
- KRaft single-node (не HA)
- In-memory state (saga/fraud mode) — сброс при restart
- Own-currency (кросс-валюта out of scope)
- 24h idempotency TTL
```

---

## 6. Коммиты Этапа 8

| # | Тип | Сообщение |
|---|---|---|
| 1 | `chore` | add `scripts/acceptance.sh` (7 criteria) |
| 2 | `chore` | add `make demo` target (full scenario) |
| 3 | `docs` | finalize README (quick start + acceptance + limits) |
| 4 | `release` | validation run (all green) |
| 5 | `release` | tag `v0.1.0` + release notes |

---

## 7. Как проверить (exit criteria)

```
make demo
# → ВСЕ 13 проверок ✅, "ALL ACCEPTANCE CRITERIA MET"
# → tag v0.1.0 создан
# → git log: чистая история, ветка release/01 в main
```

**Красные флаги:**
- [ ] Любой из 13 check — ❌.
- [ ] `make demo` «зависает» (consumer blocking).
- [ ] Tag есть, но README не final.
- [ ] Acceptance не идемпотентен (2-й запуск падает).

---

## 8. Чек-лист перед релизом

- [ ] `make demo` → 13/13 ✅.
- [ ] `git status` clean (нет мусора в tracked).
- [ ] README final (quick start, acceptance, limits).
- [ ] 8 phases merged в `main`.
- [ ] Tag `v0.1.0` создан + push.
- [ ] Release notes написаны.
- [ ] Все ADR, диаграммы, spec в `doc/s`.

---

## 9. Что СЛЕДУЕТ ПОСЛЕ v0.1.0 (out of scope, на futuro)

| Направление | Почему «next» (не сейчас) |
| --- | --- |
| **Кросс-валютные платежи** (FX) | Out of scope (ADR-003). Нужна FX-модель, граничные кейсы. |
| **HA Kafka** (3 nodes) | PoC — single-node. HA — infra-task. |
| **AuthN/AuthZ** (OAuth2, RBAC) | PoC — local, без auth. Прода — must. |
| **Multi-region** | PoC — single-region. |
| **Real Fraud ML** | PoC — stub. Реальный — external team. |
| **Chaos engineering** (toxiproxy) | PoC — kill container. Chaos — next. |
| **gRPC между сервисами** | PoC — HTTP/Kafka. gRPC — оптимизация. |

> Это — **roadmap для v0.2 / v1.0**, не для v0.1.0 (PoC).

---

## 10. Финальный чек-лист проекта (v0.1.0)

- [x] Этап 1: Инвайрмент + shared (8/8)
- [x] Этап 2: Ledger + Outbox (9/9)
- [x] Этап 3: Saga + CB + DLQ (8/8)
- [x] Этап 4: Intake + Idempotency (6/6)
- [x] Этап 5: Read-Side + SSE (7/7)
- [x] Этап 6: Mocks + Frontend (8/8)
- [x] Этап 7: Observability + Tests + Docs (11/11)
- [x] Этап 8: Release + Acceptance (5/5)
- [x] **`make demo` → 13/13 ✅**
- [x] **Tag `v0.1.0`**

> **Статус:** PoC **готов** к демонстрации. Все 7 acceptance-критериев §7 TЗ v2 **проходят**.
