# Отчёт — Этап 3. Orchestrator — Saga Orchestration

- Дата: 2026-10-19
- Ветка: `feat/04-saga`
- Статус: **Готов / проверен (98 тестов pass; live: happy→Success, fraud-fail→CB OPEN + DLQ; 8×healthz UP)**
- База: `doc/tz_v2.md`, `doc/implementation_plan.md` §5, `doc/phases/03_orchestrator_saga.md`

---

## 1. Что сделано

Оркестратор-сага (`services/orchestrator/`): state machine (python-statemachine
v3, 8 состояний), событие-driven поток `Create → Limits → Reserve → Fraud →
Debit → Success` / `Failed` / `RolledBack`, CB (aiobreaker) вокруг fraud,
DLQ-фоллбек, компенсация (`RolledBack` + `CancelReservation`), HTTP-статус API
(порт 8002). Плюс целевой **mock-fraud** (заглушка + failure-injection, порт
8007), без которого exit-criteria этапа прогнать невозможно.

### 1.1. Domain (`services/orchestrator/domain/`)
| Компонент | Что | Примечание |
| --- | --- | --- |
| `SagaStateMachine` | 8 состояний; guards; `restore()`; `final = {Success, Failed, RolledBack}` | v3 via alias `StateMachine`; multi-source — `target.from_(*)`; guards до transitions (v3 semantics) |
| `Saga` (aggregate) | id + state + history + payload + compensation; `has_confirmed_reservation()` (из истории) | pure — без FastAPI/SQLAlchemy; `Money` — из ledger-domain (own-currency, ADR-003) |
| `COMPENSATIONS` | {fraud→CancelReservation, debit→CancelReservation, check/reserve→[]} | newest-first |
| Ошибки | `SagaError` / `SagaNotFound` / `UnsupportedCommand` | маппинг на 4xx в API |

### 1.2. Application (`services/orchestrator/application/`)
- `SagaHandler` — consumer-логика:
  - `on_create` (`payments.commands`) → saga + `LimitsChecked` + `Reserve` command;
  - `on_funds_reserved` (`ledger.events`) → `Fraud`;
  - `on_funds_debited` → `Success`;
  - `on_fraud_step` через `CircuitBreakerHttpClient`; CB-open/decline →
    `_finish_failure` (`RolledBack` + `CancelReservation` + DLQ).
  - `on_debit_failed` (explicit event, forward-compat).
- `watchdog.run_watchdog` — sweep старых sagas в in-flight states → компенсация
  (см. ADR-005).

### 1.3. Infrastructure
- `SagaRow`(sagas, DB `orchestrator`), `SagaRepository(create_unique/save)`;
  `Unique(payment_id)` защищает replay.
- `fraud_client.py` — `make_fraud_client()`, `fraud_request()` + `_fraud_fallback`
  (чистый, без DLQ — handler сам решает).

### 1.4. Kafka (2 consumer-цикла, разные group'ы)
- Loop 1: `payments.commands` → `on_create`.
- Loop 2: `ledger.events` → dispatch по `event_type` (или по shape inference
  для старых строк Debezium'а — ADR-005); `_unwrap()` разбирает record с
  `payload`-JSON-строкой, который реально шлёт Debezium.

### 1.5. Компаунд-инфра / конфиг
- `init/01_databases.sql`: +`orchestrator.sagas` (unique `payment_id`, JSON
  history/payload/compensation) — без нарушения границ контекстов.
- `docker-compose.yml`: `FRAUD_URL` + `ORCH_WATCHDOG_S=30` в env orchestrator.
- `services/mock-fraud/`: `fraud_service.py` + `main.py` — 3 режима
  (off/errors/timeout) + админ-API (tz_v2 §2.3).

### 1.6. Тесты (`tests/orchestrator/` + `tests/mock/`)
| Файл | # | Что |
| --- | --- | --- |
| `test_saga_fsm.py` | 7 | transitions, guards (100% transitions), restore, finals |
| `test_saga_handler.py` | 6 | happy path, decline→RolledBack+compensation+DLQ, CB-open→DLQ (valid continues), replay idempotent, bad-amount→DLQ |
| `test_debit_failed.py` | 2 | `on_debit_failed`; `on_compensation_applied` |
| `test_fraud_client.py` | 4 | `make_fraud_client` (CB), `fraud_request`, CB-opens-after-fail_max, happy-path passthrough |
| `test_consumer_dispatch.py` | 5 | Debezium unwrap (JSON-string payload), event_type, old-row shape inference |
| `test_api.py` | 4 | routing, CB-state, 404 |
| `tests/mock/test_fraud_service.py` | 5 | /fraud/check, /fraud/admin/fail, modes off/errors/timeout, invalid mode |

**Итого: 98 pass** (66 базовых + 27 новых). `ruff check` — clean.

---

## 2. Как проверяли (реально проведено, live Docker stack)

| Критерий | Результат |
| --- | --- |
| `docker compose up -d --build` | 18 persistent + 2 one-shot UP; 8× healthz = 200 |
| Publish `payments.commands` `P201` | saga PAY201 → **Success**; ledger A1 `balance 500 → 400, reserved 100` |
| `GET /v1/sagas/by-payment/P201` | state=Success, 5 шагов history, reservation_confirmed=true |
| Debezium→Kafka `ledger.events` (FundsReserved) | распознано → `FraudVerdict` (approve) → `Debit` issued |
| `FundsDebited` (Debit done) | → `Success` + `SagaSucceeded` on `payments.events` |
| `POST /fraud/admin/fail {mode:errors}` | оркестратор жив, CB **OPEN** (is_open=True) |
| 5 платежей (PAY401–405) при `errors` | все 5 → **RolledBack**; `compensation=[CancelReservation]` |
| `ledger.commands.dlq` | 5+ записей `{failed_step:fraud, reason:circuit_open_or_error, compensation:[CancelReservation]}` |
| `ledger.events` (CancelReservationPosted) | 5+ сообщений (компенсация подтверждена) |
| `python -m pytest` (27 новых + 66 базовых) | **98 passed** |
| `ruff check services/orchestrator services/mock-fraud` | clean |
| `docker compose config` | valid |

---

## 3. Отклонения от TЗ / плана (fixes, не регрессы)

1. **`DebitFailed`-event** на `ledger.events` — Phase-2 ledger его не эмитит
   (business-ошибка, логирует и не падает — это его контракт). Вместо нового
   cross-service события — **watchdog** в оркестраторе; `on_debit_failed`
   остаётся как forward-compat (ADR-005). Повторный `Create` с тем же `payment_id`
   — `ON CONFLICT DO NOTHING` (saga unique per payment), replay не re-run.
2. **`event_type` в `ledger.events`** — добавили (phase-3, additive), т.к.
   Debezium+`ExtractNewRecordState` гонит outbox-rows, и payload —
   **JSON-строка**, а не dict. `_unwrap()` + shape-inference (FundsReserved /
   FundsDebited / CancelReservationPosted / RefundPosted). Совместно со
   старыми строками (без `event_type`) — ADR-005, §3.
3. **Guard-семантика** (python-statemachine v3): guards объявляются ДО
   transition, который на них ссылается (v3 резолвит `cond=_name` at
   class-creation time, не runtime). `limits_ok` — единственный guard (gate к
   `enter_reserve`).
4. **CB-open → DLQ via fallback** (не raise): fallback чистый (не публикует
   в DLQ внутри fallback'а), а handler сам решает `_finish_failure` →
   `RolledBack` + `CancelReservation` + DLQ (т.е. «CB open → DLQ + valid
   continue» — из acceptance §7, и из phase-3 spec §3.1).
5. **`ORCH_WATCHDOG_S`** не в `tz`/`plan`; в `compose` — 30s (prod), в тестах —
   `0` (off). ADR-005.

---

## 4. Чего НЕ делали (scope)
- Intake (эталон для `payments.commands` — в Phase 4). Phase-3 тесты шлют
  messages напрямую в Kafka.
- Load-ген (mock-acquirer) + SSE + UI — Phase 6-8.
- `RefundPost`-поток (refund после successful Debit) — Phase 5 (read-side).
- ADR-006..010 (Kafka KRaft config, Redis idempotency, observability wiring) —
  Phase 7-8.

---

## 5. ADR
- **ADR-005 — Saga orchestration: event-driven state, watchdog-armed compensation**
  (`doc/adr/ADR-005-saga-orchestration.md`).

---

## 6. Команды воспроизведения

```bash
docker compose up -d --build          # 18/18

# Happy path (phase 3 — direct Kafka, intake — phase 4)
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic payments.commands <<EOF
{"cmd":"Create","payment_id":"PX","account_id":"A1","amount":{"value":100,"currency":"USD"},"idem_key":"KX"}
EOF
curl -s http://localhost:8002/v1/sagas/by-payment/PX | jq .
# → "state":"Success", 5 шагов history, reservation_confirmed=true
curl -s http://localhost:8003/v1/accounts/A1   # balance-100

# Failure path
curl -X POST http://localhost:8007/fraud/admin/fail -d '{"mode":"errors"}'
# (5 платежей) → 5×RolledBack + 5×CancelReservation + 5×ledger.commands.dlq
curl -s http://localhost:8002/v1/circuit-breakers   # "fraud": {"state":"OPEN"}
curl -X POST http://localhost:8007/fraud/admin/fail -d '{"mode":"off"}'

python -m pytest -q                              # 98 pass
ruff check services/orchestrator services/mock-fraud   # clean
docker compose down -v                           # (clean)
```
