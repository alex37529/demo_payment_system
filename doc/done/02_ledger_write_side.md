# Отчёт — Этап 2. Ledger write-side + Transactional Outbox

- Дата: 2026-10-19
- Ветка: `feat/03-ledger`
- Статус: **Готов / проверен (64 теста pass, live stack: 8×healthz, outbox +1, Kafka +1 msg)**
- База: `doc/tz_v2.md`, `doc/implementation_plan.md` §4, `doc/phases/02_ledger_write_side.md`

---

## 1. Что сделано

Write-side CQRS: агрегаты с инвариантами → use-cases (1 TX) → SQLAlchemy rows →
Transactional Outbox → Debezium → Kafka `ledger.events`. HTTP API (порт 8003)
и consumer `ledger.commands` идут в **одни** use-cases (единый источник истины).

### 1.1. Domain (`services/ledger/domain/ledger.py`)
| Компонент | Что | Примечание |
| --- | --- | --- |
| `Money` (VO) | `amount:int` (минорные единицы) + `currency`; frozen; `to_dict()` = `{"value","currency"}` | own-currency + `amount>=0`; bool запрещён (TRY004) |
| `Account` (AggregateRoot) | `balance`, `reserved`, `available`; `debit/reserve/cancel_reservation/refund` | инварианты **внутри агрегата**; idempotency-гвард `ref` |
| `Payment` (AggregateRoot) | id, amount, state (`Created→…→Success/Failed/RolledBack`) | transitions валидируются |
| Ошибки | `InsufficientFundsError` (409), `AccountNotFound` (404), `IllegalTransitionError` | маппинг на HTTP в `api.py` |

### 1.2. Application (`services/ledger/application/use_cases.py`)
- `UseCase_Debit/Reserve/CancelReservation/Refund` — **1 TX**:
  `load row → Account.load → agg.<op>(ref) → repo.save → repo.mark_applied(idem_key) → _publish_outbox → commit`.
- Сессию **владеет use-case** (pitfall #5). Ошибка → `rollback`.
- `USE_CASES`-реестр + `LedgerUseCaseRunner` — общий для HTTP и Kafka.

### 1.3. Infrastructure (`services/ledger/infrastructure/`)
- `models.py`: `AccountRow`(accounts, `balance`/`reserved` BigInteger, optimistic `version`),
  `AppliedCommandRow`(applied_commands, `idem_key` UNIQUE), `PaymentRow`.
- `repository.py`: `SQLAlchemyAccountRepository` (конкретный `IAccountRepository`);
  `mark_applied()` — атомарная idempotency-запись (`IntegrityError` → replay = no-op).

### 1.4. API + consumer
- `api.py`: `POST /v1/accounts/{id}/debit|reserve|refund|cancel-reservation`,
  `GET /v1/accounts[/{id}]`; exception-маппинг; lifespan (create_all + consumer).
- `kafka_consumer.py`: `ledger.commands` → диспетчер по `cmd`; business-error → log (не DLQ),
  idempotent.

### 1.5. Инфра / конфиг
- **8 app-сервисов добавлены в `docker-compose.yml`** (были только в phase-1-отчёте,
  в коммите отсутствовали; `make up` теперь поднимает 18 persistent + 2 one-shot).
- `init/01_databases.sql`: таблицы `accounts` (+seed `A1` USD 500), `applied_commands`, `payments`.
- `pyproject.toml`: `greenlet` (нужен async SQLAlchemy в Docker), `aiosqlite` (dev, async SQLite для тестов), pytest `asyncio_mode=auto`.
- `shared/outbox.py`: `outbox_events.id` `BigInteger→Integer` (SQLite-совместимо для async-тестов; на Postgres `BIGSERIAL`/`INTEGER` — не влияет).

### 1.6. Тесты (`tests/ledger/`)
- `test_account_aggregate.py` — 27: Money VO, debit/reserve/refund/cancel, negative (currency,
  insufficient, over-reserve), idempotent replay, event-catalog, Payment transitions (100% путей).
- `test_use_cases.py` — 8: 1-TX (accounts+outbox+guard), idempotency, 404, over-reserve,
  refund, cancel (async SQLite = реальный TX-путь).
- `test_api.py` — 8: маршруты, replay idempotency, 409/404 маппинг, reserve-holds, overdraw.
- **Итого 64 pass** (27 shared + 27 + 8 + 8).

---

## 2. Как проверяли (реально проведено)

| Критерий | Результат |
| --- | --- |
| `docker compose up --build` | 8 svc + 10 infra = 18 running; 2 one-shot exited 0 |
| 8× `/healthz` | ✓ все 200 |
| `GET /v1/accounts/A1` | `500 USD` |
| `POST /v1/accounts/A1/debit {P1,100 USD}` | `ok`, `applied=true`, `FundsDebited` |
| Баланс A1 | `500 → 400` |
| `outbox_events` | `0 → 1` |
| **Kafka `ledger.events`** | 1 msg, payload `{account_id:A1, payment_id:P1, amount:{value:100,currency:USD}, new_balance:{value:400,...}}` |
| Replay `P1` | `applied=false`, баланс 400, outbox остаётся 1 |
| `debit 99999` | `409 InsufficientFundsError` |
| `debit 50 EUR` на A1/USD | `409 currency mismatch` |
| unknown account | `404 AccountNotFound` |
| Debezium `ledger-outbox` | `RUNNING` |
| `psql SHOW wal_level` | `logical` |
| `python -m pytest` | **64 pass** |
| Prometheus | 9 UP targets |

---

## 3. Что НЕ делали (по scope)
- Saga-оркестратор/compensation — Этап 3.
- Read-side/SSE — Этап 5 (Profile/Notification пока placeholder).
- Mocks + UI — Этап 6.

---

## 4. Отклонения / notes
1. **8 сервисов отсутствовали в `docker-compose.yml`** (phase-1-отчёт их заявлял, в коммите не было).
   Восстановил по карте портов `tz_v2 §4.1` — без них `make up`/exit test не проходили.
2. `greenlet` добавлен как зависимости (async SQLAlchemy обязан его иметь в рантайме).
3. `aiobreaker` без `async with` — `cb.call_async(...)` (ADR-004, не менялось).

---

## 5. ADR
- **ADR-003 — Own-Currency Invariant** (`doc/adr/ADR-003-own-currency.md`).

---

## 6. Команды воспроизведения
```bash
docker compose up -d --build            # 18/18
curl http://localhost:8003/v1/accounts/A1
curl -X POST http://localhost:8003/v1/accounts/A1/debit \
     -H 'Content-Type: application/json' -d '{"payment_id":"P1","amount":{"value":100,"currency":"USD"}}'
# БД: balance 400, outbox_events +1; Kafka ledger.events +1 (FundsDebited)
python -m pytest -q                       # 64 pass
docker compose down -v
```
