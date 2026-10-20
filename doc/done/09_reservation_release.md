# Отчёт — Фаза 9. Reservation release (денежный баг) + сигнал отказа

- Дата: 2026-10-20
- Ветка: `feat/09-reservation-release`
- Статус: **Готов / проверен live** (`reserved` возвращается к 0; 0 зависших саг;
  успешных платежей 99.6 % вместо ~26 %; load 50.02 TPS, p99 60.9 мс)
- База: `doc/adr/ADR-010-reservation-release.md`, `doc/done/07_observability_tests.md` §6

---

## 1. Что было сломано

Два дефекта, найденных E2E-тестом фазы 7 (не E2E-тестом этой фазы — баг был
до фазы 7, просто никто не проверял исход саг).

### 1.1. Резервация не снималась на успешном пути (денежный баг)

Saga: `Reserve(X)` → (fraud) → `Debit(X)`. `cancel_reservation` выпускался
**только** как компенсация при сбое. На успешном пути hold **никто не снимал**.

```
reserved  рос 1:1 с каждым успешным платежом
available = balance - reserved  →  уменьшался навсегда
```

**Живые данные до фикса:**

| Метрика | Значение |
| --- | --- |
| `sum(reserved)` | 3 896 186 |
| `sum(balance)` | 12 709 314 |
| аккаунтов с `available ≈ 0` | 20 из 41 |
| саг, зависших в `Reserve` | **3824** |
| успешных платежей | **~26 %** |

Два последствия:
1. **Пользователь, у которого баланс ровно равен сумме платежа, не мог
   заплатить**: `Reserve(X)` оставлял `available = 0`, и `Debit(X)`
   отклонялся с `InsufficientFundsError` («exceeds available 0»).
2. Аккаунт исчерпывался: сначала переставали проходить крупные платежи,
   потом любые.

### 1.2. Отклонённая команда не порождала события

Ledger глотал бизнес-ошибку (`except LedgerError: log.warning(...)`) и уходил
дальше. Сага ждёт подтверждения шага (`FundsReserved`) — отказа она не
отличила от «reserve ещё в пути» и **висела до вотчдога** (30 с). На дашборде
это выглядело как «failed timeout», хотя причина была «команду отклонили, а
никто не сказал».

### 1.3. Бонус: подтверждение компенсации не работало никогда

Диспетчер вызывал **все** хендлеры как `method(key, payload)`, а
`on_compensation_applied` требует ещё и `event_type` → `TypeError`, который
глотался в `except Exception`. **Каждое** `CancelReservationPosted` /
`RefundPosted` падало, и сага никогда не фиксировала «компенсация применена».

Это было видно только ПОСЛЕ того, как фаза 7 починила логирование сервисов
(до этого запись лога об исключении просто терялась). Хороший пример того,
как одна неисправность маскирует другую.

---

## 2. Что сделано

**ADR-010** — «Кто снимает резерв: Debit потребляет свой hold + отказ команды
объявляется событием».

| # | Изменение | Где |
| --- | --- | --- |
| 1 | Резервы хранятся **по платежам**: `Account.holds: {payment_id: amount}`; `reserved` — производная (сумма) | `services/ledger/domain/ledger.py` |
| 2 | **`Debit` потребляет свой hold**: spendable = `balance - (reserved - own_hold)`; hold этого платежа снимается | там же |
| 3 | `Reserve` создаёт hold для своего платежа; `CancelReservation` снимает свой | там же |
| 4 | Отклонённая команда публикует **`DebitFailed{step, reason}`** через outbox, в **отдельной** транзакции | `services/ledger/application/use_cases.py` |
| 5 | `on_debit_failed` использует **шаг из события** (было зашито `"debit"`) | `services/orchestrator/application/saga_handler.py` |
| 6 | Персистентность: `accounts.holds JSONB` (+ ALTER на старте ledger'а) | `models.py`, `init/01_databases.sql` |
| 7 | Диспетчер передаёт `event_type` тому хендлеру, который его требует | `services/orchestrator/kafka_consumer.py` |
| 8 | `GET /v1/accounts/{id}` отдаёт per-payment `holds` — зависший резерв больше не спрятан за «available ≈ 0» | `services/ledger/api.py` |
| 9 | Бакетировка причины знает текст ledger'а («exceeds available» → `insufficient_funds`) | `saga_handler.py` |

---

## 3. Проверка (живая стенка, свежий volume)

`make clean && make up` → 19/19 контейнеров, сид чистый (`reserved = 0`).

### 3.1. Инвариант

| | До | После |
| --- | --- | --- |
| `reserved` после 120 c нагрузки (50 TPS) | 3 896 186 | **0** |
| `available` vs `balance` | расходились | **равны** |
| саг, зависших в `Reserve` | 3824 | **0** |
| успешных платежей | ~26 % | **99.6 %** (5339 / 5362) |
| `payments_failed_total{reason="watchdog_stalled"}` | тысячи | **0** |

### 3.2. Нагрузка

```
3001 платёж · 50.02 TPS · p50 52.6 / p95 57.6 / p99 60.9 мс · 0 ошибок
```

### 3.3. Отказ теперь виден сразу

22 отказа (реальные «exceeds available») → сага падает **сразу** (не 30 с),
попадает в DLQ (не теряется) и в метрику с реальной причиной:
`payments_failed_total{reason="insufficient_funds", step="reserve"}`.

### 3.4. Тесты

| | |
| --- | --- |
| `make test` | **226 passed**, 7 skipped (E2E — opt-in) |
| `make e2e` | **7 passed** (было 6 passed + 1 xfail — xfail стал настоящим тестом) |
| ruff | 19 ошибок — **ровно как в baseline** (все pre-existing, не мои файлы) |

---

## 4. Изменённые тесты (осознанно)

`tests/ledger/test_account_aggregate.py::test_reserve_then_debit_blocked` использовал
**один и тот же** `ref` для reserve и debit. При новой семантике это означает
«списание своего же резерва» — и это должно быть **разрешено**. Тест разделён:

- `test_reserve_then_debit_blocked` — **чужая** резервация блокирует списание
  (защита от двойного расхода жива);
- `test_debit_consumes_its_own_hold` — списание потребляет свой резерв;
- `test_debit_may_spend_its_whole_balance` — можно заплатить ровно свой баланс
  (тот самый сломанный случай);
- `test_holds_are_tracked_per_payment` — два резерва = два холда;
- `test_cancel_reservation_releases_that_payment_hold`;
- `test_reserved_survives_a_round_trip` — холды переживают перезагрузку из БД
  (reserve и debit — **разные** транзакции).

`tests/ledger/test_use_cases.py::test_debit_insufficient_rolls_back_cleanly` —
больше не «нет outbox-строки», а «ровно одна строка: **объявление отказа**»
(ADR-010 п.4). Добавлен `test_rejected_reserve_announces_the_step`.

---

## 5. Известные ограничения

| Ограничение | Почему | Что вместо |
| --- | --- | --- |
| `accounts.holds` — JSONB, не отдельная таблица | холды всегда грузятся с аккаунтом и меняются в той же транзакции | для прода — отдельная таблица + JSONB-индекс (ADR будет пересмотрен) |
| `cancel_reservation` без своего холда освобождает чужие | компенсация саги с неизвестным `payment_id` всё равно должна освободить деньги | — |
| `DebitFailed` виден в ленте notification | это честное «платёж отклонён» | profile его игнорирует (нет `new_balance` → no-op) |
| Миграция: `ALTER` на старте ledger'а | `create_all` не ALTER'ит существующие таблицы; синтаксис Postgres-специфичен (SQLite-харнесс его пропускает) | — |

---

## 6. Коммиты (2)

```
533c436 fix(ledger)      the reservation is released on the success path +
                         a rejected command is announced (ADR-010)
2d65af5 fix(observability)  [фаза 7] the failure bucket said "timeout" for
                         a stalled saga
```

> Второй коммит — из фазы 7 (ветка `feat/08-obs-tests`, PR #8); он предшествует
> этому и является его предпосылкой.
