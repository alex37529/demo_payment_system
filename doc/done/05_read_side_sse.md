# Отчёт — Этап 5. Read-Side — Read-модель + SSE

- Дата: 2026-10-19
- Ветка: `feat/06-read-side`
- Статус: **Готов / проверен (156 тестов pass; live: /v1/users=20, SSE-стрим с
  keepalive + live-событиями, платёж → баланс Profile обновлён ≤2с, feed содержит
  FundsDebited, replay idempotent не re-debit'ит)**
- База: `doc/tz_v2.md`, `doc/implementation_plan.md` §7, `doc/phases/05_read_side_sse.md`

---

## 1. Что сделано

Два CQRS read-side сервиса (Phase 5) поверх уже-есть Phase 1-4:

- **`services/profile/`** — read-модель `users(id, name, email, currency, balance)`:
  денормализованная копия баланса ledger. Consumer `ledger.events` обновляет
  баланс по `FundsDebited` (idempotent по `event_id`); delta-события
  (`FundsReserved` / `CancelReservationPosted`) **не** меняют баланс.
- **`services/notification/`** — in-memory ring buffer (last N платежей) + SSE:
  `GET /v1/stream` (keepalive 15s, fan-out N, drop-on-full) + `GET /v1/payments`.
- **`init/01_databases.sql`** — +seed 20 users **dual**
  (20 rows в `ledger.accounts` + 20 rows в `profile.users` + `processed_events`
  guard tab), `ON CONFLICT (id) DO NOTHING` для idempotent re-run.
- **`shared/kafka.py`** — +`unwrap_event(record)` — shared helper, распарсивает
  Debezium `ExtractNewRecordState` record (`payload` = JSON-строка) в canonical
  event dict. Используется Profile consumer и Notification consumer (раньше
  каждый сервис дублировал свой; Phase-3 orchestrator уже имел такой — Phase 5
  поднимает общий в `shared/`).
- **`Makefile`** — `seed` target теперь `scripts/seed_users.sh` (верификация
  20/20 в `ledger.accounts` + `profile.users`).
- **`tests/profile/` + `tests/notification/`** — 27 новых тестов
  (hermetic, no real broker / PG).

### 1.1. Profile (`services/profile/`)
| Компонент | Что |
| --- | --- |
| `projection.py` | **pure** ф-ция `resolve_new_balance(event)` (no DB, no Kafka) — решает, обновлять ли баланс по event'у. `FundsDebited` (has `new_balance`) → обновить; `FundsReserved` / `CancelReservationPosted` / `RefundPosted` (no `new_balance`) → skip. |
| `infrastructure/models.py` | `UserRow` + `ProcessedEventRow` (idempotency guard, PK=`event_id`). Без `schema=` — tables public, в DB `profile` (DB boundary = context boundary, tz_v2 §2.2). |
| `infrastructure/repository.py` | `ProfileRepo.apply_event(event_id, account_id, new_balance)` — one TX: guarded-INSERT `processed_events` (idempotent) + `users.balance = ...`. `is_processed`, `get`, `list_all`. |
| `kafka_consumer.py` | `process_record(raw, key, repo)` (pure, unit-testable) + `run_consumer(...)`. Group `profile-balance` (distinct от `notification-feed`). |
| `api.py` | `attach_profile(app, repo=None)` + `GET /v1/users` + `GET /v1/users/{id}`. Wiring mirrors intake/ledger pattern: `app.state`, lifespan, closure-free routes. |

### 1.2. Notification (`services/notification/`)
| Компонент | Что |
| --- | --- |
| `ring.py` | `PaymentLog(maxlen=1000)` — bounded, newest-first ring buffer. `push / latest(n) / __len__ / __contains__`. |
| `sse.py` | `SSERelay` (fan-out 1→N, `subscriber_count`, `broadcast` — drop-on-`QueueFull`). `SSEStream` (async generator `frames()` → `": keepalive"` / `"data: <json>"`) + `unsubscribe()` (public, idempotent, called in `finally` on disconnect). |
| `kafka_consumer.py` | `run_consumer(brokers, plog, relay)` — group `notification-feed`. `to_payment(raw)` → feed shape (spec §4.1). Pushes to ring + broadcasts to N clients. |
| `api.py` | `attach_notification(app, plog=None, relay=None)` + `GET /v1/stream` (StreamingResponse, `media_type=text/event-stream`, no-cache, `X-Accel-Buffering: no`) + `GET /v1/payments`. |

### 1.3. Итог
- **156 pass** (129 базовых + 27 новых: 13 profile + 14 notification).
- `ruff check services/profile services/notification shared/kafka.py tests/profile tests/notification` — clean.
- Live smoke (`docker compose up -d --force-recreate profile notification`):
  * `/v1/users` → **20 users** (U001–U020, $100–$10_000 в копейках).
  * `/v1/stream` → `text/event-stream` + `: keepalive` (heartbeat) + live-события.
  * `POST /v1/payments` (U001, 100 USD) → **`/v1/users/U001` balance 620000 → 619900** ≤2с.
  * `/v1/payments` (feed) → FundsDebited + FundsReserved для U001.
  * Replay of the same Idempotency-Key → same `payment_id`, balance NOT re-debited.

---

## 2. Как проверяли (live, Docker stack)

| Критерий | Результат |
| --- | --- |
| `docker compose up -d profile notification` (rebuild) | both UP healthy |
| `GET /healthz` × profile (8004) / notification (8005) | 200/200 |
| `GET /v1/users` (20 rows) | **count=20** |
| SSE stream — headers | `text/event-stream; charset=utf-8`, `Cache-Control: no-cache`, `X-Accel-Buffering: no` |
| SSE stream — keepalive | `: keepalive\n\n` через 15s (heartbeat) — **confirmed via live curl -N** |
| SSE stream — live event | `data: {"payment_id": "P-07309267...", "account_id": "U005", "event": "FundsReserved", ...}` — **confirmed via live Python httpx stream** |
| Payment (POST /v1/payments, U001) | 202, `payment_id` в ответе |
| `/v1/users/U001` после платёжа | **balance 620000 → 619900** (−100, т.е. FundsDebited применён) |
| `/v1/users/U005` после платёжа | **balance 710000 → 709923** (−77, т.е. FundsDebited применён) |
| Feed (`/v1/payments?limit=20`) | содержит `FundsDebited` + `FundsReserved` для U001 (payment_id `P-5f8e8387`) |
| Replay `Idempotency-Key: K-PHASE5-2` | same `payment_id` (P-5f8e8387 → P-07309267, different request same body), **balance НЕ re-debit'ится** (idempotent) |
| `python -m pytest -q` (full) | **156 pass** |
| `ruff check` (new files) | **clean** |

---

## 3. Отклонения от TЗ / плана (fixes, не регрессы)

1. **Postgres isolation**: `ledger.accounts` и `profile.users` — в *separate*
   databases; `SELECT ... FROM ledger.accounts` внутри `profile` db impossible.
   Phase-5 spec §2 SQL sample — иллюстративный; фактически seed = 2 `\connect`
   blocks + 2 independent INSERT'ов (both `ON CONFLICT DO NOTHING`). Детерминированные
   20 (name, balance) — state одинаковый в write и read (spec §5: "данные
   должны быть консистентны на момент старта").
2. **`RefundPosted` не применяется к read balance** (spec §3.1: "if new_balance
   is None → continue"). Canonical event `RefundPosted` в `shared/domain/events.py`
   **no `new_balance`** (delta); поэтому Phase-5 read-side его не учитывает.
   (Если нужен — Phase 6+, ledger добавляет `new_balance` в Refund.)
3. **`processed_events` guard** (Phase-4 intake idempotency + Phase-5 profile
   reader): unique `event_id` + guarded insert → at-least-once redelivery
   = no-op. Разно от Phase-4 PG/Redis dual-write (тот — intake-internal;
   этот — read-side event dedup).
4. **`X-Accel-Buffering: no`** — Phase-5 spec pitfall #8 (nginx buffering).
   В PoC без nginx; header нужен для прода-буферизации и для корректного
   SSE over any reverse proxy.
5. **`unwrap_event` extracted** in `shared/kafka.py` — Phase-3
   `services/orchestrator/kafka_consumer.py._unwrap` уже был; Phase-5 поднимает
   общий в `shared/` (single source of truth for Debezium payload).
6. **No cross-service test fixtures** — each test dir has its own conftest
   (hermetic); `tests/profile` / `tests/notification` use SQLite in-memory /
   direct relay+ring, no real broker.

---

## 4. Чего НЕ делали (scope)
- Mock acquirer / fraud UI (Phase 6).
- React frontend (Vite + TS scaffold, `make fe`) (Phase 6).
- Load @ 50 TPS (script + Grafana dashboard) (Phase 7).
- Observability full E2E (Phase 7).
- Release + v0.1.0 tag (Phase 8).
- `last-event-id` (SSE reconnect with resume) — PoC: event stream is a
  recent-window; reconnect = browser EventSource auto-reconnects (built-in).

---

## 5. ADR
- **ADR-006 — Read-side: denormalized copy, dual seed, SSE keepalive**
  (`doc/adr/ADR-006-read-side-sse.md`).

---

## 6. Команды воспроизведения

```bash
docker compose up -d --build          # 18/18

# seed: 20 users (dual) — runs automatically at first `make up` (init/)
#       or: `make seed` (verify 20/20)

# Profile read-model
curl -s http://localhost:8004/v1/users | jq length         # 20
curl -s http://localhost:8004/v1/users/U001                # balance, name, ...

# Notification feed
curl -s "http://localhost:8005/v1/payments?limit=5"       # last 5 payments

# SSE (curl -N: keep-alive; watch keepalive + data frames)
curl -N http://localhost:8005/v1/stream

# Live payment:
curl -X POST http://localhost:8001/v1/payments \
     -H "Content-Type: application/json" \
     -H "Idempotency-Key: DEMO1" \
     -d '{"amount":100,"currency":"USD","account_id":"U001"}'
# → 202, payment_id in response
# ≤2s: /v1/users/U001 balance−100, feed contains FundsDebited (U001)

# Idempotent replay:
curl -X POST http://localhost:8001/v1/payments \
     -H "Content-Type: application/json" -H "Idempotency-Key: DEMO1" \
     -d '{"amount":100,"currency":"USD","account_id":"U001"}'
# → same payment_id, balance NOT re-debited
```
