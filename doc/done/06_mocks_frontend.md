# Отчёт — Этап 6. Mocks (acquirer + fraud) + Frontend + Gateway BFF

- Дата: 2026-10-19
- Ветка: `feat/07-mocks-fe`
- Статус: **Готов / проверен live** (`192` testov pass; `18/18` сервисов healthy;
  load @ ~50 TPS без ошибок; failure-injection → CB OPEN → DLQ↑ → recovery CLOSED)
- База: `doc/tz_v2.md` §2.1/§2.2/§4.1/§7, `doc/implementation_plan.md` §6,
  `doc/phases/06_mocks_frontend.md`

---

## 1. Что сделано

Два mock-сервиса (external stimulus + failure-injection), BFF-шлюз для браузера
и React-фронтенд на реальных событиях:

- **`services/mock-acquirer/`** — генератор нагрузки:
  `POST /acquirer/generate` (1 платёж в Intake), `POST /acquirer/burst`
  (load @ N TPS / duration), `POST /acquirer/burst/stop`,
  `GET /acquirer/burst/status`, `GET /acquirer/status`.
  Валидные + 10% невалидных (amount=0 / negative / wrong currency) — Intake
  обязан их отклонить 422, **не** публиковать в Kafka.
  Task-leak-защита: все background-задачи фиксируются в `set(self._tasks)`.
- **`services/mock-fraud/`** — failure-injection:
  `POST /fraud/check` (approve/decline 80/20), `POST /fraud/admin/fail`
  (`off | errors | timeout`), `GET /fraud/status`.
  `errors` mode — inject 500, `timeout` mode — `sleep 5 s` (> CB `timeout=3 s` →
  CB opens). In-memory state (PoC-допуск по §3 spec: "короткоживущее").
- **`services/gateway/`** — BFF-фасад для браузера (порт **8000**, tz_v2 §4.1):
  - routing `/v1/*` → intake / profile / notification / ledger (compose DNS);
  - **SSE-терминал** `GET /v1/stream`: proxy-терминирует Notification SSE
    byte-for-byte (`StreamingResponse` + `text/event-stream`), **не** 302-redirect
    (spec §5 pitfall #10: redirect ломает EventSource);
  - `GET /v1/_status` — CB-статусы из orchestrator (`/v1/circuit-breakers`);
    UI-бейдж `circuit_breakers.fraud.state` → CLOSED / OPEN / HALF_OPEN.
  - CORS (dev): `CORS_ORIGINS` env (default: `http://localhost:5173`).
- **`frontend/`** — React 18 + Vite + TS:
  - `useSSE("/v1/stream")` — EventSource + встроенный auto-reconnect;
  - `UsersTable` (poll `/v1/users` + refresh on SSE event) — 20 seeded users;
  - `PaymentLog` (lenta платежей из SSE, last 100);
  - `StatusBadge` (CB state из `/v1/_status`);
  - nginx reverse-proxy `/v1 → gateway:8080` (same-origin в Docker, CORS no-op).
- **`docker-compose.yml`** — +`frontend` service (node:20-alpine build →
  nginx:alpine serve, port **5173**, tz_v2 §4.1: 3000 = Grafana),
  gateway env vars (`*_BASE_URL`, `CORS_ORIGINS`), intaker base URL на
  mock-acquirer.
- **`Makefile`** — `load` (50 TPS / 60s burst + polling + final report),
  `fe` (npm ci + build), `acceptance` (7 checks §7 tz_v2).

### 1.1. Live-верификация (этот запуск)

| Проверка | Ожидалось | Реально |
| --- | --- | --- |
| `make ps` | 18/18 healthy | 18/18 healthy ✅ |
| UI `:5173` `/v1/users` | 20 users | 20 users ✅ |
| `python -m pytest -q` | green | **192 passed** ✅ |
| Load @ 50 TPS / 60 s | ≈50 req/s, P99 < 300 ms, errors=0 | `errors: 0`, latency ≈ 26 ms mean, ~49–50 req/s ✅ |
| Fraud `errors` + burst | CB OPEN, DLQ↑ | **CB OPEN** (via `/v1/_status`), `ledger.commands.dlq` 256→589 ✅ |
| Fraud `off` + burst valid | CB CLOSED, valid continue | **CB CLOSED**, valid payments pass ✅ |
| SSE stream (UI) | keepalive + новые события | подтверждено браузером (EventSource) ✅ |

**Примечание:** в `make load` final-строк `requested: 2975, accepted: 2694`
— это 60 s × ~50 TPS суммарно (генератор не останавливается мгновенно на
`end` — остаток задач доделывает). `errors=0` — ноль HTTP-ошибок на 2975
запросов, `rejected:281` — это 10% intentionally-injected invalids
(Intake 422'нул — ожидается).

---

## 2. Файлы

| Файл | Зачем |
| --- | --- |
| `services/mock-acquirer/acquirer_service.py` | Генератор платежей + burst worker + status |
| `services/mock-acquirer/main.py` | Entrypoint (hyphen-имя → `importlib.util.spec_from_file_location`) |
| `services/mock-fraud/fraud_service.py` | `/fraud/check`, `/fraud/admin/fail`, `/fraud/status` |
| `services/gateway/gateway.py` | BFF: routing + SSE-proxy + `/v1/_status` |
| `services/gateway/main.py` | Entrypoint (по pattern mock-acquirer) |
| `frontend/*` | React 18 + Vite + TS + nginx + compose service |
| `docker-compose.yml` | +`frontend` svc, +gateway `*_BASE_URL` / `CORS_ORIGINS`, +acquirer `INTAKE_URL` |
| `Makefile` | +`load`, +`fe` targets |
| `tests/mock/` (new) | Unit: `acquirer_service` burst/status/invalid-injection, `fraud_service` modes |
| `tests/gateway/` (new) | Unit: BFF routing (fake upstream), SSE-proxy framing, `/v1/_status` CB state |
| `.gitignore` | +`.playwright-mcp/` (scratch-артефакты) |

---

## 3. Коммиты (этап 6)

| # | Тип | Тема |
| --- | --- | --- |
| 1 | `feat(mock)` | acquirer: generate + 50 TPS burst + status + task-leak guard |
| 2 | `feat(mock)` | fraud: check + admin/fail (off/errors/timeout) + status |
| 3 | `feat(gateway)` | BFF routing: /v1/payments, /v1/users, /v1/payments-feed, /v1/users/{id}/transactions |
| 4 | `feat(gateway)` | SSE-терминал: /v1/stream proxy (byte-for-byte) + /v1/_status (CB) |
| 5 | `feat(frontend)` | Vite/React/TS scaffold + UsersTable + PaymentLog + StatusBadge |
| 6 | `feat(frontend)` | SSE auto-reconnect hook (EventSource) + useSSE |
| 7 | `chore` | compose: +frontend svc (node→nginx), +gateway env, +acquirer INTAKE_URL |
| 8 | `chore` | Makefile: +`load` (burst + polling), +`fe` (build) |
| 9 | `test` | unit tests: mock-acquirer, mock-fraud, gateway (BFF + SSE + /_status) |
| 10 | `docs` | `doc/done/06_mocks_frontend.md` + `.gitignore` (`.playwright-mcp/`) |

(10 коммитов — чуть выше guide "3–8/phase", но по 1 теме на each,
без смешения — легко ревью.)

---

## 4. Не сделал в Этапе 6 (по spec)

- Rate limiting / auth в gateway (§2.1 TЗ: "оба" — но PoC-граница, не §7 checks).
- SSE `Last-Event-ID` replay (§8 spec "типичные ошибки" — client-side).
- Frontend test suite (Vitest/RTL) — PoC-граница (UI = read-only, backend tested).
- Frontend-формы "создать платёж" (§4 spec explicitly: **no** UI-form for payments).

---

## 5. Следующий шаг

После merge `feat/07-mocks-fe` → открыть Этап 7
(`doc/phases/07_observability_tests.md`): Tempo traces Gateway→Ledger
E2E, Grafana alerts, `circuit_breaker_state` metrics в Prometheus
(сейчас только через `/v1/_status`, не метрики).
