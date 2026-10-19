# Этап 6. Mocks + Frontend

> **Ветка:** `feat/07-mocks-fe`
> **Результат:** UI в реальном времени обновляет баланс + ленту платежей; load-тест @ 50 TPS держится 60 c; Fraud API можно «сломать» в рантайме; Gateway маршрутизирует и терминирует SSE.
> **Чему учимся здесь:** как устроены внешние шлюзы (Acquirer, Fraud) — генераторы нагрузки и failure-injection, как React-фронтенд потребляет REST + SSE, как Gateway/BFF шьёт всё в один фасад для браузера.

---

## 1. Два Mock-сервиса: зачем нужны

| Сервис | Роль | Ключевой endpoint |
| --- | --- | --- |
| **Mock Acquirer** | Имитирует внешних мерчантов/шлюзов. Генерирует **валидные** и **невалидные** платежи; может держать **load @ 50 TPS**. | `POST /acquirer/burst {tps, duration}` |
| **Mock Fraud API** | Имитирует внешнюю фрод-проверку. **Failure-injection**: `POST /fraud/admin/fail {mode: off\|errors\|timeout}`. | `POST /fraud/check`, `POST /fraud/admin/fail` |

Оба — **заглушки** (не реальная бизнес-логика). Их задача — дать системе **внешние стимулы** и **предсказуемые отказы** для проверки отказоустойчивости.

> **Подводный камень №1 (Mock ≠ production code):** Мок-кода **не должно** быть в сервисе. Он **внешний** и **управляемый** (endpoint `/admin/fail`). В production эти сервисы заменятся на реальные (шлюзы, фрод-ML), но **контракт** (endpoint, payload, статусы) тот же. ТЗ §2.2 v2: «Fraud API — только заглушка».

---

## 2. Mock Acquirer (генератор нагрузки)

### 2.1. Генерация платежей

```python
# services/mock-acquirer/main.py
import random
from fastapi import FastAPI

app = FastAPI(title="mock-acquirer")
setup(app, service="mock-acquirer")
INTAKE_URL = "http://intake:8001"
client = httpx.AsyncClient(timeout=5.0)
ACCOUNTS = [f"A{i:02d}" for i in range(1, 21)]   # A01..A20 (сидированы)

@app.post("/acquirer/generate")
async def generate(payload: GenerateRequest):
    """Сгенерировать 1 (валидный/невалидный) платёж и отправить в Intake."""
    account = random.choice(ACCOUNTS)
    currency = random.choice(["USD", "EUR"])
    amount = random.randint(10_00, 100_000)    # $10..$1000 (в копейках)
    # 10% «невалидных» (отрицательный, не-USD/EUR, 0)
    if payload.include_invalids and random.random() < 0.10:
        amount = 0 if random.random()<0.5 else -1000   # невалидный
    body = {"amount": amount, "currency": currency, "account_id": account}
    idem = f"acq-{uuid4()}"
    r = await client.post(f"{INTAKE_URL}/v1/payments",
                          json=body, headers={"Idempotency-Key": idem})
    return {"status": r.status_code, "payment_id": r.json().get("payment_id")}
```

### 2.2. Load mode @ 50 TPS

**Цель:** 50 платежей/сек в течение `duration` секунд (60 c по умолчанию). Измерить P99, CB-статус, DLQ.

```python
@app.post("/acquirer/burst")
async def burst(payload: BurstRequest):
    """Load-тест: tps=50, duration=60s."""
    tps = payload.tps          # 50
    duration = payload.duration # 60
    interval = 1.0 / tps        # 0.02s = 50/с
    async def worker():
        end = time.time() + duration
        while time.time() < end:
            await asyncio.create_task(generate(GenerateRequest(include_invalids=True)))
            await asyncio.sleep(interval)
    await asyncio.create_task(worker())
    return {"status": "running", "tps": tps, "duration": duration}
```

> **Подводный камень №2 (TPS ≠ latency):** 50 TPS = 50 запросов/сек, **не** «50 за 50мс». `interval = 1/tps` = 20ms. Если latency одного запроса > 20ms — TPS упадёт **ниже** 50. Это ОК: load-тест измеряет **фактический** throughput, не «желаемый».
>
> **Подводный камень №3 (невалидность 10%):** По §2.2 v2: «Генерирует валидные **и невалидные** (с ошибками, таймаутами) платежи». 10% невалидных (amount=0/-1000) — Intake должен их **отклонить** (422), **не** опубликовать в Kafka. Проверка: `payments.commands` содержит **только** валидные.
>
> **Подводный камень №4 (async task leak):** `asyncio.create_task` — fire-and-forget. Если не хранить reference — GC может «убить» task. Решение: собрать в `set`, удалить по done. В PoC — ок (короткий burst).

---

## 3. Mock Fraud API + failure-injection

### 3.1. Нормальный режим

```python
# services/mock-fraud/main.py
app = FastAPI(title="mock-fraud")
setup(app, service="mock-fraud")
_state = {"mode": "off"}   # off | errors | timeout

@app.post("/fraud/check")
async def check(payload: dict):
    """Имитация фрод-проверки. В «off» — approve, в «errors» — 500, в «timeout» — sleep 5s."""
    if _state["mode"] == "errors":
        raise HTTPException(status_code=500, detail="fraud service down (injected)")
    if _state["mode"] == "timeout":
        await asyncio.sleep(5)    # > CB timeout (3s) → CB opens
    # Нормальный режим: approve 80%, decline 20%
    return {"decision": "approve" if random.random() < 0.8 else "decline",
            "score": round(random.random(), 3)}

@app.post("/fraud/admin/fail")
async def admin_fail(payload: dict):
    """Failure-injection: переключить mode. Это «ручка» для демо/тестов."""
    mode = payload.get("mode", "off")
    if mode not in ("off", "errors", "timeout"):
        return JSONResponse(status_code=400, content={"error": "bad mode"})
    _state["mode"] = mode
    log.warning("Fraud mode → %s", mode)
    return {"mode": _state["mode"]}

@app.get("/fraud/status")
async def status():
    return {"mode": _state["mode"]}
```

### 3.2. Как это проверяется в acceptance (§7 TЗ v2)

```
# Карта портов: tz_v2 §4.1 — fraud=8007, acquirer=8006
1. curl -X POST http://localhost:8007/fraud/admin/fail -d '{"mode":"errors"}'
2. curl -X POST http://localhost:8006/acquirer/burst -d '{"tps":50,"duration":30}'
3. Ждём 30c.
4. Проверки:
   - CB open: pybreaker_open=1 (Prometheus)
   - DLQ: ledger.commands.dlq содержит ≥ N (все fraud-fail)
   - Валидные: ≥ 10 TPS Success (fraud OK)
   - Оркестратор: **не упал** (CB изолирует)
5. curl -X POST .../fraud/admin/fail -d '{"mode":"off"}'  (восстановить)
```

> **Подводный камень №5 (timeout mode = 5s sleep):** CB-таймаут (3s) **короче** — значит, CB «увидит» timeout и откроется. Если CB-таймаут **длиннее** (10s) — CB не откроется (все запросы «успевают»). **Важно:** CB timeout (3s) < fraud timeout-injection (5s). Настройка CB (§3.6 TЗ v2): `timeout=3.0`.
>
> **Подводный камень №6 (mode in-memory, не в БД):** `_state` — in-memory. Restart Fraud API → mode сбросится в `off`. Это ОК для PoC (failure-injection — короткоживущее). Для persist — Redis/БД.

---

## 4. Frontend (React 18 + Vite + TypeScript)

### 4.1. Структура

```
frontend/
├── index.html
├── package.json
├── Dockerfile
├── vite.config.ts
├── tsconfig.json
└── src/
    ├── main.tsx
    ├── App.tsx
    ├── api/
    │   ├── users.ts       # GET /v1/users
    │   └── stream.ts      # EventSource /v1/stream
    ├── components/
    │   ├── UsersTable.tsx
    │   ├── PaymentLog.tsx
    │   └── StatusBadge.tsx
    └── hooks/
        └── useSSE.ts      # SSE + auto-reconnect
```

### 4.2. SSE consumer (auto-reconnect)

```typescript
// src/hooks/useSSE.ts
import { useEffect, useRef, useState } from "react";

export function useSSE(url: string) {
  const [payments, setPayments] = useState<Payment[]>([]);
  const [status, setStatus] = useState<"open"|"closed"|"connecting">("connecting");

  useEffect(() => {
    const es = new EventSource(url);
    es.onopen   = () => setStatus("open");
    es.onmessage= (e) => {
      const p = JSON.parse(e.data);
      setPayments(prev => [p, ...prev].slice(0, 100));   // последние 100
    };
    es.onerror  = () => {
      setStatus("closed");
      // EventSource АВТОМАТИЧЕСКИ reconnect'ится (встроен)
      // Можно логгировать:
      console.warn("SSE disconnected, will auto-reconnect");
    };
    return () => { es.close(); };
  }, [url]);

  return { payments, status };
}
```

### 4.3. Users Table (fetch + polling)

```typescript
// src/components/UsersTable.tsx
export function UsersTable() {
  const [users, setUsers] = useState<User[]>([]);
  useEffect(() => {
    const load = async () => setUsers(await (await fetch("/v1/users")).json());
    load();
    const iv = setInterval(load, 5000);   // refresh каждые 5s (или на SSE-событии)
    return () => clearInterval(iv);
  }, []);
  return <table>
    <thead><tr><th>Имя</th><th>Email</th><th>Валюта</th><th>Баланс</th></tr></thead>
    <tbody>
      {users.map(u => <tr key={u.id}>
        <td>{u.name}</td><td>{u.email}</td><td>{u.currency}</td>
        <td>{formatMoney(u.balance, u.currency)}</td>
      </tr>)}
    </tbody>
  </table>;
}
```

### 4.4. StatusBadge (CB status)

```typescript
// src/components/StatusBadge.tsx
export function StatusBadge() {
  const [cb, setCb] = useState<boolean | null>(null);
  useEffect(() => {
    const load = async () => {
      // Gateway (BFF) агрегирует /_status от сервисов, включая CB
      const r = await fetch("/v1/_status");
      setCb(r.json().then(o => o.circuit_breakers?.open ?? false));
    };
    load();
    const iv = setInterval(load, 5000);
    return () => clearInterval(iv);
  }, []);
  return <span className={cb ? "badge-red" : "badge-green"}>
    CB: {cb ? "OPEN" : "CLOSED"}
  </span>;
}
```

> **Подводный камень №7 (frontend — read-only):** Frontend **не** создаёт платежи (только Intake — through Gateway). UI: таблица юзеров + лента платежей + CB badge. **Не** форма «создать платёж» (это не PoC-фича).
>
> **Подводный камень №8 (CORS + порт):** Frontend (порт **5173**, см. `tz_v2 §4.1`) → API (порт 8000) — **разные** origin. Nginx (Gateway) **проксирует** — CORS не нужен. В dev (без nginx) — `CORS` на Gateway: `allow_origins=["http://localhost:5173"]`. **Почему 5173, а не 3000:** 3000 занят Grafana (оба нужны одновременно). Vite default — 5173.

### 4.5. Vite config (dev proxy)

```typescript
// vite.config.ts
export default defineConfig({
  server: {
    port: 5173,                                   // НЕ 3000 — занят Grafana (tz_v2 §4.1)
    proxy: {
      "/v1": { target: "http://localhost:8000", changeOrigin: true },   // gateway
    },
  },
  build: { outDir: "dist" },
});
```

**Docker:**
```dockerfile
FROM node:20-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build
FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf   # proxy /v1 → gateway
# expose 80
```

---

## 5. Gateway (BFF) — фасад для браузера

**Gateway/BFF** (§2.1 TЗ v2):
- **Маршрутизация** `/v1/*` → соответствующий сервис (Intake, Profile, Notification).
- **SSE-терминал** — терминирует SSE от Notification для браузера (прокси + keepalive).
- **Идемпотентность** — middleware (уже в shared, Этап 1).
- **Агрегация** `/v1/_status` — CB-статусы от сервисов.

```python
# services/gateway/main.py
app = FastAPI(title="gateway")
setup(app, service="gateway")

# Routes → Intake
@app.post("/v1/payments")
async def create_payment(req: CreatePaymentRequest, idem: str | None = Header(alias="Idempotency-Key")):
    return await intake_client.forward(req, idem)

@app.get("/v1/payments/{id}")
async def get_payment(id: str): return await intake_client.forward(id)

# Routes → Profile
@app.get("/v1/users")
async def users(): return await profile_client.forward()

# SSE — проксируем Notification
@app.get("/v1/stream")
async def sse(request: Request):
    return await notification_client.stream()   # StreamingResponse

# Агрегация статусов (CB, health)
@app.get("/v1/_status")
async def status():
    return {
        "up": True,
        "circuit_breakers": {
            "fraud": await fraud_cb_client.get_state()   # open/closed/half-open
        }
    }
```

> **Подводный камень №9 (Gateway ≠ API Gateway):** «API Gateway» = **маршрутизация + rate limit + auth**. «BFF» = **адаптация данных для клиента**. У нас **оба** (маршрутизация **+** SSE-прокси **+** status-агрегация). Название: «Gateway/BFF» (TЗ v2 §2.1).
>
> **Подводный камень №10 (SSE-проксирование):** Gateway **должен** термировать (не «перенаправлять») SSE. Если browser → gateway (SSE), gateway → notification (SSE) — два SSE (fan-out). Gateway **проксирует** streaming response (не `302 redirect`). `StreamingResponse` + `media_type="text/event-stream"`.

---

## 6. Коммиты Этапа 6

| # | Тип | Сообщение |
|---|---|---|
| 1 | `feat(mock)` | add Acquirer generator (valid/invalid, random) |
| 2 | `feat(mock)` | add 50 TPS load mode (async worker) |
| 3 | `feat(mock)` | add Fraud stub + `admin/fail` (off/errors/timeout) |
| 4 | `feat(frontend)` | add Vite/React/TS scaffold |
| 5 | `feat(frontend)` | add UsersTable (fetch) + PaymentLog (SSE) + StatusBadge (CB) |
| 6 | `feat(frontend)` | add SSE auto-reconnect (built-in EventSource) |
| 7 | `feat(gateway)` | add BFF with routing + SSE-прокси + /_status |
| 8 | `feat(gateway)` | add CORS (dev) + nginx (prod) |

**Body (пример):**
```
feat(mock): add Fraud stub + admin/fail (off/errors/timeout)

Why:
  TЗ §2.2 — Fraud API «только заглушка» (§4 decision: заглушка).
  TЗ §7 — Failure: CB open, DLQ, valid ≥10 TPS.

What:
  - POST /fraud/check: approve 80% (off), 500 (errors), sleep 5s (timeout)
  - POST /fraud/admin/fail: переключить mode (off/errors/timeout)
  - GET /fraud/status: текущий mode
  - In-memory state (PoC: кратковременное)
  - 5s timeout > CB timeout 3s → CB opens (fraud timeout-injection)

Refs: doc/tz_v2.md §2.2, §3.6, §7
```

---

## 7. Как проверить (exit criteria)

```
make up

# 1. UI поднимается:
docker compose ps frontend         # running
# Browser: http://localhost:5173 → таблица 20 юзеров + StatusBadge (CB: CLOSED)
# (3000 — Grafana, tz_v2 §4.1)

# 2. SSE stream:
#   (в browser DevTools → Network → /v1/stream → Events)
#   → keepalive каждые 15s

# 3. Load-тест:
curl -X POST http://localhost:8006/acquirer/burst -d '{"tps":50,"duration":60}'
# → "status":"running"
# Ждём 60c. В UI: лента платежей «летит» (50/с), баланс обновляется.

# 4. Failure-injection:
curl -X POST http://localhost:8007/fraud/admin/fail -d '{"mode":"errors"}'
# → {"mode":"errors"}
# Ждём:
#   CB badge → OPEN (красный)
#   DLQ depth растёт
#   Валидные continue (≥10 TPS)
#   Оркестратор НЕ упал

# 5. Восстановление:
curl -X POST http://localhost:8007/fraud/admin/fail -d '{"mode":"off"}'
# → CB CLOSED, новые платежи Success
```

**Красные флаги:**
- [ ] UI — пустой (404 /v1/users, /v1/stream).
- [ ] Load @ 50 TPS — P99 > 300ms (SLO, §4 TЗ v2).
- [ ] CB не open (fraud fail не сработал).
- [ ] Валидные платежи «остановились» (CB блокирует).
- [ ] SSE — без keepalive (закрылся).
- [ ] Gateway — не маршрутизирует (404).

---

## 8. Типичные ошибки

| Ошибка | Симптом | Как избежать |
| --- | --- | --- |
| Frontend CORS | 403 (different origin) | Nginx (Gateway) или CORS-headers на Gateway |
| SSE-redirect (не proxy) | Browser → 302 → SSE не работает | Gateway **проксирует** (не redirect) |
| Load @ 50 TPS = 50/20ms | TPS ниже (latency) | Измерять **фактический** throughput, не «желаемый» |
| Fraud timeout < CB timeout | CB не opens | fraud timeout (5s) > CB (3s) |
| In-memory state (fraud) reset | Mode сбросился после restart | Persist в Redis/БД (PoC: ок) |
| EventSource без `Last-Event-ID` | После reconnect — дубли/потеря | Client `Last-Event-ID` + server start-from |

---

## 9. Чек-лист перед merge Этапа 6

- [ ] `make up` → все UP.
- [ ] UI: 20 юзеров + CB badge (CLOSED).
- [ ] SSE: keepalive + новые платежи.
- [ ] Load @ 50 TPS: P99 < 300ms (Grafana).
- [ ] Fraud fail (mode=errors): CB OPEN, DLQ ↑, valid continue.
- [ ] 8 коммитов в `feat/07-mocks-fe`.

> **После merge в `main`** — открывать Этап 7 (`07_observability_tests.md`).
