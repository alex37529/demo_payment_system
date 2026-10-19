# Этап 5. Read-Side — Read-модель + SSE

> **Ветка:** `feat/06-read-side`
> **Результат:** UI видит обновление баланса и latest payment ≤2 c после `POST /v1/payments`; SSE-стрим reconnect'ится при обрыве; 20 юзеров сидированы.
> **Чему учимся здесь:** как устроена Read-Side в CQRS: read-модель (Profile), projection из событий (ledger.events → balances), Payment-Log ring buffer, SSE (Server-Sent Events) с keepalive и reconnect.

---

## 1. Зачем Read-Side отдельно

Из Этапа 2 мы знаем: **Write-Side** (`ledger`) отвечает на команды и хранит инварианты. Он **медленный** (транзакции, блокировки) и **не оптимизирован** под «покажи список 20 юзеров + баланс в таблице UI».

**Read-Side** (Profile + Notification) — **быстрое чтение**:
- **Profile Service**: read-модель `users(id, name, email, currency, balance)` — **денормализованная** (1 строка = юзер + его баланс). Consumer `ledger.events` обновляет баланс.
- **Notification Service**: ring buffer последних N платежей (in-memory) + SSE-стрим.

**Общий приём:** Read-Side **слушает** `ledger.events` (события из Write-Side, через Debezium→Kafka, Этап 2) и **проецирует** их в удобные для чтения формы. Это и есть CQRS (§3.8 TЗ v2):
- **Command** (запись) → Ledger.
- **Query** (чтение) → Profile/Notification.

> **Подводный камень №1 (консистентность Write/Read):** Read-Side **eventual consistency** — баланс в Profile может «отставать» на 100–300ms от Ledger (время события в Kafka + обработка). Это **норм** для CQRS (TЗ §5: «данные должны быть консистентны на момент старта» — seed атомарный; дальше — eventual). UI должен понимать: «баланс обновится через 100ms», не «сразу».

---

## 2. Seed 20 юзеров (при старте)

**Требование** (§5 TЗ v2): 20 юзеров, баланс $100–$10 000. **Консистентность**: Write- и Read-модели на момент старта **одинаковы**.

**Решение:** Seed выполняет два действия **в одной миграции**:
1. INSERT 20 `ledger.accounts` (Write-Side).
2. INSERT 20 `profile.users` (Read-Side, денормализованная копия баланса).

```sql
-- init/50_seed.sql (выполняется при первом старте postgres)
-- Write-side (ledger.accounts)
INSERT INTO ledger.accounts (id, owner_name, currency, balance, reserved)
VALUES
  ('A01', 'John Doe',   'USD', 1250000, 0),
  ('A02', 'Jane Smith', 'EUR',  830000, 0),
  -- ... 18 more (случайные $100..$10_000 в копейках)
;

-- Read-side (profile.users) — ДЕНОРМАЛИЗОВАННАЯ
CREATE SCHEMA IF NOT EXISTS profile;
CREATE TABLE profile.users (
  id          TEXT PRIMARY KEY,
  name        TEXT,
  email       TEXT,
  currency    TEXT,
  balance     BIGINT,
  updated_at  TIMESTAMPTZ
);
INSERT INTO profile.users (id, name, email, currency, balance)
SELECT id, owner_name, (id || '@example.com'), currency, balance
FROM ledger.accounts;   -- ★ копия баланса из Write
```

> **Подводный камень №2 (seed = одна TX на оба):** Если seed в 2 разных контейнерах (migration ledger + migration profile) — **не атомарно** (расхождение при crash). Решение: **одна** migration делает оба INSERT'а (или «seed-скрипт» запускается **одним** `docker compose run seed`). В PoC — `init/50_seed.sql` (выполняется PG-при первом старте, **одна** TX).

> **Подводный камень №3 (баланс в копейках):** 1250000 = $12 500.00 (12 500 × 100). Не 12500.00 (float!). int.

---

## 3. Profile Service — read-модель балансов

### 3.1. Consumer `ledger.events` → update balances

```python
# services/profile/consumers.py
class ProfileConsumer:
    def __init__(self, repo, brokers):
        self.repo = repo
        self.c = KafkaConsumer(brokers, group="profile-balance", topics=["ledger.events"])

    async def run(self):
        await self.c.start()
        async for key, payload in self.c.stream():
            # payload = FundsDebited (или FundsReserved, CancelReservation, Refund)
            evt = payload.get("event", payload)   # normalisation
            account_id = payload["account_id"]
            new_balance = payload.get("new_balance", {}).get("amount")
            if new_balance is None:
                # Delta-based (FundsReserved) — не обновляем «final balance»
                continue
            # ★ Idempotent (по event_id):
            if await self.repo.is_processed(payload["event_id"]): continue
            await self.repo.set_balance(account_id, new_balance)
            await self.repo.mark_processed(payload["event_id"])
```

### 3.2. Read-модель (denormalized)

```python
# services/profile/infrastructure/models.py
class UserRow(Base):
    __tablename__ = "users"
    __table_args__ = {"schema": "profile"}
    id          = Column(String(64), primary_key=True)
    name        = Column(String(255))
    email       = Column(String(255))
    currency    = Column(String(3))
    balance     = Column(BigInteger)
    updated_at  = Column(DateTime(timezone=True), onupdate=datetime.utcnow)
```

> **Подводный камень №4 (denormalized vs joined):** Profile **не делает JOIN** к ledger.accounts (разные СХЕМЫ + разные БД — `profile` ≠ `ledger`, см. §2.2 TЗ v2). Он **копирует** баланс в свою таблицу (consumer `ledger.events`). Если бы JOIN — нарушались бы границы контекстов (CQRS-нарушение).

> **Подводный камень №5 (event_id idempotency):** Kafka at-least-once → `ledger.events` может прийти 2 раза. `mark_processed(event_id)` (unique) — защита от dup.

### 3.3. API `GET /v1/users`

```python
@app.get("/v1/users")
async def list_users():
    rows = await repo.list_all()
    return [{"id": r.id, "name": r.name, "email": r.email,
             "currency": r.currency, "balance": r.balance} for r in rows]
```

---

## 4. Notification Service — ring buffer + SSE

### 4.1. Payment-Log ring buffer

**Цель:** удерживать последние N (напр., `N=1000`) платежей для мгновенного отображения в UI (без запроса к БД). **In-memory** (fast), с опциональным SQLite-persist (на случай restart).

```python
# services/notification/ring.py
from collections import deque
class PaymentLog:
    def __init__(self, maxlen: int = 1000):
        self._dq = deque(maxlen=maxlen)

    def push(self, payment: dict):
        self._dq.appendleft(payment)   # newest first

    def latest(self, n: int = 20) -> list[dict]:
        return list(self._dq)[:n]

    def __len__(self): return len(self._dq)
```

**Consumer:** Notification **слушает** `ledger.events` (те же события, что Profile), но **не обновляет** баланс — **добавляет** в ring.

```python
async def consume(self):
    async for key, payload in self.c.stream():
        payment = {
            "payment_id": payload["payment_id"],
            "account_id": payload["account_id"],
            "event": payload["event"],
            "amount": payload.get("amount"),
            "new_balance": payload.get("new_balance"),
            "ts": payload["ts"],
        }
        self.log.push(payment)
        await self._broadcast(payment)   # → SSE subscribers
```

### 4.2. SSE endpoint

**SSE (Server-Sent Events)** — HTTP/1.1, один-на-много, **unencrypted** (не WS). Client подписывается на `/v1/stream`, server «пушит» новые платежи.

```python
# services/notification/sse.py
import asyncio
from starlette.responses import StreamingResponse

class SSERelay:
    """Fan-out: 1 событие → N подписчиков."""
    def __init__(self):
        self._subscribers: list[asyncio.Queue] = []

    def subscribe(self) -> asyncio.Queue:
        q = asyncio.Queue(maxsize=100)
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue):
        if q in self._subscribers:
            self._subscribers.remove(q)

    async def broadcast(self, event: dict):
        for q in list(self._subscribers):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass   # «медленный» client — drop (не блокируем)

    def format_sse(self, event: dict) -> str:
        """SSE protocol: `data: <json>\n\n`."""
        import json
        return f"data: {json.dumps(event)}\n\n"

    async def keepalive_gen(self, q: asyncio.Queue):
        """Генератор для StreamingResponse: события + heartbeat."""
        while True:
            try:
                evt = q.get_nowait()
                yield self.format_sse(evt)
            except asyncio.QueueEmpty:
                # Heartbeat (каждые 15s) — client не «думает, что обрыв»
                yield ": keepalive\n\n"
                await asyncio.sleep(15)
```

### 4.3. Endpoint

```python
@app.get("/v1/stream")
async def sse_stream(request: Request):
    q = relay.subscribe()
    return StreamingResponse(
        relay.keepalive_gen(q),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",   # nginx: не буферизовать
        })
```

**Формат** (каждое событие):
```
: keepalive
data: {"payment_id":"P1","account_id":"A1","event":"FundsDebited","amount":{"amount":100,"currency":"USD"},"ts":"2026-10-19T12:00:00Z"}

: keepalive
data: {"payment_id":"P2","account_id":"A2","event":"FundsDebited",...}

```

> **Подводный камень №6 (keepalive 15s):** Без heartbeat — proxy/браузер закроет коннект через 30–60s (idle timeout). `: keepalive` (SSE comment) «оживляет» — client понимает «есть соединение».
>
> **Подводный камень №7 (QueueFull):** «Медленный» client (network) — Queue overflow. **Drop**, не блокируем. Иначе один «медленный» client тормозит всех (head-of-line blocking).
>
> **Подводный камень №8 (X-Accel-Buffering):** Nginx **буферизует** по умолчанию — SSE «зависает» (события копятся, а не «летят»). `X-Accel-Buffering: no` — отключает буфер. (В PoC без nginx — но хедер нужен для прода.)

### 4.4. Client (browser) — auto-reconnect

```javascript
// frontend (Этап 6)
const evtSource = new EventSource("/api/v1/stream");
evtSource.onmessage = (e) => {
  const payment = JSON.parse(e.data);
  // обновить UI (таблица пользователей + лента платежей)
};
evtSource.onerror = () => {
  // Браузер АВТОМАТИЧЕСКИ reconnect'ится (встроенная фича EventSource)
  // Мы можем логгировать: "SSE disconnected, retry..."
  console.warn("SSE disconnected");
};
```

**Важно:** `EventSource` **встроенный reconnect** (exponential backoff, до 30s). Если Notification restart'ится — client **сам** переподключится (в течение секунды). Это критично для acceptance §7 TЗ v2: «Фронтенд корректно переподключается при обрыве SSE».

> **Подводный камень №9 (EventSource vs WebSocket):** SSE — **односторонний** канал (сервер → клиент). WS — **двунаправленный**. Для «ленты платежей» хватает SSE. WS — лишняя сложность (protocol upgrade, binary, backpressure). TЗ v2: «EventSource» — SSE.
>
> **Подводный камень №10 (EventSource + CORS):** SSE **не поддерживает** кастомные headers (только `Last-Event-ID`). Если используем auth — только cookie (не Bearer). В PoC — без auth (local). Для прода — cookie + SameSite.

---

## 5. Коммиты Этапа 5

| # | Тип | Сообщение |
|---|---|---|
| 1 | `feat(profile)` | add read model + seed 20 users (dual ledger+profile) |
| 2 | `feat(profile)` | add consumer `ledger.events` → update balances (idempotent) |
| 3 | `feat(profile)` | add `GET /v1/users` API |
| 4 | `feat(notification)` | add Payment-Log ring buffer (in-mem, maxlen=1000) |
| 5 | `feat(notification)` | add SSE endpoint with keepalive (15s) |
| 6 | `feat(notification)` | add fan-out broadcast (N subscribers) |
| 7 | `test(notification)` | add SSE integration test (reconnect on kill) |

**Body (пример):**
```
feat(notification): add SSE endpoint with keepalive (15s)

Why:
  TЗ §3.9 — Realtime Notification Service: SSE-пуш для дашборда.
  TЗ §7 — UI/UX: front корректно переподключается при обрыве SSE.

What:
  - SSERelay: fan-out (1 событие → N подписчиков)
  - /v1/stream: StreamingResponse (media_type=text/event-stream)
  - Keepalive (15s) — heartbeat («: keepalive»)
  - X-Accel-Buffering: no (nginx не буферизует)
  - EventSource (client-side) — auto-reconnect встроен

Design notes:
  - Queue(maxsize=100) — «медленный» client не блокирует broadcast
  - Drop on QueueFull (head-of-line protection)
  - SSE (не WS) — одна (client ← server), проще, EventSource встроен

Refs: doc/tz_v2.md §3.9, §7
```

---

## 6. Как проверить (exit criteria)

```
make up
# 1. 20 юзеров сидированы:
curl http://localhost:8004/v1/users | jq length
# → 20

# 2. SSE стрим подключается:
curl -N http://localhost:8005/v1/stream
# (keepalive каждые 15s, пустые)

# 3. Создаём платёж (через Intake, Этап 4):
curl -X POST http://localhost:8001/v1/payments \
     -H 'Idempotency-Key: K9' -d '{"amount":100,"currency":"USD","account_id":"A01"}'
# → 202

# 4. Ждём 2s. Проверяем:
#    a) SSE: в потоке появилось событие (FundsDebited/P1)
#    b) Bаланс A01 в /v1/users убавился на 100
#    c) Payment-Log ring: latest() содержит P1

# 5. SSE reconnect: docker compose stop notification
#    → SSE отключился (client browser: "SSE disconnected")
#    → docker compose start notification
#    → Client АВТОМАТИЧЕСКИ reconnect'ится (EventSource встроен)
#    → Новые платежи снова «летят»
```

**Красные флаги:**
- [ ] /v1/users — пустой или <20.
- [ ] SSE — 404 или не стримится.
- [ ] Платёж создан, но баланс не обновился (consumer сломан).
- [ ] SSE — нет keepalive (закрылся через 30s).
- [ ] Kill Notification → Client не reconnect'ится.

---

## 7. Типичные ошибки

| Ошибка | Симптом | Как избежать |
| --- | --- | --- |
| Consumer не idempotent | Двойное обновление баланса | Unique `event_id` + `mark_processed` |
| SSE без keepalive | Закрылся через 30s (idle) | `: keepalive` каждые 15s |
| Nginx buffer'ит SSE | События «зависают» | `X-Accel-Buffering: no` |
| «Медленный» client блокирует broadcast | Задержки у всех | Queue + drop on QueueFull |
| Seed в 2 миграциях | Рассхождение (crash) | Одна TX / seed-скрипт (atomaric) |
| JOIN в Profile к ledger | Нарушение CQRS | Consumer `ledger.events`, копирует |
| EventStream без `Last-Event-ID` | После reconnect — дубли | Client присылает `Last-Event-ID`, server начинает с `>` |

---

## 8. Чек-лист перед merge Этапа 5

- [ ] `make up` → все UP.
- [ ] `GET /v1/users` → 20 юзеров.
- [ ] `GET /v1/stream` → SSE (keepalive).
- [ ] Платёж (через Intake) → баланс обновился ≤2s.
- [ ] Payment-Log ring — latest содержит платёж.
- [ ] Kill Notification → Client reconnect'ится.
- [ ] 7 коммитов в `feat/06-read-side`.

> **После merge в `main`** — открывать Этап 6 (`06_mocks_frontend.md`).
