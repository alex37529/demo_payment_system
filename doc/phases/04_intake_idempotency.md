# Этап 4. Payment Intake — приём платежей + идемпотентность

> **Ветка:** `feat/05-intake`
> **Результат:** `POST /v1/payments` с дублем `Idempotency-Key` → 2-й ответ кэширован, **ровно 1** сообщение в Kafka `payments.commands`.
> **Чему учимся здесь:** вход в систему: DTO-валидация, идемпотентность (Redis + PG fallback), генерация `Idempotency-Key`, producer `payments.commands`.

---

## 1. Роль Intake

Intake — **вход** в систему (после Gateway/BFF, §2.1 TЗ v2). Он:

1. Принимает HTTP-запрос «создать платёж».
2. **Валидирует DTO** (Pydantic v2): формат, валюта, позитивность, лимиты.
3. **Обеспечивает идемпотентность** — `Idempotency-Key` из заголовка (§3.1 TЗ v2).
4. **Генерирует** / принимает `payment_id`.
5. **Опубликует** команду `Create` в Kafka `payments.commands` → Оркестратор.

**Intake НЕ знает о списании** (§2.1 TЗ v2) — бизнес-логика в Ledger и Orchestrator. Intake — «фасад» + идемпотентность + producer.

> **Почему Intake отдельный от Gateway?** Gateway — единая точка входа, SSE, routing. Intake — «приём конкретного домена: платежи». Разное: Gateway может принимать и «профиль» запросы, и «платеж» — Intake только платежи. Границы контекстов (Bounded Contexts, §1 TЗ v2).

---

## 2. DTO-валидация (Pydantic v2)

```python
# services/intake/application/dtos.py
from pydantic import BaseModel, Field, field_validator
from typing import Literal

class CreatePaymentRequest(BaseModel):
    """Вход: создать платёж."""
    amount: int = Field(..., gt=0, le=100_000, description="amount in cents, $1 = 100")
    currency: Literal["USD", "EUR"]
    account_id: str = Field(..., min_length=1, max_length=64)
    client_ref: str | None = Field(default=None, max_length=128)   # клиентская ссылка

    @field_validator("amount")
    @classmethod
    def _amount_positive(cls, v: int) -> int:
        if v <= 0: raise ValueError("amount must be > 0")
        return v

    @field_validator("account_id")
    @classmethod
    def _account_format(cls, v: str) -> str:
        import re
        if not re.match(r"^[A-Za-z0-9_-]+$", v):
            raise ValueError("account_id invalid format")
        return v

    def to_command(self) -> dict:
        """Команда → Kafka `payments.commands` (payload §3.11 TЗ v2)."""
        return {
            "cmd": "Create",
            "payment_id": self._gen_id(),
            "amount": {"amount": self.amount, "currency": self.currency},
            "account_id": self.account_id,
            "client_ref": self.client_ref,
        }
```

**Ключевые правила DTO:**
- **Pydantic v2** (`field_validator`, `Literal`, `ge/le`).
- **amount в копейках** (int, не float) — см. Этап 2.
- **Validation = защита до Kafka**. Невалидный DTO → 422, не попадает в Kafka (не тратим ресурсы оркестратора).

> **Подводный камень №1:** `Literal["USD","EUR"]` — жёсткий набор. Если клиент шлёт `"usd"` (lowercase) → 422. Это **фича**, не баг (строгая схема).

---

## 3. Идемпотентность — ключевая часть этапа

### 3.1. Концепт (§3.1 TЗ v2)

`Idempotency-Key` — **заголовок** HTTP-запроса. Один и тот же ключ = один и тот же результат. Если клиент «повторяет» запрос (retry, network) — Intake не дублирует.

```
Client ──POST /v1/payments ──▶ Intake
   │        Idempotency-Key: K1
   ▼
Redis: SET idem:K1 NX EX 86400     (SET NX = Set-if-NotExist)
   │
   ├─ [K1 не был] → продолжаем:
   │     publish `payments.commands` (Create)
   │     cache response: SET idem:K1 = response_json
   │     → 200
   │
   └─ [K1 уже был] →
         GET idem:K1 → cached response (от 1-го раза)
         → 200 (тот же самый!)
         (не публикуем повторно)
```

### 3.2. Формулировка проблемы

**Без идемпотентности:**
```
Client: POST /v1/payments (K1)
  → Intake: publish `Create` → Kafka
  → Network timeout (response не дошёл до Client)
Client: retry → POST /v1/payments (K1)   (тот же ключ!)
  → Intake: publish `Create` → Kafka  (ЕЩЁ РАЗ)
  → Result: 2 платежа = двойное списание! 💸
```

**С идемпотентностью:**
```
Client: POST /v1/payments (K1)
  → Intake: SET idem:K1 NX (success), publish `Create`, cache response, 200
Client: network timeout, retry → POST (K1)
  → Intake: SET idem:K1 NX (FAIL, уже был) → GET idem:K1 → cached response, 200
  → Result: 1 платёж ✓
```

### 3.3. Redis + PG fallback (dual-write)

**Redis SET NX** — быстрая, в-memory. **PG fallback** — если Redis down, идемпотентность **всё ещё нужна** (БД).

```python
# services/intake/application/idempotency.py
import aioredis
from shared.db.base import Base
from sqlalchemy import Column, String, JSON, DateTime

class IdempotencyKeyTable(Base):
    __tablename__ = "idempotency_keys"
    __table_args__ = {"schema": "intake"}
    key         = Column(String(128), primary_key=True)
    response    = Column(JSON)                 # cached response
    created_at  = Column(DateTime(timezone=True), server_default=func.now())
    ttl_until   = Column(DateTime(timezone=True))  # 24h

class IdempotencyService:
    def __init__(self, redis: aioredis, session_factory):
        self.redis = redis
        self.session_factory = session_factory

    async def acquire(self, key: str) -> bool:
        """True = «новый», False = «повтор». ATOmically."""
        try:
            # SET NX EX 24h — атомарно: 1 из N вызовов вернёт True
            ok = await self.redis.set(f"idem:{key}", "1", nx=True, ex=86400)
            if ok:
                # DUAL-WRITE: персистентная копия в PG (на случай сбоя Redis)
                async with self.session_factory() as session:
                    session.add(IdempotencyKeyTable(key=key, response={}, ttl_until=...))
                    await session.commit()
                return True
            return False
        except Exception:
            # Redis down → fallback на PG
            async with self.session_factory() as session:
                existing = await session.get(IdempotencyKeyTable, key)
                if existing is None:
                    session.add(IdempotencyKeyTable(key=key, response={}, ttl_until=...))
                    await session.commit()
                    return True
                return False

    async def store_response(self, key: str, response: dict):
        try:
            await self.redis.set(f"idem:{key}", json.dumps(response), ex=86400)
        except Exception:
            pass   # не критично

    async def get_cached(self, key: str) -> dict | None:
        try:
            cached = await self.redis.get(f"idem:{key}")
            if cached: return json.loads(cached)
        except Exception:
            pass
        # Fallback PG
        async with self.session_factory() as session:
            row = await session.get(IdempotencyKeyTable, key)
            return row.response if row else None
```

> **Подводный камень №2 (SET NX — MUST):** Всегда `SET NX EX`, **не** `SET`. `SET` перезапиет. `SET NX` — только один из N параллельных вызовов «входит».
>
> **Подводный камень №3 (dual-write ordering):** `SET NX` → commit PG **после**. Если crash между — «ключ в Redis, но не в PG». OK для PoC (Redis-ключ 24h, PG — «дополнительная» страховка). Для строгости можно реверс (PG → Redis), но это «медленнее» (2 roundtrip'а в PG).
>
> **Подводный камень №4 (Redis TTL 24h):** 24 часа — «разумный» (client не будет «ждать» сутки). Коротче (1h) — клиент с retry после 1h получит new (не idem). 24h — баланс.

### 3.4. Формулировка «idempotent response»

Каждый Intake-запрос → **один** ответ. При идемпотентном повторе — **тот же самый** ответ (body + status). Это важно: Client может использовать `payment_id` из первого ответа — повтор вернёт тот же.

```python
# services/intake/api.py
@app.post("/v1/payments", status_code=202)
async def create_payment(req: CreatePaymentRequest,
                         idem_key: str | None = Header(default=None, alias="Idempotency-Key")):
    if not idem_key:
        return JSONResponse(status_code=400, content={"error":"Idempotency-Key required"})

    # 1. Идемпотентность?
    if not await idem_service.acquire(idem_key):
        cached = await idem_service.get_cached(idem_key)
        if cached: return JSONResponse(status_code=200, content=cached)
        # Ключ был, но response «потерян» (Redis crash) — retry:
        return JSONResponse(status_code=409, content={"error":"already processing"})

    # 2. Валидация (Pydantic уже проверил при parse)
    cmd = req.to_command()

    # 3. Publish → Kafka `payments.commands`
    try:
        await kafka.publish("payments.commands", key=cmd["payment_id"], value=cmd)
    except Exception as e:
        # Kafka down — rollback идемпотентности (ключ «снимаем», клиент может retry)
        await idem_service.rollback(idem_key)
        return JSONResponse(status_code=503, content={"error":"kafka unavailable"})

    # 4. Cache response (для идемпотентного повтора)
    response = {"status":"accepted","payment_id":cmd["payment_id"],
                "account_id":req.account_id}
    await idem_service.store_response(idem_key, response)
    return JSONResponse(status_code=202, content=response)
```

**Ключевые детали:**
- `202 Accepted` — «приняли, будем работать» (не `200`, т.к. async).
- **Kafka publish → cache response**. Если publish упал — `rollback` идемпотентности (клиент может retry, не потеряет).
- `409 Conflict` — «уже обрабатывается, не могу гарантировать ответ» (редкий случай: Redis crash между acquire и store).

> **Подводный камень №5 (status 202 vs 200):**
> - `202 Accepted` — «приняли, работаем» (async). **Использовать для Create** (Oркестратор будет работать).
> - `200 OK` — «готово» (sync). Использовать для GET-запросов статуса.
> - Если client ожидает `200` для idempotent retry — возвращаем `200` + cached response (из кэша). Это **дополнительно** к первоначальному `202`.

---

## 4. Producer `payments.commands`

```python
# services/intake/infrastructure/producer.py (использует shared.kafka.KafkaProducer)
# (Код — см. Этап 1, shared/kafka.py)

# В Intake:
kafka = KafkaProducer(brokers=settings.kafka_brokers)
# При publish — retry + backoff + jitter + DLQ (shared уже реализует)
```

**Payload** (соответствует §3.11 TЗ v2):
```json
{
  "cmd": "Create",
  "payment_id": "...",
  "amount": {"amount": 100, "currency": "USD"},
  "account_id": "A1",
  "client_ref": "C-123"
}
```

> **Подводный камень №6 (partition key):** `key=payment_id` — одна partition. Все платежи с одним `payment_id` — в одну partition (ordering). Для разных платежей — распределение по partitions (parallelism).

---

## 5. Коммиты Этапа 4

| # | Тип | Сообщение |
|---|---|---|
| 1 | `feat(intake)` | add DTOs (CreatePaymentRequest/Response) |
| 2 | `feat(intake)` | add idempotency service (Redis SET NX + PG fallback) |
| 3 | `feat(intake)` | add `payments.commands` producer |
| 4 | `feat(intake)` | add POST /v1/payments + GET /v1/payments/{id} |
| 5 | `feat(intake)` | add rollback on kafka failure |
| 6 | `test(intake)` | add idempotency unit tests (dual requests, Redis down) |

**Body (пример):**
```
feat(intake): add idempotency service (Redis SET NX + PG fallback)

Why:
  TЗ §3.1 — Idempotency: middleware в Gateway проверяет Redis/БД.
  TЗ §2.4 — Intake генерирует идемпотентные ключи.

What:
  - IdempotencyService.acquire() — SET NX EX 24h (Redis), PG fallback
  - IdempotencyService.store_response/get_cached — cache + PG
  - IdempotencyService.rollback — on kafka failure (client может retry)
  - Table: intake.idempotency_keys (key PK, response, created_at, ttl_until)

Design notes:
  - SET NX = атомарность (1 из N parallel «входит»)
  - Dual-write Redis + PG = resilience (Redis down → PG)
  - 24h TTL = «разумный» (кратчайший client retry window)

Refs: doc/tz_v2.md §3.1, §4, ADR-005 (Redis idempotency)
```

---

## 6. Как проверить (exit criteria)

```
make up

# 1-й request:
curl -X POST http://localhost:8001/v1/payments \
     -H 'Idempotency-Key: K1' -H 'Content-Type: application/json' \
     -d '{"amount":100,"currency":"USD","account_id":"A1"}'
# → 202 {"status":"accepted","payment_id":"P123",...}

# 2-й request (same K1): SHOULD БЫТЬ cached response (same payment_id!):
curl -X POST http://localhost:8001/v1/payments \
     -H 'Idempotency-Key: K1' -H 'Content-Type: application/json' \
     -d '{"amount":100,"currency":"USD","account_id":"A1"}'
# → 200 {"status":"accepted","payment_id":"P123",...}   (тот же!)

# Kafka: должно быть ТОЛЬКО 1 сообщение:
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic payments.commands \
  --max-messages 10
# → 1 сообщение (2-й не создан)

# Redis:
docker compose exec redis redis-cli GET idem:K1
# → {"status":"accepted","payment_id":"P123",...}
```

**Красные флаги:**
- [ ] 2-й request → 2-й `payment_id` (идемпотентность сломана).
- [ ] Kafka: 2 сообщения (dual write problem!).
- [ ] `Idempotency-Key` — нет в header → 200 (должен быть 400).
- [ ] Redis down → 500 (должен быть PG fallback).

---

## 7. Типичные ошибки

| Ошибка | Симптом | Как избежать |
| --- | --- | --- |
| `SET` вместо `SET NX` | Race: оба параллельных прошли | Только `SET NX EX` |
| Cache response «до» publish | Client получает 202, но `payment_id` не реальный | Publish → THEN cache |
| `400` при retry (Redis down) | Неясно, новый ли | PG fallback + `409` (ambiguous) |
| Status 200 для `Create` | Client «думает» уже готово | `202 Accepted` для async |
| `Idempotency-Key` — not required | Каждый request новый | `400` при отсутствии |

---

## 8. Чек-лист перед merge Этапа 4

- [ ] `make up` → все UP.
- [ ] 1-й request → 202 + `payment_id`.
- [ ] 2-й request (same key) → 200 + **тот же** `payment_id`.
- [ ] Kafka: 1 сообщение (не 2).
- [ ] Redis: кэш.
- [ ] PG: `intake.idempotency_keys` — 1 row.
- [ ] 6 коммитов в `feat/05-intake`.

---

## 9. Outbox Delivery Tracker (закрытие gap Этапа 2)

Этап 2 (`02_ledger_write_side.md`) оставил колонки `status` / `published_at` /
`last_error` в `outbox_events`, задокументировал `PENDING / PUBLISHED / FAILED`,
но **не поставил** компонент, который переключал бы их. В Этапе 4
(`feat/05-intake`) закрываем этот gap, чтобы жизненный цикл outbox был
полным до read-side работы.

**Что делает.** `services/ledger/application/delivery_tracker.py` —
фоновый цикл (запускается в lifespan сервиса ledger, `LEDGER_ENABLE_TRACKER=1`
по умолчанию) + ручной endpoint. На каждой итерации (`sweep_once()`):

1. читает последние записи на целевом topic Kafka (`ledger.events`, константа
   `TOPIC_LEDGER_EVENTS` из `use_cases.py`), собирает их `event_id`;
2. по каждой строке `outbox_events.status = 'pending'`:
   * `event_id` в собранном множестве → `status='published'`,
     `published_at=now()`, `last_error=NULL`;
   * строка старше `horizon_seconds` (по умолчанию 300с) и НЕ в множестве →
     `status='failed'`, `last_error='no delivery after Ns (topic=...)'`;
   * иначе → остаётся `pending` (Debezium ещё не забрал).

**Намеренно просто.** Только «последние записи на topic» — без полного
WAL-скана и bookkeeping offsets. Для PoC достаточно; production-grade можно
добавить в Этапе 7–8 (DLQ / retries).

**Ручные поверхности (ops / smoke).**

| Метод | Путь | Описание |
| --- | --- | --- |
| `POST` | `/v1/outbox/sweep` | один прогон `sweep_once()`, возвращает счётчики |
| `GET` | `/v1/outbox?limit=N` | последние N строк outbox с `status`/`published_at`/`last_error` |

**Тесты (без живого Kafka).** `tests/ledger/test_delivery_tracker.py` —
monkeypatch `_detect_delivered_event_ids` фиксированным множеством и
проверяем transitions: `pending→published` (seen), `pending→failed`
(age>horizon, unseen), `pending` остаётся (age<horizon, unseen), sweep
не трогает непиндинги, инъекция детекции short-circuit'ит Kafka. 5 тестов.

> **Примечание.** Компонент живёт в `services/ledger/` (не в `services/intake/`)
> — это **закрытие gap Этапа 2**, не новая функциональность Этапа 4. В §1
> (scope) Этапа 4 tracker не заявлен; здесь он задокументирован как
> сопутствующая работа в ветке `feat/05-intake`.

> **После merge в `main`** — открывать Этап 5 (`05_read_side_sse.md`).
