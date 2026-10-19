# Этап 3. Orchestrator — Saga Orchestration

> **Ветка:** `feat/04-saga`
> **Результат:** happy-path `Create→Success`; failure-path при `POST /fraud/admin/fail {mode:errors}` → CB open, фрод-проверки в DLQ, валидные ≥10 TPS. Компенсации при ошибке `Debit`.
> **Чему учимся здесь:** как устроена Saga Orchestration: state machine, шаги с CB/DLQ, компенсации, хранение состояния, идемпотентность.

---

## 1. Почему Sagа Orchestration (а не Chores/2PC)

**2PC** (Two-Phase Commit) — блокирует ресурсы на всех узлов, требует «все ли согласны» — **не подходит** для микросервисов (блокировки, не отказоустойчиво).

**Choreography** — каждый сервис «слышит» события соседнего и сам решает «следующий шаг». Гибко, но **сложно отследить состояние** и **невозможна** централизованная компенсация.

**Orchestration** (это наш выбор, §1 TЗ v2) — **Оркестратор** управляет:
1. Состоянием саги (`Created → Checking → Reserve → Fraud → Debit → Success`).
2. Отправляет **команды** соседним сервисам (`ledger.commands` → `Reserve`, `Debit`).
3. Слушает **события** (`ledger.events` → `FundsReserved`).
4. При ошибке — **компенсирует** (цепочка `CancelReservation → RolledBack`).

**Оркестратор — единственная точка контроля**. Каждый шаг — отдельный сервис, но «кто первый, кто следующий, что делать если упало» — решает Оркестратор.

---

## 2. State Machine саги

### 2.1. Шаги (§3.4 TЗ v2)

```
payments.commands (Create)
       │
       ▼
  Created ──→ Checking ──→ Reserve ──→ Fraud ──→ Debit ──→ Success
                  │           │          │          │
                  ▼           ▼          ▼          ▼
              (LimitsOK)   (FundsRes)  (Verdict)  (DebitOK)
                                                    │
              Any failure ─────────────────────────►  Failed
                                                    │
              Reservation made + Debit failed ─────►  RolledBack
```

### 2.2. Python-statemachine

```python
# services/orchestrator/domain/saga_fsm.py
from statemachine import StateMachine, State

class SagaState(State):
    def __init__(self, machine):
        super().__init__(machine)

class SagaStateMachine(StateMachine):
    created   = State("Created", initial=True)
    checking  = State("Checking")
    reserve   = State("Reserve")
    fraud     = State("Fraud")
    debit     = State("Debit")
    success   = State("Success", final=True)
    failed    = State("Failed", final=True)
    rolledback= State("RolledBack", final=True)

    # Transitions
    start_checking   = created.to(checking)
    move_to_reserve  = checking.to(reserve,  conditions=["limits_ok"])
    move_to_fraud    = reserve.to(fraud,     conditions=["reserved_ok"])
    move_to_debit    = fraud.to(debit,      conditions=["fraud_ok"])
    move_to_success  = debit.to(success,    conditions=["debit_ok"])
    # Compensations
    fail             = [checking, reserve, fraud, debit].to(failed)
    rollback         = [reserve, fraud, debit].to(rolledback)
    # Guards (устанавливаются вызывающим кодом)
    limits_ok = lambda: True
    reserved_ok = lambda: True
    fraud_ok = lambda: True
    debit_ok = lambda: True
```

> **Подводный камень №1 (guards по умолчанию TRUE):** Если забыл установить guard → transition всегда доступен. Рекомендую явно `conditions=["..."]` **и** явно `sm.limits_ok = lambda: <real_check>` перед `transition()`.

> **Подводный камень №2 (no state in FSM):** `sm2 = SagaStateMachine()` — всегда `created`. Восстановление из БД: `SagaStateMachine(starting_state=row.state)`.

### 2.3. `Saga` Aggregate

```python
class Saga(shared_aggregate):
    def __init__(self, saga_id, payment_id, state, history=None, compensation=None):
        super().__init__(); self.saga_id = saga_id
        self.payment_id = payment_id; self.state = state
        self.history = history or []     # [{step, cmd, event, ts, ok}]
        self.compensation = compensation or []   # цепочка компенсаций
        self.created_at = datetime.utcnow()

    def mark_step(self, step: str, cmd: str, event: str, ok: bool, err: str = ""):
        self.history.append({"step": step, "cmd": cmd, "event": event,
                             "ts": datetime.utcnow(), "ok": ok, "err": err})
        if ok:
            self.state = state_after[step]   # по маппингу §3.4
        else:
            self._add_compensation(step)
            self.state = "Failed"   # или "RolledBack"

    def _add_compensation(self, step: str):
        # При ошибке «впереди» — компенсируем «за собой»
        if step == "debit":
            self.compensation.append("CancelReservation")
            self.compensation.append("Refund")
        elif step == "fraud" and "reserve" in [h["step"] for h in self.history]:
            self.compensation.append("CancelReservation")
```

---

## 3. Saga Handler (consumer `payments.commands`)

**Основной поток:**
1. Подписывается на `payments.commands` (топик от Intake, Этап 4).
2. Для каждой команды `Create` → создаёт `Saga`, начинает состояние-машину.
3. Каждый «шаг» → отправка команды в соответствующий сервис.
4. Слушает `ledger.events` → реагирует на `FundsReserved`, `FundsDebited`.
5. Ошибки → CB (fraud) / DLQ / компенсация.

```python
# services/orchestrator/application/saga_handler.py
class SagaHandler:
    def __init__(self, saga_repo, ledger_producer, fraud_client, settings):
        self.repo = saga_repo
        self.ledger = ledger_producer      # shared.kafk.KafkaProducer (DLQ)
        self.fraud = fraud_client          # shared.circuit.CircuitBreakerHttpClient
        self.s = settings
        self.payment_sm_cache = {}         # payment_id → PaymentStateMachine (in-mem)

    async def on_payment_created(self, payload: dict):
        """Consumer `payments.commands` cmd=Create."""
        saga = Saga(saga_id=str(uuid.uuid4()), payment_id=payload["payment_id"], state="Created")
        await self.repo.save(saga)

        # Шаг 1: CheckLimits (в ledger — это read-side, но для PoC упрощаем)
        await self._step(saga, step="check", cmd="CheckLimits", topic="ledger.commands",
                         payload={**payload, "cmd": "CheckLimits"})

    async def on_funds_reserved(self, payload: dict):
        """Consumer `ledger.events` event=FundsReserved."""
        saga = await self.repo.get_by_payment(payload["payment_id"])
        sm = SagaStateMachine(starting_state=saga.state)
        sm._SagaStateMachine__reserved_ok = lambda: True
        sm.move_to_fraud()
        saga.state = "Fraud"
        await self.repo.save(saga)
        await self._fraud_step(saga)

    async def _fraud_step(self, saga: Saga):
        """Шаг Fraud через CircuitBreaker."""
        try:
            result = await self.fraud.post(
                f"{self.s.fraud_url}/fraud/check",
                json={"payment_id": saga.payment_id, "amount": saga.payload["amount"]})
            ok = result.get("decision") == "approve"
        except CircuitBreakerError as e:
            # CB open → fallback: платёж → DLQ, Оркестратор живёт
            await self._send_to_dlq(saga, reason="circuit_open", err=str(e))
            saga.state = "Failed"
            await self.repo.save(saga)
            return
        saga.mark_step("fraud", cmd="FraudCheck", event="FraudVerdict", ok=ok,
                       err="" if ok else result.get("reason", ""))
        if ok:
            await self._step(saga, step="debit", cmd="Debit", topic="ledger.commands",
                             payload={...})
        else:
            saga.state = "Failed"
            await self.repo.save(saga)
            await self._send_to_dlq(saga, reason="fraud_declined",
                                    err=result.get("reason", ""))

    async def _send_to_dlq(self, saga, reason, err):
        """Платёж в DLQ — не теряется, но не обрабатывается."""
        await self.ledger.publish(
            topic="ledger.commands.dlq",   # DLQ (отдельный топик)
            key=saga.payment_id,
            value={"saga_id": saga.saga_id, "payment_id": saga.payment_id,
                   "reason": reason, "err": err, "state": saga.state})
        # Валидные платежи продолжают обрабатываться (CB не блокирует весь поток)

    async def on_debit_failed(self, payload: dict):
        """Consumer `ledger.events` event=DebitFailed."""
        saga = await self.repo.get_by_payment(payload["payment_id"])
        saga._add_compensation("debit")
        # Отправка компенсаций
        for cmd in saga.compensation:
            await self.ledger.publish(topic="ledger.commands", key=saga.payment_id,
                                      value={"cmd": cmd, **saga.payload})
        saga.state = "RolledBack"
        await self.repo.save(saga)

    async def _step(self, saga, step, cmd, topic, payload):
        saga.mark_step(step, cmd, "in_flight", ok=True)
        saga.state = state_after[step]
        await self.repo.save(saga)
        await self.ledger.publish(topic=topic, key=saga.payment_id, value=payload)
```

> **Подводный камень №3 (consumer'ы на 2 топака):** Saga Handler подписан на **два** топака: `payments.commands` (вход) и `ledger.events` (ответы). Два consumer-цикла (asyncio tasks) в одном сервисе.
>
> **Подводный камень №4 (in-memory state):** `payment_sm_cache` — in-mem. Если Оркестратор перезапустится — state «потерян» (нужно восстановить из БД). **Это ок для PoC** — в проде state должен быть в БД (у нас `saga.state`).
>
> **Подводный камень №5 (CB + fallback):** Если CB открыт — **все** fraud-вызовы сразу «fail» (быстро, без ожиданий). Fallback должен **немедленно** отправить платёж в DLQ и «отпустить» поток. **Не ждите** retry — CB открыт, все retry'ы тоже fail. Это норм.

### 3.1. `CircuitBreakerHttpClient` для Fraud

```python
# services/orchestrator/infrastructure/fraud_client.py
def fraud_fallback(url, payload, err):
    # Платёж в DLQ — не падает, не блокирует поток
    log.warning("CB open, payment → DLQ: %s", payload["payment_id"])
    return {"decision": "route_to_dlq"}

fraud_client = CircuitBreakerHttpClient(
    fallback=fraud_fallback, fail_max=5, reset_timeout=30)
```

> **Подводный камень №6:** `fail_max=5` — 5 подряд ошибок → CB opens. Но «подряд» значит: 1 OK → счётчик сбрасывается. Если fraud «падает на 1 из 10» — CB не откроется. Для PoC это ок. Для строгости `fail_max=3` + `reset_timeout=10`.

### 3.2. Compensations

```python
# services/orchestrator/application/compensation.py
COMPENSATIONS = {
    "debit":      ["CancelReservation", "Refund"],   # отмена reserve + возврат
    "fraud":      ["CancelReservation"],              # отмена reserve
    "reserve":    [],                                 # нечего компенсировать (reserve ещё не подтверждён)
    "check":      [],                                 # limits-check не изменяет состояние
}
```

> **Подводный камень №7 (порядок компенсаций):** Компенсации идут **в обратном** порядке от последнего успешного шага. Пример: `debit` упал → компенсируем `reserve` (`CancelReservation`). Если и `reserve` не было (`check` упал) → компенсация не нужна.
>
> **Подводный камень №8 (refunds):** `Refund` — «вернули деньги». В PoC упрощаем: `CancelReservation` достаточно (снять réserve). Реальный refund — только если `debit` реально прошёл.

---

## 4. Storage: `saga_state` БД

```python
# services/orchestrator/infrastructure/models.py
class SagaRow(Base):
    __tablename__ = "sagas"
    __table_args__ = {"schema": "orchestrator"}
    id            = Column(String(36), primary_key=True)
    payment_id    = Column(String(36), unique=True, index=True)
    state         = Column(String(32), default="Created")
    history       = Column(JSON)              # list of steps
    compensation  = Column(JSON)              # list of pending compensations
    payload       = Column(JSON)              # исходный payload (amount, currency)
    created_at    = Column(DateTime(timezone=True), server_default=func.now())
    updated_at    = Column(DateTime(timezone=True), onupdate=datetime.utcnow)
```

**Key:** `payment_id` **unique** (1 payment = 1 saga). Повторный `Create` с тем же `payment_id` → `ON CONFLICT DO NOTHING`.

---

## 5. Kafka consumer'ы (2 цикла)

```python
# services/orchestrator/kafka_consumer.py
async def run_consumers(handler: SagaHandler, s: Settings):
    # Consumer 1: payments.commands (Create)
    c1 = KafkaConsumer(s.kafka_brokers, group="orchestrator", topics=["payments.commands"])
    # Consumer 2: ledger.events (FundsReserved, FundsDebited, DebitFailed)
    c2 = KafkaConsumer(s.kafka_brokers, group="orchestrator", topics=["ledger.events"])
    await c1.start(); await c2.start()

    async def loop1():
        async for key, payload in c1.stream():
            if payload["cmd"] == "Create":
                await handler.on_payment_created(payload)
    async def loop2():
        async for key, payload in c2.stream():
            if payload["event"] == "FundsReserved":
                await handler.on_funds_reserved(payload)
            elif payload["event"] == "DebitFailed":
                await handler.on_debit_failed(payload)

    await asyncio.gather(loop1(), loop2())
```

> **Подводный камень №9 (group_id):** Оба consumer'а в одной группе `orchestrator`. Если бы разные `group_id` — 2 независимые группы, OK. Если один — partition'ы делятся между ними. Для PoC **разные** `group_id` (orchestrator-cmds, orchestrator-evt) — проще.

---

## 6. HTTP API

```python
# services/orchestrator/main.py
@app.get("/v1/sagas/{saga_id}")
async def get_saga(saga_id: str):
    row = await repo.get(saga_id)
    return {"id": row.id, "payment_id": row.payment_id, "state": row.state,
            "history": row.history, "compensation": row.compensation}

@app.get("/v1/sagas/by-payment/{payment_id}")
async def get_saga_by_payment(payment_id: str): ...
```

**Для ручного теста:**
```
curl http://localhost:8002/v1/sagas/{saga_id}
# {
#   "id": "...", "payment_id": "P1", "state": "Success",
#   "history": [
#     {"step":"check","cmd":"CheckLimits","ts":"...","ok":true},
#     {"step":"reserve","cmd":"Reserve","ts":"...","ok":true},
#     {"step":"fraud","cmd":"FraudCheck","ts":"...","ok":true},
#     {"step":"debit","cmd":"Debit","ts":"...","ok":true}],
#   "compensation": []
# }
```

---

## 7. Integration тесты (happy + failure)

```python
# tests/orchestrator/test_saga_e2e.py
import pytest
from testcontainers.postgres import PostgresContainer
from testcontainers.kafka import KafkaContainer   # or reuse shared docker

@pytest.mark.asyncio
async def test_saga_happy_path(shared_env):
    """Create → Success (все шаги OK)."""
    # 1. Создаём payment через Intake (Этап 4) или напрямую в Kafka:
    await producer.publish("payments.commands", "P100", {
        "cmd": "Create", "payment_id": "P100",
        "amount": {"amount":100, "currency":"USD"}, "account_id":"A1"})
    # 2. ждём 5s (или по событию), проверяем saga:
    saga = await get_saga_by_payment("P100")
    assert saga["state"] == "Success"
    # 3. Баланс A1 убавился:
    bal = await get_balance("A1")
    assert bal == initial_balance - 100

@pytest.mark.asyncio
async def test_saga_fraud_failure(shared_env):
    """Create → Fraud fail → DLQ + CB open, но валидные обрабатываются."""
    # 1. «Сломаем» Fraud API:
    await http.post("http://mock-fraud:8007/fraud/admin/fail",
                    json={"mode":"errors"})
    # 2. Отправляем 20 платежей (10 валидных, 10 «fraud fail»):
    for i in range(10):
        await producer.publish("payments.commands", f"P200{i}", {...})
    # 3. Ждём, проверяем:
    #    - CB open (metric pybreaker_open=1)
    #    - 10 платежей в DLQ (ledger.commands.dlq)
    #    - 0 в Success (все fraud fail)
    #    - Валидные (без fraud) — continue
```

**Testcontainers:** PG + Kafka + Redis — 3 контейнера на каждый тест. `docker-compose.test.yml` — изолированный.

> **Подводный камень №10 (Testcontainers на Win):** Docker Desktop on Windows — slow start (+30s на первый контейнер). Кэш образов в `docker-compose.test.yml` — следующий запуск быстрее.

---

## 8. Коммиты Этапа 3

| # | Тип | Сообщение |
|---|---|---|
| 1 | `feat(orchestrator)` | add Saga aggregate + state machine (7 states, guards) |
| 2 | `feat(orchestrator)` | add SagaHandler (consumer payments.commands, dispatch) |
| 3 | `feat(orchestrator)` | add CB-protected fraud step + DLQ fallback |
| 4 | `feat(orchestrator)` | add compensation chain (debit→CancelRes→Refund) |
| 5 | `feat(orchestrator)` | add saga_status query API |
| 6 | `feat(orchestrator)` | add consumer `ledger.events` (on_reserved, on_debit_failed) |
| 7 | `feat(orchestrator)` | add saga persistence (SagaRow, repository) |
| 8 | `test(orchestrator)` | add saga integration tests (happy + fraud-failure) |

**Body (пример):**
```
feat(orchestrator): add CB-protected fraud step + DLQ fallback

Why:
  TЗ §3.6 — Circuit Breaker: при падении Fraud API — не обрушить Оркестратор.
  TЗ §7 — From Failure: CB open → DLQ, valid ≥10 TPS continue.

What:
  - FraudClient extends CircuitBreakerHttpClient (pybreaker,
    fail_max=5, reset_timeout=30)
  - Fallback: immediate DLQ publish + continue (no blocking)
  - Metric: pybreaker_open gauge (для Grafana)
  - DLQ: separate topic `ledger.commands.dlq` (payload + reason + err)

Refs: doc/tz_v2.md §3.6, §4, ADR-004 (CB library)
```

---

## 9. Как проверить (exit criteria)

```
make up
# Happy path:
curl -X POST http://localhost:8001/v1/payments \
     -H 'Idempotency-Key: K1' -H 'Content-Type: application/json' \
     -d '{"payment_id":"P1","amount":{"amount":100,"currency":"USD"},"account_id":"A1"}'
# (Intake, Этап 4 — здесь только placeholder)
curl http://localhost:8002/v1/sagas/by-payment/P1
# → "state":"Success", history 4 шага

# Failure path:
curl -X POST http://localhost:8007/fraud/admin/fail -d '{"mode":"errors"}'
# Отправляем 20 платежей → CB open, 10 в DLQ
curl -s http://localhost:9090/api/v1/query?query=pybreaker_open
# → {"value":"1"} (CB open)
# Проверка DLQ:
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic ledger.commands.dlq \
  --from-beginning --max-messages 1
# → {"saga_id":"...","payment_id":"...","reason":"circuit_open",...}
```

**Красные флаги:**
- [ ] Happy path: saga не «Success».
- [ ] Fraud fail: CB не open.
- [ ] DLQ — нет сообщений.
- [ ] Валидные платежи — остановлены (CB блокирует весь поток).
- [ ] Integration тесты — не pass на Testcontainers.

---

## 10. Типичные ошибки

| Ошибка | Симптом | Как избежать |
| --- | --- | --- |
| CB открывает «на всё» | Валидные платежи останавливаются | CB только на **Fraud** (отдельный клиент), не на «всё» |
| Compensations «пропущены» | Деньги «застряли» в reserve | `saga.compensation` — явный список, проверяется в UI |
| Consumer 2 не подхватывает | Reserve не переходит в Fraud | 2 consumer-цикла (asyncio.gather) |
| `group_id` 1 на 2 consumer'а | 1 partition → 1 consumer | Разные `group_id` |
| Saga state в in-mem, потеряно после restart | Restart → «Created» | State в БД (`saga.state`), восстанавливаем через `starting_state=` |

---

## 11. Чек-лист перед merge Этапа 3

- [ ] `make up` → все UP.
- [ ] Happy path: saga `Success`, 4 шага в history, баланс −100.
- [ ] Fraud fail: CB open (Prometheus metric=1), 10+ в DLQ, валидные continue.
- [ ] `test_saga_happy_path` pass.
- [ ] `test_saga_fraud_failure` pass.
- [ ] 8 коммитов в `feat/04-saga`.

> **После merge в `main`** — открывать Этап 4 (`04_intake_idempotency.md`).
