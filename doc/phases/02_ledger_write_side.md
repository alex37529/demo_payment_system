# Этап 2. Ledger — Write-Side (ядро + Transactional Outbox)

> **Ветка:** `feat/03-ledger`
> **Результат:** `POST /ledger/v1/accounts/{id}/debit` → баланс убавился в БД + строка в `outbox_events` + сообщение в Kafka `ledger.events`. Unit-тесты агрегатов — 100% переходов.
> **Чему учимся здесь:** как устроен Write-Side в CQRS: агрегаты с инвариантами, use-cases, Transactional Outbox (BД + событие в одной транзакции), и как Debezium разносит это event в Kafka.

---

## 1. Что такое Write-Side в CQRS

**CQRS** (§3.8 TЗ) — разделение команд (запись) и запросов (чтение). Write-Side отвечает **только** на команды: «списать X с счёта Y». Он **не** отвечает на запросы «какой баланс?» — это делает Read-Side (Этап 5, Profile Service).

**Почему?** Запись и чтение имеют разные требования:
- **Запись** — строгая согласованность, инварианты, идемпотентность, транзакции.
- **Чтение** — скорость, дублирование проекций (балансы в `profile`, лента в `notification`).

Разделив их, мы можем масштабировать чтение независимо (Replica×10, а запись×1).

**Здесь (Этап 2)** мы строим Write-Side:
- **Агрегаты** с инвариантами (DDD).
- **Use-cases** (оркестрация домена).
- **Transactional Outbox** — событие записывается в той же транзакции, что и данные (§3.2 TЗ v2).
- **Debezium** — читает WAL → публикует события в Kafka.

---

## 2. Ключевая идея: One TX = data + outbox

Рисунок-схема:
```
UseCase_Debit (session = AsyncSession, TX open)
   ├─ account = repo.get(account_id)       # SELECT
   ├─ account.debit(Money(100,USD))         # в памяти: balance -= 100 (инварианты внутри)
   ├─ repo.save(account)                    # UPDATE accounts
   ├─ outbox.add(OutboxEvent(topic=ledger.events, payload=FundsDebited...))   # INSERT outbox_events
   └─ session.commit()                      # ★ АТОМАРНО: UPDATE + INSERT
                                                 │
                                                 ▼
                                       WAL Postgres (log)
                                                 │
                                                 ▼
                                       Debezium (CTE на WAL)
                                                 │
                                                 ▼
                                       Kafka: ledger.events
```

**Ключ:** `session.commit()` коммитит **оба** изменения (UPDATE accounts + INSERT outbox_events). Если crash при commit — откат. Если crash после commit — оба изменения в БД. Debezium подхватит `outbox_events` (insert).

> **Подводный камень №1 (Dual Write):** Если бы мы делали сначала `session.commit()` (баланс), а потом `kafka.publish(outbox row)` — это ДВА независимых действия. Crash между ними = баланс изменён, Kafka-событие потеряно. **Outbox-паттерн убирает это**: событие и данные — одна TX.

---

## 3. Домен: Агрегаты и инварианты

### 3.1. `Money` (Value Object)

```python
# services/ledger/domain/ledger.py
from shared.domain.base import ValueObject

class Money(ValueObject):
    def __init__(self, amount: int, currency: str):
        if amount < 0: raise ValueError("amount must be >= 0")
        if currency not in ("USD", "EUR"): raise ValueError(f"unsupported currency {currency}")
        self.amount = amount
        self.currency = currency
    def to_dict(self): return {"amount": self.amount, "currency": self.currency}
```

### 3.2. `Account` (Aggregate Root)

```python
class Account(shared_aggregate):
    def __init__(self, account_id: str, owner_name: str, currency: str, balance: Money):
        super().__init__(); self.account_id = account_id
        self.owner_name = owner_name; self.currency = currency
        self.balance = balance
        self.reserved = Money(0, currency)   # выделен, но ещё не списан (reserve-шаг)
        self._version = 1                    # optimistic lock

    # --- команды, доступные только через агрегат (инварианты) ---

    def debit(self, amount: Money):
        if amount.currency != self.currency:
            raise InsufficientFundsError("currency mismatch")
        if self.balance.amount < amount.amount:
            raise InsufficientFundsError(f"need {amount}, have {self.balance}")
        self.balance = Money(self.balance.amount - amount.amount, self.balance.currency)
        self._bump()
        self.add_event(FundsDebited(account_id=self.account_id,
                                     payment_id=get_current_payment_id(),
                                     amount=amount.to_dict(),
                                     new_balance=self.balance.to_dict()))

    def reserve(self, amount: Money):
        if amount.currency != self.currency: raise ValueError("currency")
        if self.balance.amount < amount.amount + self.reserved.amount:
            raise InsufficientFundsError("reserved + debit exceeds balance")
        self.reserved = Money(self.reserved.amount + amount.amount, self.currency)
        self._bump()
        self.add_event(FundsReserved(account_id=self.account_id,
                                      payment_id=get_current_payment_id(),
                                      amount=amount.to_dict()))

    def cancel_reservation(self, amount: Money):
        if amount.amount > self.reserved.amount: raise ValueError("reserved < X")
        self.reserved = Money(self.reserved.amount - amount.amount, self.currency)
        self._bump()
        self.add_event(CancelReservationPosted(account_id=self.account_id,
                                                payment_id=get_current_payment_id(),
                                                amount=amount.to_dict()))

    def refund(self, amount: Money):   # «вернули назад» (кросс-списания)
        if amount.currency != self.currency: raise ValueError("currency")
        self.balance = Money(self.balance.amount + amount.amount, self.currency)
        self._bump()
        self.add_event(RefundPosted(...))

    def _bump(self): self._version += 1
```

**Ключевые инварианты:**
1. `balance ≥ 0` — никогда не «долг» (в PoC).
2. `balance ≥ amount` при `debit` (§3.10 TЗ v2).
3. `balance ≥ reserved + debit` при `reserve`.
4. **Currency match**: списание только со счёта своей валюты (§3.10 v2, ADR-003).
5. Изменение — **только через методы агрегата** (никаких `account.balance = ...` снаружи).

### 3.3. `Payment` (Aggregate Root)

```python
class Payment(shared_aggregate):
    def __init__(self, payment_id: str, amount: Money, account_id: str, idem_key: str):
        super().__init__(); self.payment_id = payment_id
        self.amount = amount; self.account_id = account_id
        self.idem_key = idem_key
        self.state = "Created"      # синхронизирован с PaymentStateMachine
        self.created_at = datetime.utcnow()

    def transition(self, new_state: str):
        VALID = {"Created": ["Validating"], "Validating": ["Processing"],
                 "Processing": ["Success","Failed","RolledBack"]}
        if new_state not in VALID.get(self.state, []):
            raise IllegalTransitionError(f"{self.state} → {new_state} not allowed")
        self.state = new_state
```

> **Подводный камень №2 (state + SM):** У нас **две** модели «состояния платежа»: `Payment.state` (строка в БД) и `PaymentStateMachine` (в shared). Чтобы не было рассинхрона — `set_state` только через `PaymentStateMachine`. На Этапе 3 (сaga) `sm.current` = source of truth, `payment.state` = проекция.

### 3.4. `DomainEvent`s (ledger)

См. `shared/domain/events.py` (Этап 1). Изменения здесь: `FundsDebited(FundsReserved, CancelReservationPosted, RefundPosted)` — уже описаны.

---

## 4. Use-cases (application layer)

### 4.1. Структура

```
services/ledger/
├── main.py                    # FastAPI app + /healthz + endpoints
├── domain/ledger.py           # Account, Payment, Money
├── application/
│   ├── use_cases.py           # UseCase_Debit, UseCase_Reserve, ...
│   └── context.py             # current_payment_id (contextvar)
├── infrastructure/
│   ├── models.py              # AccountRow, PaymentRow, OutboxEventRow
│   └── repository.py          # SQLAlchemyAccountRepository, ...
└── kafka_consumer.py          # consumer ledger.commands → use cases
```

### 4.2. `UseCase_Debit`

```python
# services/ledger/application/use_cases.py
from typing import AsyncContextManager
from shared.outbox import OutboxMixin
from shared.domain.events import FundsDebited
from services.ledger.domain.ledger import Money, InsufficientFundsError

class UseCase_Debit(OutboxMixin):
    def __init__(self, repo, session_factory):
        self.repo = repo; self.session_factory = session_factory

    async def execute(self, account_id: str, amount: Money, payment_id: str) -> dict:
        async with self.session_factory() as session:
            try:
                account = await session.get(AccountRow, account_id)
                if account is None: raise NotFoundError(f"account {account_id}")
                agg = Account.from_row(account)   # mapping Row → Aggregate
                agg.debit(amount)                   # ★ инварианты внутри агрегата
                to_row = agg.to_row()               # Aggregate → Row
                for k, v in to_row.items(): setattr(account, k, v)
                self._publish_outbox(session, "ledger.events", agg.pull_events()[-1])
                # ★ outbox строка — в той же TX
                await session.commit()
            except InsufficientFundsError as e:
                await session.rollback()
                raise InsufficientFundsErrorAPIError(str(e)) from e
        return {"payment_id": payment_id, "account_id": account_id, "amount": amount.to_dict()}
```

**Ключевые правила:**
1. **Вход:** use-case получает `session_factory` — каждая операция = 1 TX.
2. **Aggregate в TX:** все изменения — через методы агрегата. После `agg.debit()` — `to_row()` → `session.commit()`.
3. **Outbox в TX:** `_publish_outbox()` добавляет строку в `outbox_events` **до** commit (commit — единственное, что меняет БД).
4. **Error → rollback:** любой доменной error (InsufficientFunds) → `session.rollback()`, наружу — HTTP 4xx.

### 4.3. `UseCase_Reserve`, `UseCase_CancelReservation`, `UseCase_Refund`

Аналогично. `Reserve` → `agg.reserve()`. `CancelReservation` → `agg.cancel_reservation()`. `Refund` → `agg.refund()`. Каждый пишет своё Event в outbox.

---

## 5. Infrastructure: SQLAlchemy models

```python
# services/ledger/infrastructure/models.py
from datetime import datetime
from sqlalchemy import String, BigInteger, DateTime, Integer, BigInteger
from shared.db.base import Base

class AccountRow(Base):
    __tablename__ = "accounts"          # schema: ledger
    __table_args__ = {"schema": "ledger"}
    id            = Column(String(64), primary_key=True)
    owner_name    = Column(String(255))
    currency      = Column(String(3))
    balance       = Column(BigInteger, nullable=False)   # cents!
    reserved      = Column(BigInteger, nullable=False, default=0)
    version       = Column(Integer, nullable=False, default=1)   # optimistic lock
    updated_at    = Column(DateTime(timezone=True), onupdate=datetime.utcnow)

class PaymentRow(Base):
    __tablename__ = "payments"
    __table_args__ = {"schema": "ledger"}
    id            = Column(String(36), primary_key=True)
    account_id    = Column(String(64), index=True)
    amount        = Column(BigInteger, nullable=False)
    currency      = Column(String(3), nullable=False)
    idem_key      = Column(String(128), unique=True, index=True)
    state         = Column(String(32), default="Created")
    saga_id       = Column(String(36), default=None)
    created_at    = Column(DateTime(timezone=True), server_default=func.now())
    updated_at    = Column(DateTime(timezone=True), onupdate=datetime.utcnow)

# OutboxEventRow — уже в shared/outbox.py (OutboxEvent)
```

> **Подводный камень №3 (balance в копейках):** `balance` — **BigInteger в минорных единицах** ($1 = 100 cents). Никаких `float` — ошибка округления при 1000 транзакций. `Money.amount = int` (cents). Это стандарт.

> **Подводный камень №4 (optimistic lock):** `version`-поле. В PoC не обязательно, но если 2 платежа списываются одновременно — один должен упасть с `VersionConflictError`. `WHERE version = :v` при UPDATE. В PoC можно отложить до Этапа 7 (load-тест покажет).

### 5.1. Repository

```python
# services/ledger/infrastructure/repository.py
from typing import Optional
class SQLAlchemyAccountRepository(IAccountRepository):
    def __init__(self, session_factory):
        self.session_factory = session_factory   # use-case владеет сессией
    async def get(self, account_id: str) -> Optional[Account]:
        async with self.session_factory() as session:
            row = await session.get(AccountRow, account_id)
            return Account.from_row(row) if row else None
    # save — use-case вызывает (не репозиторий)
```

> **Подводный камень №5 (session ownership):** Session — **владеет use-case**, не репозиторий. Один use-case = одна session = одна TX. Если репозиторий создаёт свою session — «потеря TX» (данные в разных TX → outbox-паттерн ломается).

---

## 6. Kafka consumer `ledger.commands`

Команды из Оркестратора: `Reserve`, `Debit`, `CancelReservation`, `Refund`. Каждая → use-case.

```python
# services/ledger/kafka_consumer.py
async def run_consumer(ledger_commands: str, use_cases: dict, brokers: str):
    consumer = KafkaConsumer(brokers, group="ledger", topics=[ledger_commands])
    await consumer.start()
    async for key, payload in consumer.stream():
        cmd = payload["cmd"]            # "Debit" | "Reserve" | ...
        uc = use_cases[cmd]             # UseCase_Debit / UseCase_Reserve / ...
        try:
            await uc.execute(**payload)
        except InsufficientFundsErrorAPIError:
            # Платёж в Failed — Оркестратор сам примет решение (compensate)
            log.warning("insufficient funds: %s", payload)
        except Exception:
            log.exception("unknown error")
            # DLQ — это на стороне shared.kafka (producer)
```

**Сема**: consumer **idempotent** (уникальный `payment_id` в БД). Повторное получение = no-op (уже обработано).

> **Подводный камень №6 (идемпотентность consumer):** Если Kafka «повторит» сообщение (rebalance, at-least-once) — use-case вызовется 2 раза. `debit(100)` дважды = двойное списание. **Защита:** unique index на `payments.idem_key` + `INSERT ... ON CONFLICT DO NOTHING`. Или `payment_id` + состояние. В PoC достаточно unique на `idem_key` + проверка «уже Success» → skip.

---

## 7. Debezium → Kafka

**Настрока** (`kafka/connect/debezium-ledger.json`):
```json
{
  "name": "ledger-outbox",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "fintech",
    "database.password": "fintech",
    "database.include.list": "ledger",
    "table.include.list": "ledger.outbox_events",
    "publication.name": "outbox_publication",
    "topic.prefix": "",
    "slot.name": "outbox_slot",
    "schema.name": "ledger"
  }
}
```

> **Подводный камень №7 (publication/slot):** Debezium PostgreSQL **требует** `wal_level=logical` **и** `publications + replication slot`.publication создаётся автоматом (`outbox_publication`), slot — `outbox_slot`. Если `wal_level` не `logical` — «logical decoding not supported».
>
> **Подводный камень №8 (топики):** Debezium по умолчанию создаёт топики `ledger.outbox_events` (префикс = `topic.prefix`). Нам нужен **`ledger.events`** (TЗ §3.11). Две опции:
> 1. `topic.prefix=""` — топаки `outbox_events` (без схемы). Но тогда payload = raw row.
> 2. **SMT `RegexRouter`**: `RegexRouter(RULES=^ledger\.outbox_events$->REWRITE_TOPIC=ledger.events)`.
>
> **Внимание:** Debezium публикует **raw row** (`{id, event_id, topic, payload, created_at}`), а не сам `payload`. Нужен **Unwrap transform** (или SMT `ExtractNewFieldState`) который «распаковывает» `payload` из nested key. Иначе consumers получают `{before:null, after:{...}}` (CDC format). **Решение:** `transforms.ExtractNewFieldState.field=path=after.payload` → результат = JSON из `OutboxEvent.payload`.

> **Подводный камень №9 (ordering):** Debezium — **at-least-once** доставка. Consumer-ы должны быть idempotent (см. п.6). `event_id` — уникальная защита.

---

## 8. HTTP API (для ручных тестов)

```python
# services/ledger/main.py
def create_app() -> FastAPI:
    app = FastAPI(title="ledger")
    setup(app, service="ledger")          # shared.observability
    @app.post("/v1/accounts/{account_id}/debit")
    async def debit(account_id: str, body: DebitRequest):
        uc = UseCase_Debit(repo, session_factory)
        return await uc.execute(account_id, Money(body.amount, body.currency), body.payment_id)
    @app.post("/v1/accounts/{account_id}/reserve")      # analog
    @app.post("/v1/accounts/{account_id}/cancel-reservation")
    @app.post("/v1/accounts/{account_id}/refund")
    return app
```

> **Подводный камень №10 (HTTP vs Kafka):** HTTP-API — удобен для ручного теста (`curl POST /debit`). **Основной** путь — Kafka `ledger.commands` (из Orchestrator, Этап 3). Оба пути используют **те же** use-cases (single source of truth).

---

## 9. Unit-тесты (100% переходов)

```python
# tests/ledger/test_account_aggregate.py
import pytest
from services.ledger.domain.ledger import Account, Money, InsufficientFundsError

@pytest.fixture
def account_dollar():
    return Account(account_id="A1", owner_name="John", currency="USD",
                   balance=Money(500, "USD"))

def test_debit_success(account_dollar):
    account_dollar.debit(Money(100, "USD"))
    assert account_dollar.balance == Money(400, "USD")
    assert len(account_dollar.pull_events()) == 1    # FundsDebited

def test_debit_insufficient(account_dollar):
    with pytest.raises(InsufficientFundsError):
        account_dollar.debit(Money(600, "USD"))
    assert account_dollar.balance == Money(500, "USD")   # НЕ изменилено

def test_debit_currency_mismatch(account_dollar):
    with pytest.raises(InsufficientFundsError):
        account_dollar.debit(Money(100, "EUR"))
    # currency mismatch → raise (ADR-003: own-currency only)

def test_reserve_then_debit(account_dollar):
    account_dollar.reserve(Money(200, "USD"))
    # теперь доступно: 500 - 200 = 300 (не в balance, а в "available" = balance - reserved)
    # debit(400) должен упасть: 400 > 300
    with pytest.raises(InsufficientFundsError):
        account_dollar.debit(Money(400, "USD"))

def test_refund_increases_balance(account_dollar):
    account_dollar.debit(Money(100, "USD"))
    account_dollar.refund(Money(100, "USD"))
    assert account_dollar.balance == Money(500, "USD")

def test_cancel_reservation_frees_reserved(account_dollar):
    account_dollar.reserve(Money(200, "USD"))
    assert account_dollar.reserved == Money(200, "USD")
    account_dollar.cancel_reservation(Money(200, "USD"))
    assert account_dollar.reserved == Money(0, "USD")
```

> **Покрытие:** `pytest --cov services/ledger --fail-under=100`. Все переходы агрегата покрыты.

---

## 10. Коммиты Этапа 2 (порядок)

| # | Тип | Сообщение | Что |
|---|---|---|---|
| 1 | `feat(ledger)` | add Money, Account, Payment aggregates + invariants | Domain |
| 2 | `feat(ledger)` | add domain events (ledger) | Events |
| 3 | `feat(ledger)` | add use-cases (Debit, Reserve, Cancel, Refund) | Application |
| 4 | `feat(ledger)` | add SQLAlchemy models + repository | Infra |
| 5 | `feat(ledger)` | add HTTP command API | API |
| 6 | `feat(ledger)` | add `ledger.commands` consumer | Kafka |
| 7 | `feat(infra)` | wire Debezium `ledger.outbox → ledger.events` | CDC |
| 8 | `test(ledger)` | add unit tests for aggregates (100%) + coverage | Tests |
| 9 | `docs` | ADR-003 own-currency invariant | ADR |

**Body (пример):**
```
feat(ledger): add use-cases (Debit, Reserve, Cancel, Refund)

Why:
  TЗ §2.4 — Ledger хранит Агрегаты, реализует CQRS (Write-side).
  §3.2 — Transactional Outbox: событие в той же TX, что и данные.

What:
  - UseCase_Debit/Reserve/CancelReservation/Refund
  - Каждый use-case: 1 TX, изменения через методы агрегата,
    _publish_outbox() в той же TX, единый commit.
  - Ошибки → rollback → HTTP 4xx.

Refs: doc/tz_v2.md §2.4, §3.2, ADR-003
```

---

## 11. Как проверить (exit criteria)

```
make up
curl -X POST http://localhost:8003/v1/accounts/A1/debit \
     -H 'Content-Type: application/json' \
     -d '{"payment_id":"P1","amount":100,"currency":"USD"}'
# Response: {"payment_id":"P1","account_id":"A1","amount":{"amount":100,"currency":"USD"}}

# БД:
docker compose exec postgres psql -U fintech -d ledger \
  -c 'SELECT balance FROM accounts WHERE id="A1"'   # 400 (если был 500)
docker compose exec postgres psql -U fintech -d ledger \
  -c 'SELECT count(*) FROM outbox_events'            # +1
# Kafka:
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic ledger.events --from-beginning --max-messages 1
# → {"event_id":"...","account_id":"A1","payment_id":"P1","new_balance":{"amount":400,"currency":"USD"},...}
```

**Красные флаги:**
- [ ] Баланс не изменился.
- [ ] `outbox_events` — нет строки.
- [ ] Kafka `ledger.events` — нет сообщения.
- [ ] Debezium — `errors` в Connect logs.
- [ ] Unit tests — <100% coverage.

---

## 12. Типичные ошибки (на что реально спотыкаются)

| Ошибка | Симптом | Как избежать |
| --- | --- | --- |
| Commit «до» outbox | Событие потеряно при crash | Outbox-строка в той же TX, **до** commit |
| `float` для balance | Ошибка округления | `int` в копейках |
| `account.balance = ...` снаружи | Инварианты обходятся | Только через `account.debit()` |
| Разные session в use-case и repo | Разные TX → outbox-паттерн ломается | Session — владелец use-case |
| `wal_level=replica` | Debezium «logical decoding not supported» | `wal_level=logical` |
| Consumer не idempotent | Двойное списание при rebalance | Unique `idem_key` + `ON CONFLICT` |
| Currency mismatch не checked | Списание EUR со счёта USD | `if amount.currency != self.currency: raise` |

---

## 13. Чек-лист перед merge Этапа 2

- [ ] `make up` → все 18 контейнеров UP.
- [ ] `curl POST /v1/accounts/A1/debit` → баланс −100, outbox +1, Kafka +1 msg.
- [ ] `make test` → unit tests 100% pass.
- [ ] 9 коммитов в `feat/03-ledger`, каждый с ADR-ссылкой.
- [ ] Debezium в Connect — `RUNNING`, `offsets` растут.
- [ ] `psql -c 'SHOW wal_level'` → `logical`.

> **После merge в `main`** — открывать Этап 3 (`03_orchestrator_saga.md`).
