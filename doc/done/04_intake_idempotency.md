# Отчёт — Этап 4. Intake + Idempotency

- Дата: 2026-10-20 (отчёт дописан в фазе 8 — сама фаза смержена в PR #4)
- Ветка: `feat/05-intake`
- Статус: **Готов / проверен** (`make test` зелёный; приёмка фазы 8 — критерий 7 «дубль
  Idempotency-Key → 1 событие» — проходит)
- База: `doc/tz_v2.md` §3.1, `doc/phases/04_intake_idempotency.md`, `doc/adr/ADR-011-idempotency.md`

---

## 1. Что сделано

`intake` — единственная точка входа платежа. Её контракт: **`Idempotency-Key` → один
ответ**, и контракт должен держаться при ретраях клиента, при рестарте процесса и при
нескольких инстансах.

| Слой | Файл | Что |
| --- | --- | --- |
| API | `services/intake/api.py` | `POST /v1/payments`, `GET /v1/payments/{id}`; **без** `Idempotency-Key` → 400 (красный флаг §6) |
| Application | `services/intake/application/use_case.py` | `IntakeService.create()`: acquire → publish → complete / rollback |
| | `services/intake/application/idempotency.py` | `IdempotencyService`: Redis `SET NX EX` + Postgres-зеркало, два TTL (pending 120 c / answer 24 ч) |
| | `services/intake/application/dtos.py` | `CreatePaymentRequest` + валидация (amount > 0, currency из списка) |
| Domain | `services/intake/domain/errors.py` | `PaymentNotFound`, `KafkaUnavailable` |
| Infra | `services/intake/infrastructure/{pgrepo,models}.py` | таблица `intake.idempotency_keys` (зеркало) + периодический prune просроченных строк |

## 2. Ключевые решения

- **Redis — основное хранилище, Postgres — зеркало.** `SET NX EX` даёт атомарность
  (ровно один победитель) и общий для всех инстансов стенд; Postgres переживает
  потерю Redis (flush) и рестарт всего стека. → **ADR-011**.
- **Два TTL.** `pending` — 120 с (иначе упавший воркер навсегда заблокирует ключ),
  финальный ответ — 24 ч (контракт).
- **5xx не сохраняет ответ** — неуспешный запрос можно повторить с тем же ключом.
- **Платит создаёт только победитель** (`SET NX`); проигравший возвращает сохранённый
  ответ как есть (тот же `payment_id`).
- **Kafka недоступен → 503 + rollback** ключа: клиент может повторить, и это не
  «потерянный» платёж.

## 3. Проверка

| Проверка | Где | Результат |
| --- | --- | --- |
| Дубль `Idempotency-Key` → тот же `payment_id` | `tests/intake/test_idempotency.py` | ✅ |
| Дубль → **одно** событие в `payments.commands` | критерий 7 приёмки (фаза 8) | ✅ |
| Без `Idempotency-Key` → 400 | `tests/intake/test_api.py` | ✅ |
| Невалидная сумма/валюта → 422 | `tests/intake/test_dto.py` | ✅ |
| Kafka недоступен → 503 | `tests/intake/test_api.py` | ✅ |

## 4. Отклонения от спеки фазы

- **Docker-образ не пересобирается под фазу** — intake использует общий
  `services/shared_app.py`, отдельной сборки нет.
- **`Idempotency-Key` обязателен** (в спеке §6 это «красный флаг»): запрос без
  него → 400, а не «создать платёж молча».

## 5. Что дальше

Отдельных «следующих шагов» нет: контракт закрыт, приёмка фазы 8 (критерий 7) это
проверяет на живом стенде.

---

> **Почему отчёт появился только сейчас.** Фаза 4 была смержена (PR #4) **без** отчёта в
> `doc/done/` — единственная фаза, где он отсутствовал. AGENTS.md требует отчёт на
> каждую фазу; отчёт дописан в фазе 8 по фактически реализованному коду.
