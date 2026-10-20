# v0.1.0 — FinTech PoC

Первый (и последний на текущий объём) релиз демо-стенда: 8 микросервисов на Kafka,
с сагой, Transactional Outbox, CQRS и полной обсервабельностью.

## Что внутри

**Ядро**
- **8 сервисов**: Gateway/BFF, Intake, Orchestrator, Ledger, Profile, Notification,
  mock-acquirer, mock-fraud.
- **Kafka KRaft** (single-node) + **Debezium** — CDC вычитывает WAL, поэтому
  событие не может потеряться: Transactional Outbox (событие в той же TX, что и
  изменение данных), outbox **append-only** (одно сообщение в Kafka на событие).
- **Saga Orchestration** (`python-statemachine`): FSM — единственный авторитет
  по допустимым переходам; компенсация выводится из истории шагов, её нельзя
  пропустить; watchdog добивает зависшие саги.
- **Circuit Breaker** (`aiobreaker`) + **DLQ** на каждый топик: отказ фрод-проверки
  не теряет платёж, а припарковывает его; валидный поток продолжает течь.
- **CQRS**: write-side (Ledger) и read-side (Profile + Notification) — разные БД,
  read-side обновляется событиями, чужую БД не читает никто.
- **Идемпотентность**: `Idempotency-Key` → один ответ (Redis `SET NX EX` +
  Postgres-зеркало, два TTL).
- **SSE + React-фронтенд**: живая лента платежей, таблица юзеров, бейдж CB.

**Observability**
- **OpenTelemetry → Tempo**: сквозной трейс одного платежа (Gateway → Intake →
  Orchestrator → Ledger), перенос контекста через Kafka-заголовки (W3C
  `traceparent`).
- **Prometheus**: HTTP-метрики + бизнес-сигналы (CB, lag, DLQ, payments, SSE).
- **Loki**: JSON-логи всех сервисов с лейблами `service`/`level`.
- **Grafana**: дашборд `payments-overview` (7 панелей), **Alertmanager** с
  рабочими правилами (p99, CB, DLQ, lag, read-side упал).

**Тесты и приёмка**
- 227 unit-тестов (hermetic, без Docker) + 7 E2E против живой стенки.
- `make demo` → 27/27 проверок приёмки (§7 ТЗ v2) на свежей стенке.

## Как запустить

```bash
make up            # поднять стенд (19 контейнеров)
make demo          # полный сценарий + приёмка (27/27)
```
- UI платежей: http://localhost:5173
- Grafana (без логина): http://localhost:3000
- Prometheus :9090 · Tempo :3200 · Loki :3100 · Alertmanager :9093

## Как это чинили

Три находки, которые стоили больше всего (все — из-за того, что **лог-пиллар
молчал**):

1. **Логи не доходили до Loki вообще** — log-driver `local` пишет gzipped-файлы,
   promtail их не читал. Плюс `log.info` самого сервиса терялись: uvicorn держит
   root-логгер на WARNING.
2. **Денежный баг**: резерв не снимался на успешном пути → `available` сжимался
   навсегда → пользователь с балансом ровно в сумму платежа не мог заплатить.
   (ADR-010.)
3. **Outbox не был append-only** — delivery-tracker обновлял его строку, а Debezium
   слал второе сообщение на то же событие.

## Границы (PoC)

KRaft single-node · in-memory состояние fraud-режима · own-currency (кросс-валюта
вне скоупа, ADR-003) · 24 h TTL идемпотентности · retention логов 72 h · без
AuthN/AuthZ · chaos-тесты = kill контейнера. Полный список — в
[README](../README.md) и `doc/phases/08_release_acceptance.md` §9.

## Документация

- ТЗ: `doc/tz_v2.md` · план: `doc/implementation_plan.md`
- отчёты фаз: `doc/done/` · решения: `doc/adr/` (ADR-001..012)
- контракты событий: `doc/events.md` · операции: `doc/runbook.md` · схемы: `doc/diagrams/`
