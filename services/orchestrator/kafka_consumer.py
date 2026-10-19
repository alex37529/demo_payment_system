"""Orchestrator Kafka consumers (two loops, phase-3 spec §5, pitfall #9).

Loop 1 — ``payments.commands`` (group ``orchestrator-cmds``):
    ``Create`` -> ``SagaHandler.on_create``.
Loop 2 — ``ledger.events`` (group ``orchestrator-evt``):
    dispatch by the canonical ``event_type`` discriminator (see
    ``shared/domain/events.py:DomainEvent.to_json``):
      * ``FundsReserved``          -> on_funds_reserved
      * ``FundsDebited``           -> on_funds_debited
      * ``CancelReservationPosted``-> on_compensation_applied
      * ``DebitFailed``            -> on_debit_failed
    (``RefundPosted`` is a no-op for now — phase 5).

Two loops run as separate ``asyncio`` tasks with **distinct** consumer groups
so the single partition on each topic is not split across loops (spec pitfall #9).

Note: `DebitFailed` on ``ledger.events`` is currently NOT produced by the ledger
(Phase 2 logs it and stays up, by design). The `SagaWatcher` (watchdog) is the
actual compensation trigger when a saga stalls in an in-flight state. The
`on_debit_failed` handler remains available should that event be added in a
follow-up phase.
"""
from __future__ import annotations

import asyncio
import logging

from services.orchestrator.application import (
    TOPIC_LEDGER_EVENTS,
    TOPIC_PAYMENTS_COMMANDS,
    SagaHandler,
)
from shared.kafka import KafkaConsumer

log = logging.getLogger(__name__)

CMD_GROUP = "orchestrator-cmds"
EVT_GROUP = "orchestrator-evt"

EVENT_DISPATCH = {
    "FundsReserved": "on_funds_reserved",
    "FundsDebited": "on_funds_debited",
    "CancelReservationPosted": "on_compensation_applied",
    "RefundPosted": "on_compensation_applied",
    "DebitFailed": "on_debit_failed",
}


def _unwrap(record: dict) -> dict:
    """Flatten a Debezium-flavoured record to the canonical event dict.

    What Debezium + `ExtractNewRecordState` actually produces (verified live in
    phase 3): the Kafka record value is the *outbox row* with the event kept in
    `payload` as a **JSON string** (it is a PG `JSONB` column):
    ``{"id":1, "topic":"ledger.events", "payload":"{...}", "status":"pending"}``.

    Returns the inner event dict when present (the thing our handler cares
    about), else the record itself so a future Debezium reconfig degrades
    gracefully. `payments.commands` (inline dicts, no Debezium) pass through.
    """
    inner = record.get("payload")
    if isinstance(inner, str):
        import json

        try:
            return json.loads(inner)
        except (json.JSONDecodeError, ValueError):
            return record
    if isinstance(inner, dict):
        return inner
    return record


def _event_type(inner: dict) -> str | None:
    """Name the canonical event from its inner dict.

    * **new** rows (phase 3+) carry an explicit `event_type` (see
      `shared.domain.events.DomainEvent.to_json`) — use it directly.
    * **old** rows (already in `ledger.events` before this phase) have no
      `event_type` — infer from shape: `new_balance` → `FundsDebited`;
      `reason` → `CancelReservationPosted`; else `FundsReserved`.
    """
    if "event_type" in inner:
        return str(inner["event_type"])
    if "new_balance" in inner:
        return "FundsDebited"
    if "reason" in inner:
        return "CancelReservationPosted"
    if "amount" in inner:
        return "FundsReserved"
    return None


async def run_consumers(
    handler: SagaHandler,
    *,
    brokers: str,
    enabled: bool = True,
) -> tuple[asyncio.Task, asyncio.Task] | tuple[None, None]:
    if not enabled:
        return (None, None)

    c_cmd = KafkaConsumer(brokers, CMD_GROUP, [TOPIC_PAYMENTS_COMMANDS])
    c_evt = KafkaConsumer(brokers, EVT_GROUP, [TOPIC_LEDGER_EVENTS])
    await c_cmd.start()
    await c_evt.start()

    async def loop_cmds() -> None:
        log.info("orchestrator: consumer loop 1 up (%s)", TOPIC_PAYMENTS_COMMANDS)
        try:
            async for key, payload in c_cmd.stream():
                try:
                    await handler.on_create(key, payload)
                except Exception:
                    log.exception("on_create failed key=%s", key)
        finally:
            await c_cmd.stop()

    async def loop_events() -> None:
        log.info("orchestrator: consumer loop 2 up (%s)", TOPIC_LEDGER_EVENTS)
        try:
            async for key, raw in c_evt.stream():
                payload = _unwrap(raw)               # Debezium record -> inner event
                etype = _event_type(payload)
                if etype is None:
                    log.warning("ledger.events with no recognisable event: %s", raw)
                    continue
                method_name = EVENT_DISPATCH.get(etype)
                if method_name is None:
                    log.info("ignoring %s for payment %s", etype, payload.get("payment_id"))
                    continue
                method = getattr(handler, method_name)
                try:
                    await method(key, payload)
                except Exception:
                    log.exception("%s failed key=%s etype=%s", method_name, key, etype)
        finally:
            await c_evt.stop()

    return (asyncio.create_task(loop_cmds(), name="orch-cmds"),
            asyncio.create_task(loop_events(), name="orch-evt"))
