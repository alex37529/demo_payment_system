"""Ledger write-side consumer: `ledger.commands` -> use-cases.

Commands (orchestrator, phase 3):
  {payment_id, saga_id?, account_id, cmd: "Debit"|"Reserve"|"CancelReservation"|"Refund",
   amount: {value, currency}, reason?}

Idempotent: the use-case's `applied_commands` guard returns the same (no-op)
result on replay, so at-least-once delivery never double-applies.
Business failures (InsufficientFundsError) are logged and NOT re-raised — the
orchestrator owns compensation / DLQ decisions.
"""
from __future__ import annotations

import logging

from services.ledger.application import USE_CASES
from services.ledger.domain import LedgerError, Money
from shared.kafka import KafkaConsumer

log = logging.getLogger(__name__)

TOPIC = "ledger.commands"


async def run_consumer(
    *,
    brokers: str,
    runner,
    group: str = "ledger-write",
) -> None:
    consumer = KafkaConsumer(brokers, group, [TOPIC])
    await consumer.start()
    try:
        async for key, payload in consumer.stream():
            cmd = payload.get("cmd")
            uc_cls = USE_CASES.get(cmd or "")
            if uc_cls is None:
                log.warning("unknown ledger command %r (%s) -> ignore", cmd, key)
                continue
            uc = uc_cls(session_factory=runner.session_factory, repo=runner.repo)
            try:
                amount = Money.from_dict(payload.get("amount")) or Money(0, "USD")
                res = await uc.execute(
                    account_id=payload.get("account_id", ""),
                    amount=amount,
                    payment_id=payload.get("payment_id", ""),
                    reason=payload.get("reason", ""),
                )
                log.info("applied=%s cmd=%s payment_id=%s ref=%s",
                         res.get("applied"), cmd, res.get("payment_id"), res.get("ref"))
            except LedgerError as e:
                log.warning("ledger command %s failed (business): %s", cmd, e)
            except Exception:
                log.exception("ledger command %s failed (unexpected)", cmd)
    finally:
        await consumer.stop()
