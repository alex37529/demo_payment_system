"""SagaHandler — the saga orchestrator (phase 3, tz_v2 §3 + spec §3).

Responsibilities (single coordination point, tz_v2 §1):
  1. Consume ``payments.commands`` (``Create``) — start a saga.
  2. Drive the state machine:  ``Checking → Reserve → Fraud → Debit → Success``,
     issuing commands on ``ledger.commands`` and reacting to the ledger's events
     (``FundsReserved`` / ``FundsDebited``) on ``ledger.events``.
  3. Fraud step behind a circuit breaker; CB-open / decline ⇒ payment →
     ``ledger.commands.dlq`` (not lost, and the valid stream keeps flowing).
  4. Compensation: a saga past a *confirmed* reservation that then fails gets
     that reservation cancelled (``CancelReservation``) and ends ``RolledBack``;
     a failure at/without a reservation ends ``Failed``.
  5. Publish the saga lifecycle events to ``payments.events``.

Note on the publisher: every call goes through ``shared.kafka.KafkaProducer``
which retries with exp-backoff+jitter and, after the budget, routes the message
to ``<topic>.dlq`` (then raises). The handler catches that so a single bad
message can never wedge the consumer loop.
"""
from __future__ import annotations

import logging
import uuid
from typing import Any, Protocol

from services.orchestrator.domain import Saga, SagaStateMachine, normalize_amount
from services.orchestrator.infrastructure.fraud_client import fraud_request
from services.orchestrator.infrastructure.repository import SagaRepository
from shared.circuit import CircuitBreakerHttpClient
from shared.domain.events import (
    DebitFailed,
    FraudVerdict,
    LimitsChecked,
    SagaFailed,
    SagaSucceeded,
)
from shared.metrics import inc_dlq, inc_failed, inc_processed

log = logging.getLogger(__name__)

TOPIC_PAYMENTS_COMMANDS = "payments.commands"
TOPIC_PAYMENTS_EVENTS = "payments.events"
TOPIC_LEDGER_COMMANDS = "ledger.commands"
TOPIC_LEDGER_EVENTS = "ledger.events"
TOPIC_DLQ = "ledger.commands.dlq"

#: saga states that are terminal & final
TERMINAL_STATES = frozenset({"Success", "Failed", "RolledBack"})


def _failure_reason(failed_step: str, reason: str) -> str:
    """Map a free-form failure reason to a STABLE, low-cardinality label.

    A Prometheus label must not carry a free-text reason (unbounded
    cardinality — one series per distinct message). We bucket by the saga step
    and the few known reasons; everything else collapses to ``other``.

    The order matters: the watchdog's reason is
    ``"watchdog_timeout: saga stalled in in-flight state"`` — it CONTAINS the
    word "timeout", but it is NOT a timeout. It means "the saga never got an
    answer and the watchdog had to compensate it". Bucketing it as
    ``timeout`` was actively misleading (it looked like a network/DB timeout
    when the real cause is a command that failed a BUSINESS validation and
    therefore produced no event at all).
    """
    text = (reason or "").lower()
    if "watchdog" in text:
        return "watchdog_stalled"
    if "circuit" in text or "circuit_open" in text:
        return "cb_open"
    if "decline" in text or "high_amount" in text:
        return "fraud_decline"
    if "insufficient" in text:
        return "insufficient_funds"
    if "timeout" in text:
        return "timeout"
    if failed_step == "fraud":
        return "fraud_decline"
    if failed_step == "debit":
        return "insufficient_funds"
    if failed_step == "limits":
        return "limits"
    return "other"


class LedgerPublisher(Protocol):
    """Minimal async publisher the handler depends on (KafkaProducer satisfies it)."""

    async def publish(self, topic: str, key: str, value: dict) -> None: ...


class SagaHandler:
    def __init__(self, *, repo: SagaRepository, publisher: LedgerPublisher,
                 fraud: CircuitBreakerHttpClient, fraud_url: str) -> None:
        self.repo = repo
        self.pub = publisher
        self.fraud = fraud
        self.fraud_url = fraud_url

    # ===================================================================== #
    # entry points (one per consumer-loop message)
    # ===================================================================== #
    async def on_create(self, key: str, payload: dict) -> None:
        cmd = payload.get("cmd")
        if cmd is not None and cmd != "Create":
            log.warning("payments.commands cmd=%r ignored (key=%s)", cmd, key)
            return
        payment_id = str(payload.get("payment_id") or key)
        if not payment_id:
            log.warning("payments.commands Create without payment_id — dropped")
            return
        try:
            amount = normalize_amount(payload.get("amount"), str(payload.get("currency", "")))
        except ValueError as e:
            await self._bad_command(payment_id, payload, reason=f"bad amount: {e}")
            return

        saga = Saga(saga_id=str(uuid.uuid4()), payment_id=payment_id, state="Created",
                    payload={"account_id": str(payload.get("account_id", "")),
                             "amount": amount,
                             "idem_key": str(payload.get("idem_key", ""))})
        if not await self.repo.create_unique(saga):
            log.info("saga for payment %s already exists — replay, no re-run", payment_id)
            return

        # Step 1: CheckLimits (PoC: in-memory OK — the hard limit lives on the
        # ledger side in the reserve guard; here we just record the step done,
        # then advance to Reserve and issue the Reserve command).
        sm = SagaStateMachine()
        sm.start_checking()
        saga.mark_step("check", cmd="CheckLimits", event="LimitsChecked", ok=True)
        saga.state = "Checking"
        await self._emit(LimitsChecked(payment_id=payment_id, saga_id=saga.saga_id))
        log.info("saga %s: limits OK -> issuing Reserve", saga.saga_id)
        await self._issue_reserve(saga)

    async def on_funds_reserved(self, key: str, payload: dict) -> None:
        payment_id = str(payload.get("payment_id") or key)
        row = await self.repo.get_by_payment(payment_id)
        if row is None:
            log.warning("FundsReserved for unknown payment %s — ignoring", payment_id)
            return
        if row.state not in ("Reserve", "Fraud"):
            log.warning("FundsReserved in unexpected state %s (payment %s) — ignoring",
                        row.state, payment_id)
            return
        saga = Saga.load(row)
        saga.mark_step("reserve", cmd="Reserve", event="FundsReserved", ok=True)
        sm = SagaStateMachine.restore(saga.state)
        sm.enter_fraud()
        saga.state = "Fraud"
        await self.repo.save(saga)
        log.info("saga %s: reservation confirmed -> fraud (has_confirmed=%s)",
                 saga.saga_id, saga.has_confirmed_reservation())
        await self._fraud_step(saga)

    async def on_funds_debited(self, key: str, payload: dict) -> None:
        payment_id = str(payload.get("payment_id") or key)
        row = await self.repo.get_by_payment(payment_id)
        if row is None or row.state in TERMINAL_STATES:
            return
        if row.state != "Debit":
            log.warning("FundsDebited in unexpected state %s — ignoring", row.state)
            return
        saga = Saga.load(row)
        saga.mark_step("debit", cmd="Debit", event="FundsDebited", ok=True)
        sm = SagaStateMachine.restore(saga.state)
        sm.complete()
        saga.state = "Success"
        await self.repo.save(saga)
        await self._emit(SagaSucceeded(payment_id=saga.payment_id, saga_id=saga.saga_id))
        # Prometheus: the payment completed end-to-end (phase-7 spec §3.1).
        inc_processed("orchestrator")
        log.info("saga %s: SUCCESS (payment %s)", saga.saga_id, payment_id)

    async def on_compensation_applied(self, key: str, payload: dict, *, event_type: str) -> None:
        """`CancelReservationPosted` / `RefundPosted`: our compensation landed."""
        payment_id = str(payload.get("payment_id") or key)
        row = await self.repo.get_by_payment(payment_id)
        if row is None or row.state not in ("RolledBack", "Failed"):
            log.warning("%s for payment %s in state %s — treating as replay",
                        event_type, payment_id, row.state if row else "none")
            return
        saga = Saga.load(row)
        saga.mark_step("compensation", cmd=event_type, event=event_type, ok=True)
        await self.repo.save(saga)
        log.info("saga %s: compensation %s applied", saga.saga_id, event_type)

    async def on_debit_failed(self, key: str, payload: dict) -> None:
        """Ledger rejected a Debit (business error) — compensate + RolledBack."""
        payment_id = str(payload.get("payment_id") or key)
        row = await self.repo.get_by_payment(payment_id)
        if row is None or row.state in TERMINAL_STATES:
            return
        saga = Saga.load(row)
        await self._finish_failure(saga, failed_step="debit",
                                  reason=str(payload.get("reason", "ledger: debit failed")))

    # ===================================================================== #
    # step impls
    # ===================================================================== #
    async def _issue_reserve(self, saga: Saga) -> None:
        sm = SagaStateMachine.restore(saga.state)
        sm.limits_ok = True
        sm.enter_reserve()
        saga.mark_step("reserve", cmd="Reserve", event="in_flight", ok=True)
        saga.state = "Reserve"
        await self.repo.save(saga)
        # state stays `Reserve`; the ledger's `FundsReserved` event confirms it.
        await self._send_command(saga, "Reserve")

    async def _fraud_step(self, saga: Saga) -> None:
        body = fraud_request({"payment_id": saga.payment_id,
                              "account_id": saga.account_id, "amount": saga.amount})
        result = await self.fraud.post(self.fraud_url, body) or {}
        decision = str(result.get("decision", "")).lower()
        score = float(result.get("score") or 0.0)
        await self._emit(FraudVerdict(payment_id=saga.payment_id, saga_id=saga.saga_id,
                                      decision=decision, score=score))
        if decision == "approve":
            sm = SagaStateMachine.restore(saga.state)
            sm.enter_debit()
            saga.mark_step("fraud", cmd="FraudCheck", event="FraudVerdict", ok=True)
            saga.state = "Debit"
            await self.repo.save(saga)
            log.info("saga %s: fraud OK -> issuing Debit", saga.saga_id)
            await self._issue_debit(saga)
            return
        await self._finish_failure(saga, failed_step="fraud",
                                  reason=result.get("reason") or decision or "fraud")

    async def _issue_debit(self, saga: Saga) -> None:
        saga.mark_step("debit", cmd="Debit", event="in_flight", ok=True)
        # state is already `Debit`; the ledger's `FundsDebited` confirms it.
        await self._send_command(saga, "Debit")

    async def _finish_failure(self, saga: Saga, *, failed_step: str, reason: str) -> None:
        sm = SagaStateMachine.restore(saga.state)
        compensation = saga.compensation_commands(failed_step)
        if compensation:
            sm.rollback()
            saga.state = "RolledBack"
        else:
            sm.fail()
            saga.state = "Failed"
        saga.compensation = compensation
        saga.mark_step(failed_step, cmd=failed_step.capitalize(), event="failure", ok=False, err=reason)
        await self.repo.save(saga)

        # Prometheus: every failure path converges here, so this is the one
        # place that counts failures (phase-7 spec §3.1 `payments_failed_total`).
        inc_failed(_failure_reason(failed_step, reason), step=failed_step)

        if compensation:  # reverse the booked reservation (newest first)
            for cmd in compensation:
                await self._send_command(saga, cmd, reason=reason)

        # failure reason -> DLQ (not lost; the valid stream keeps flowing)
        await self._publish_raw(TOPIC_DLQ, {
            "saga_id": saga.saga_id, "payment_id": saga.payment_id,
            "failed_step": failed_step, "reason": reason, "compensation": compensation,
        })

        # canonical events: always `SagaFailed`; + `DebitFailed` when Debit was the
        # step that failed (event catalog §3.11.1).
        await self._emit(SagaFailed(saga_id=saga.saga_id, payment_id=saga.payment_id,
                                    failed_step=failed_step, reason=reason))
        if failed_step == "debit":
            await self._emit(DebitFailed(payment_id=saga.payment_id, saga_id=saga.saga_id,
                                         step="debit", reason=reason))
        log.info("saga %s: FAILED at %s -> %s (compensation=%s)",
                 saga.saga_id, failed_step, saga.state, compensation)

    # ===================================================================== #
    # low-level plumbing
    # ===================================================================== #
    def _ledger_command(self, saga: Saga, cmd: str, *, reason: str = "") -> dict:
        return {
            "cmd": cmd, "saga_id": saga.saga_id, "payment_id": saga.payment_id,
            "account_id": saga.account_id, "amount": saga.amount, "reason": reason,
        }

    async def _send_command(self, saga: Saga, cmd: str, *, reason: str = "") -> None:
        await self._publish_raw(TOPIC_LEDGER_COMMANDS, self._ledger_command(saga, cmd, reason=reason))

    async def _publish_raw(self, topic: str, value: dict) -> None:
        key = value.get("payment_id", "")
        if topic.endswith(".dlq"):
            # Prometheus: a message we deliberately routed to the DLQ
            # (phase-7 spec §3.1 `dlq_messages_total`).
            inc_dlq(topic)
        try:
            await self.pub.publish(topic, key, value)
        except Exception:  # noqa: BLE001 — producer already routed to its own DLQ
            log.error("publish to %s failed (handler-level DLQ safety net) payment_id=%s",
                      topic, key)

    async def _emit(self, event: Any) -> None:
        payload = dict(event.to_json())
        key = payload.get("payment_id") or payload.get("saga_id") or ""
        try:
            await self.pub.publish(TOPIC_PAYMENTS_EVENTS, key, payload)
        except Exception:  # noqa: BLE001
            log.error("payments.events publish failed event=%s", type(event).__name__)

    async def _bad_command(self, payment_id: str, payload: dict, *, reason: str) -> None:
        await self._publish_raw(TOPIC_DLQ, {
            "saga_id": "", "payment_id": payment_id, "reason": reason, "original": payload,
        })
        log.warning("payment %s: bad command -> DLQ (%s)", payment_id, reason)
