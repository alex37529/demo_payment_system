"""Profile consumer: ``ledger.events`` -> update denormalized balances.

Wires the shared Debezium ``unwrap_event`` (phase-5 spec §3.1) to the pure
``resolve_new_balance`` decision and the idempotent ``ProfileRepo.apply_event``.

Group id ``profile-balance`` is distinct from the Notification reader so the
single partition of ``ledger.events`` is not split (shared/kafka.py, pitfall #18):
the topic fans out to BOTH read-side readers (spec §3, pitfall #4: each context
copies, never joins).

Business rule (spec §3.1, pitfall "delta vs final"): only events that settle the
final balance (``FundsDebited`` → ``new_balance``) touch ``profile.users.balance``;
delta events (FundsReserved / CancelReservationPosted) are recorded as processed
but do not change the balance.
"""
from __future__ import annotations

import logging

from sqlalchemy.exc import IntegrityError

from services.profile.infrastructure import ProfileRepo
from services.profile.projection import resolve_new_balance
from shared.kafka import KafkaConsumer, unwrap_event

log = logging.getLogger(__name__)

TOPIC = "ledger.events"
GROUP = "profile-balance"


async def process_record(raw: dict, key: str, repo: ProfileRepo) -> None:
    """Handle ONE raw ``ledger.events`` record (unwrap -> resolve -> apply).

    Extracted from the consumer loop so it is unit-testable WITHOUT a broker
    (the conftest feeds live Debezium-shaped records through it).
    """
    event = unwrap_event(raw)      # Debezium outbox-row -> canonical event
    account_id = str(event.get("account_id") or "")
    event_id = str(event.get("event_id") or key or "")
    event_type = str(event.get("event_type") or "")
    if not event_id:
        log.warning("ledger.events without event_id (%s) -> skip", key)
        return

    new_balance = resolve_new_balance(event)
    if new_balance is None:
        # delta-only (or unrecognised): the read-side final balance does
        # not change. Record it so a replay is still an idempotent no-op.
        await _safe_mark(repo, event_id, event_type, account_id)
        log.debug("profile: %s %s (no new_balance) -> no balance change",
                  event_type or "?", account_id or key)
        return

    try:
        was_applied = await repo.apply_event(
            event_id=event_id, account_id=account_id,
            new_balance=new_balance, event_type=event_type)
    except IntegrityError:
        # A concurrent replay of the same event won the guarded insert first
        # -> this read-side update is a no-op.
        log.info("profile: %s %s already applied (dup) -> no-op", event_type, account_id)
        return
    log.info("profile: %s %s -> balance=%s (applied=%s)",
             event_type, account_id, new_balance, was_applied)


async def run_consumer(*, brokers: str, repo: ProfileRepo) -> None:
    consumer = KafkaConsumer(brokers, GROUP, [TOPIC])
    await consumer.start()
    try:
        async for key, raw in consumer.stream():
            await process_record(raw, key, repo)
    finally:
        await consumer.stop()


async def _safe_mark(repo: ProfileRepo, event_id: str, event_type: str,
                     account_id: str) -> None:
    # mark_processed is best-effort: a transient hiccup must not drop the loop.
    try:
        await repo.mark_processed(event_id, event_type=event_type, account_id=account_id)
    except Exception:  # noqa: BLE001
        log.warning("profile: mark_processed failed for %s (non-fatal)", event_id)
