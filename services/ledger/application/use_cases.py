"""Ledger use-cases: Debit / Reserve / CancelReservation / Refund.

Pattern (phase-2 spec §4.2):
  1. ONE session (the use-case owns it — pitfall #5)
  2. lock the `accounts` row (`SELECT ... FOR UPDATE`, Postgres) -> hydrate the aggregate
     — pessimistic lock so concurrent debits/refunds serialize on the row; the journal's
     `balance_after` then always equals the committed `accounts.balance` (no lost update)
  3. call the aggregate method (invariants + event produced)
  4. persist aggregate back (`repo.save`)
  5. record idempotency guard (`applied_commands.idem_key` UNIQUE)
  6. publish OutboxEvent in the SAME transaction
  7. single `commit()` -> UPDATE accounts + INSERT applied_commands + INSERT outbox_events
Any `InsufficientFundsError` / `IntegrityError` (replay) => `rollback`, raise.
"""
from __future__ import annotations

import logging
import uuid
from typing import Any

from services.ledger.domain import (
    Account,
    AccountNotFound,
    InsufficientFundsError,
    Money,
)
from services.ledger.infrastructure.models import AccountRow, TransactionRow
from shared.db.repository import IAccountRepository
from shared.domain.events import (
    CancelReservationPosted,
    DebitFailed,
    FundsDebited,
    FundsReserved,
    RefundPosted,
)
from shared.metrics import inc_processed
from shared.outbox import OutboxMixin

TOPIC_LEDGER_EVENTS = "ledger.events"

log = logging.getLogger(__name__)


def idem_ref(payment_id: str, op: str) -> str:
    """Stable idempotency key for a (payment, op) pair. Replays collide -> no-op."""
    return f"{payment_id}:{op}"


class _AccountUseCase(OutboxMixin):
    """Common machinery: one TX, one aggregate, one outbox row."""

    #: which op this use-case performs — only for observability / guard key
    OP = ""

    def __init__(self, *, session_factory: Any, repo: IAccountRepository) -> None:
        self.session_factory = session_factory
        self.repo = repo

    async def _run(
        self,
        *,
        account_id: str,
        amount: Money,
        payment_id: str,
        reason: str = "",
        saga_id: str = "",
    ) -> dict:
        ref = payment_id or str(uuid.uuid4())
        session = self.session_factory()
        try:
            lock = getattr(self.repo, "get_for_update", None)
            if lock is not None:  # row lock (Postgres), so concurrent mutations serialize
                row = await lock(account_id, session=session)
            else:
                row = await session.get(AccountRow, account_id)
            if row is None:
                raise AccountNotFound(f"account {account_id} not found")
            agg = Account.load(row)
            event = self._apply(agg, amount, ref, reason)
            await self.repo.save(agg, session=session)
            applied = await self.repo.mark_applied(
                idem_key=idem_ref(ref, self.OP),
                op=self.OP,
                account_id=account_id,
                payment_id=payment_id,
                saga_id=saga_id,
                session=session,
            )
            if not applied:  # replay -> drop our changes, return idempotent success
                await session.rollback()
                return {
                    "payment_id": payment_id,
                    "account_id": account_id,
                    "amount": amount.to_dict(),
                    "applied": False,
                }
            outbox_row = self._publish_outbox(session, TOPIC_LEDGER_EVENTS, event)
            self._record_journal(session, event, balance_after=agg.balance.amount,
                                 outbox_row=outbox_row, payment_id=payment_id)
            await session.commit()
            # Prometheus: the money actually moved (phase-7 spec §3.1).
            # Counted only for balance-moving ops — a reservation does not
            # settle a payment.
            if type(event).__name__ in self.JOURNALABLE:
                inc_processed("ledger")
        except (InsufficientFundsError, AccountNotFound) as e:
            await session.rollback()
            # A rejected command MUST be announced (ADR-010). The saga waits
            # for the confirmation event of the step it asked for; if the
            # ledger answers a rejection with SILENCE, the saga cannot tell
            # "the reserve is still in flight" from "the reserve was
            # refused" and hangs until the watchdog compensates it 30s
            # later. The rejection is published in its OWN transaction (the
            # command's TX is rolled back — nothing was changed).
            await self._publish_rejection(
                payment_id=payment_id, saga_id=saga_id, reason=str(e)
            )
            raise
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
        return {
            "payment_id": payment_id,
            "account_id": account_id,
            "amount": amount.to_dict(),
            "ref": idem_ref(ref, self.OP),
            "applied": True,
            "event_type": type(event).__name__,
        }

    # each concrete use-case exposes this to keep the shared _run() dumb
    def _apply(self, agg: Account, amount: Money, ref: str, reason: str):
        raise NotImplementedError

    #: canonical events that MOVE the account balance (the ones journaled).
    #: Reserve / CancelReservation only move `reserved`, not `balance`, so they
    #: are intentionally excluded from the balance journal.
    JOURNALABLE = ("FundsDebited", "RefundPosted")

    def _record_journal(self, session: Any, event: FundsDebited, *,
                        balance_after: int, outbox_row: Any, payment_id: str) -> None:
        """Append one row to the append-only balance journal IN THE SAME TX.

        Called after the outbox publish so the journal row can reuse the same
        canonical `event_id` (a replay of the same event -> same `event_id` ->
        the UNIQUE constraint makes it a no-op, consistent with the idempotency
        guard). Rolls back with the whole use-case on failure — there is no
        window where the balance changed but the journal is missing a row.
        """
        if type(event).__name__ not in self.JOURNALABLE:
            return  # not a balance-moving op (e.g. reserve); nothing to journal
        event_id = str(getattr(event, "event_id", "") or (getattr(outbox_row, "event_id", "") or ""))
        session.add(TransactionRow(
            account_id=event.account_id,
            event_id=event_id,
            event_type=type(event).__name__,
            amount=int(event.amount.get("value", 0)),
            currency=str(event.amount.get("currency", "USD")),
            balance_after=int(balance_after),
            payment_id=payment_id or event.payment_id,
        ))

    async def execute(self, *, account_id: str, amount: Money,
                      payment_id: str = "", reason: str = "",
                      saga_id: str = "") -> dict:
        return await self._run(account_id=account_id, amount=amount,
                               payment_id=payment_id, reason=reason, saga_id=saga_id)

    async def _publish_rejection(self, *, payment_id: str, saga_id: str, reason: str) -> None:
        """Announce a REJECTED command as a `DebitFailed` event (ADR-010).

        The event goes through the outbox like every other event — but in
        its OWN transaction: the command's transaction has been rolled back
        (nothing was changed), and a rejection event written into a
        rolled-back transaction would be lost with it.

        The canonical catalogue already has the event (`DebitFailed`) and it
        already carries a ``step`` field — the orchestrator's
        `on_debit_failed` handler is already wired to it. It was simply
        never produced (the phase-3 note said so explicitly). Now the
        saga learns about a refusal immediately instead of stalling until
        the watchdog compensates it.
        """
        if not payment_id:
            return
        event = DebitFailed(
            payment_id=payment_id,
            saga_id=saga_id,
            step=self.OP,
            reason=reason,
        )
        session = self.session_factory()
        try:
            async with session.begin():
                self._publish_outbox(session, TOPIC_LEDGER_EVENTS, event)
        except Exception:  # noqa: BLE001 — a failed announcement must not mask
            # the original rejection (the saga will still time out and be
            # compensated by the watchdog).
            log.warning("could not publish the rejection event for %s: %s",
                        payment_id, reason)
        finally:
            await session.close()


class UseCase_Debit(_AccountUseCase):
    OP = "debit"

    def _apply(self, agg: Account, amount: Money, ref: str, reason: str) -> FundsDebited:
        return agg.debit(amount, ref=ref)


class UseCase_Reserve(_AccountUseCase):
    OP = "reserve"

    def _apply(self, agg: Account, amount: Money, ref: str, reason: str) -> FundsReserved:
        return agg.reserve(amount, ref=ref)


class UseCase_CancelReservation(_AccountUseCase):
    OP = "cancel_reservation"

    def _apply(self, agg: Account, amount: Money, ref: str, reason: str) -> CancelReservationPosted:
        return agg.cancel_reservation(amount, ref=ref)


class UseCase_Refund(_AccountUseCase):
    OP = "refund"

    def _apply(self, agg: Account, amount: Money, ref: str, reason: str) -> RefundPosted:
        return agg.refund(amount, ref=ref)


USE_CASES: dict[str, type[_AccountUseCase]] = {
    "Debit": UseCase_Debit,
    "Reserve": UseCase_Reserve,
    "CancelReservation": UseCase_CancelReservation,
    "Refund": UseCase_Refund,
}


class LedgerUseCaseRunner:
    """Factory + dispatcher shared by the HTTP API and the `ledger.commands` consumer."""

    def __init__(self, *, session_factory: Any, repo: IAccountRepository) -> None:
        self.session_factory = session_factory
        self.repo = repo

    def make(self, op: str) -> _AccountUseCase:
        try:
            return USE_CASES[op](session_factory=self.session_factory, repo=self.repo)
        except KeyError as e:
            raise ValueError(f"unknown ledger command {op!r}") from e
