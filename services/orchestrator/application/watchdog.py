"""Saga watchdog — compensation trigger for stale in-flight sagas.

Why a watchdog (ADR-005, phase-3 deviation):
* The spec §3.5 shows a **ledger** event `DebitFailed` driving compensation.
  The ledger (Phase 2) does NOT emit that event — its consumer logs the
  business error and stays up (correct: the ledger doesn't *own* saga
  compensation, the orchestrator does).
* Rather than (a) wiring a new `DebitFailed` producer into the Phase-2 ledger,
  or (b) silently dropping the compensation path for the debit-rejected case,
  the watchdog is a small asyncio task *in the orchestrator* that catches any
  saga left in an in-flight state (`Reserve` / `Fraud` / `Debit`) past a
  timeout and applies the same compensation the spec's `on_debit_failed` would.
* `SagaHandler.on_debit_failed` stays available for an explicit `DebitFailed`
  event if it's added later; the watchdog remains the primary trigger.

Tunable via `ORCH_WATCHDOG_S` (seconds; `0`/`false` disables). Tests pass
`max_iters=` and a tiny `interval=` to exercise the loop without waiting.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import UTC, datetime, timedelta

from sqlalchemy import select

from services.orchestrator.application.saga_handler import SagaHandler
from services.orchestrator.domain import Saga
from services.orchestrator.infrastructure.models import SagaRow

log = logging.getLogger(__name__)

IN_FLIGHT_STATES = ("Reserve", "Fraud", "Debit")


def _enabled() -> bool:
    return os.environ.get("ORCH_WATCHDOG_S", "60") not in ("0", "false", "False", "")


def _interval_s() -> float:
    try:
        return max(0.0, float(os.environ.get("ORCH_WATCHDOG_S", "60")))
    except ValueError:
        return 60.0


async def run_watchdog(handler: SagaHandler, *,
                       interval: float | None = None,
                       horizon_seconds: float | None = None,
                       max_iters: int | None = None) -> None:
    """Sweep stale in-flight sagas every `interval` seconds and apply compensation.

    Args:
        handler: active `SagaHandler` (repo + publisher + fraud are reused).
        interval: override the env-derived loop period (seconds).
        horizon_seconds: treat a saga as "stale" if `updated_at` is older than
            this many seconds (default 60s).
        max_iters: cap the loop (used by tests; None = run until cancelled).
    """
    if not _enabled():
        log.info("watchdog disabled (ORCH_WATCHDOG_S=0)")
        return
    if interval is None:
        interval = _interval_s()
    horizon = horizon_seconds if horizon_seconds is not None else 60.0
    iters = 0
    while True:
        iters += 1
        try:
            cutoff = datetime.now(UTC) - timedelta(seconds=horizon)
            async with handler.repo.session_factory() as s:
                rows = (await s.execute(
                    select(SagaRow).where(
                        SagaRow.state.in_(IN_FLIGHT_STATES),
                        SagaRow.updated_at < cutoff,
                    ))).scalars().all()
            for row in rows:
                log.warning("watchdog: stale saga %s in state %s (payment %s) — compensating",
                            row.saga_id, row.state, row.payment_id)
                saga = Saga.load(row)
                await handler._finish_failure(
                    saga, failed_step=(row.state or "").lower(),
                    reason="watchdog_timeout: saga stalled in in-flight state",
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("watchdog sweep failed")
        if max_iters is not None and iters >= max_iters:
            break
        await asyncio.sleep(interval)
