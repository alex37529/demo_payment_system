"""Notification in-memory Payment-Log ring buffer (phase-5 spec §4.1).

Holds the last N payment events so the UI can render a live feed WITHOUT a
database round-trip. Pure in-memory: the moment the process restarts the
buffer is empty (acceptable — this *is* the recent window for the UI feed;
the durable record is the ledger write-side / profile read model).

``maxlen`` caps memory (default 1000, spec §4.1). ``appendleft`` keeps the
*newest* first so ``latest(n)`` is a cheap slice.
"""
from __future__ import annotations

from collections import deque
from typing import Any


class PaymentLog:
    """Bounded, newest-first ring buffer of payment events (dicts)."""

    def __init__(self, maxlen: int = 1000) -> None:
        if maxlen < 1:
            raise ValueError("maxlen must be >= 1")
        self._dq: deque[dict[str, Any]] = deque(maxlen=maxlen)

    def push(self, payment: dict[str, Any]) -> None:
        """Insert a payment at the head (newest). Evicts the oldest on overflow."""
        self._dq.appendleft(payment)

    def latest(self, n: int = 20) -> list[dict[str, Any]]:
        """The newest ``n`` payments, newest first (cheap slice)."""
        n = max(0, n)
        return list(self._dq)[:n]

    def __len__(self) -> int:
        return len(self._dq)

    def __contains__(self, payment_id: str) -> bool:
        """Cheap "is this payment in the feed" check (tests / red flags)."""
        return any(p.get("payment_id") == payment_id for p in self._dq)
