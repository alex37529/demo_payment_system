"""Intake domain errors (phase 4).

`IntakeError` is the HTTP-translation base (`shared.observability`'s handler
maps it to 400 by mirroring `LedgerError`); `PaymentNotFound` → 404.
"""
from __future__ import annotations


class IntakeError(Exception):
    """Base for intake domain errors."""


class PaymentNotFound(IntakeError):
    """No such payment id was accepted by intake (GET /v1/payments/{id})."""

    def __init__(self, payment_id: str) -> None:
        self.payment_id = payment_id
        super().__init__(f"payment {payment_id} was not accepted by intake")
