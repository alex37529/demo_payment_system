"""Intake DTOs — request/response shapes for POST /v1/payments (Pydantic v2).

`CreatePaymentRequest` is validated at the edge (amount>0 <= 100_000 cents,
currency in {USD, EUR}, account_id format) so junk never reaches Kafka (phase-4
spec §2: "validation = protection before Kafka"). `to_command()` builds the
canonical `payments.commands` payload the orchestrator consumes
(`SagaHandler.on_create`), and always carries a `payment_id` — a client
supplied one (stable across idempotent retries) or a fresh `P-{uuid4}`.
"""
from __future__ import annotations

import re
import uuid
from typing import Literal

from pydantic import BaseModel, Field, field_validator

_ACCOUNT_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_MAX_CENTS = 100_000
_CURRENCY = Literal["USD", "EUR"]


class CreatePaymentRequest(BaseModel):
    """Incoming create-payment request (POST /v1/payments body)."""

    amount: int = Field(..., gt=0, le=_MAX_CENTS, description="amount in cents; $1 = 100")
    currency: _CURRENCY = "USD"
    account_id: str = Field(..., min_length=1, max_length=64)
    client_ref: str | None = Field(default=None, max_length=128)  # client reference
    payment_id: str = Field(default="", max_length=64)  # optional: stable id across retries

    @field_validator("currency")
    @classmethod
    def _currency(cls, v: str) -> str:
        up = v.upper()
        if up not in ("USD", "EUR"):
            raise ValueError("currency must be USD or EUR")
        return up

    @field_validator("account_id")
    @classmethod
    def _account(cls, v: str) -> str:
        if not _ACCOUNT_RE.match(v):
            raise ValueError("account_id invalid format (allowed: A-Z a-z 0-9 _ -)")
        return v

    def to_command(self) -> dict:
        """Build the `payments.commands` command (payload §3.11 TЗ v2)."""
        return {
            "cmd": "Create",
            "payment_id": self.payment_id or f"P-{uuid.uuid4()}",
            "amount": {"value": self.amount, "currency": self.currency},
            "account_id": self.account_id,
            "client_ref": self.client_ref,
        }


class CreatePaymentResponse(BaseModel):
    """Returned to the client (202 fresh / 200 idempotent replay)."""

    status: str = "accepted"
    payment_id: str
    account_id: str = ""
    amount: dict = {}
