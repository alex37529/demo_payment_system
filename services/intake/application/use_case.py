"""IntakeService — the create-payment use-case (phase-4 spec §3.4/§5).

Flows for `POST /v1/payments` with an `Idempotency-Key`:

  new key (202):
    acquire(key) -> True
    Kafka publish `payments.commands` (Create)         [on fail: rollback + 503]
    response   = {status:accepted, payment_id, ...}
    complete(key, response)                            [cache for replay]

  replay (200):
    acquire(key) -> False
    cached = get_cached(key)
    cached ? 200 + cached  : 409 (in-progress / answer lost)

The service has **no FastAPI** — the API layer maps these results to status
codes, keeping the domain/application layer transport-agnostic (layering rule).
"""
from __future__ import annotations

import logging
from typing import Protocol

from services.intake.application.dtos import CreatePaymentRequest, CreatePaymentResponse
from services.intake.application.idempotency import IdempotencyService

log = logging.getLogger(__name__)

TOPIC_PAYMENTS_COMMANDS = "payments.commands"


class KafkaUnavailable(Exception):
    """Publish to `payments.commands` failed after retries (-> caller 503s)."""


class Publisher(Protocol):
    async def publish(self, topic: str, key: str, value: dict) -> None: ...


class IntakeService:
    def __init__(self, idem: IdempotencyService, publisher: Publisher) -> None:
        self.idem = idem
        self.pub = publisher

    async def create(self, req: CreatePaymentRequest, *, idem_key: str) -> tuple[int, dict]:
        """Execute the create-payment flow. Returns `(status_code, body)`.

        The status codes follow spec §3.4/§6: `202` (fresh, accepted),
        `200` (idempotent replay, cached answer), `409` (replay, no answer),
        `503` (Kafka down — key released so the client can retry).
        """
        if not await self.idem.acquire(idem_key):
            cached = await self.idem.get_cached(idem_key)
            if cached:
                return 200, dict(cached)          # same payment_id, no re-publish
            return 409, {"error": "already processing", "idempotency_key": idem_key}

        cmd = req.to_command()
        # publish MUST precede caching (pitfall #7): the real payment_id is only
        # authoritative once it is in Kafka.
        try:
            await self.pub.publish(TOPIC_PAYMENTS_COMMANDS, cmd["payment_id"], cmd)
        except Exception as e:
            log.error("publish payments.commands failed for %s: %s", idem_key, e)
            await self.idem.rollback(idem_key)    # release so the client can retry
            raise KafkaUnavailable(str(e)) from e

        response: CreatePaymentResponse = CreatePaymentResponse(
            status="accepted",
            payment_id=cmd["payment_id"],
            account_id=cmd["account_id"],
            amount=cmd["amount"],
        )
        body = response.model_dump()
        await self.idem.complete(
            idem_key, response=body, payment_id=cmd["payment_id"],
        )
        return 202, body
