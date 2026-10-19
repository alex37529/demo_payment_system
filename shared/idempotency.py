"""Idempotency middleware (§3.1 TЗ v2).

One `Idempotency-Key` -> one response. Atomic `SET NX EX` so a duplicate can't
win a race (pitfall #27). Double TTL (pitfall #29a): the `pending` marker is
short (120s) so a 5xx/worker-dead doesn't wedge the key forever; the final
answer is long (24h, contract). The PG fallback copy lives in phase 4 (Intake).
"""
from __future__ import annotations

import json
from typing import Any

import redis.asyncio as aioredis
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse


class IdempotencyMiddleware(BaseHTTPMiddleware):
    PENDING_TTL = 120   # seconds — "in progress"; short so 5xx doesn't wedge the key
    ANSWER_TTL = 86400  # seconds — final answer; 24h contract (TЗ v2 solution #1)

    def __init__(self, app: Any, redis_url: str) -> None:
        super().__init__(app)
        self.redis = aioredis.from_url(redis_url, decode_responses=True)

    async def dispatch(self, request: Request, call_next: Any) -> Any:
        key = request.headers.get("Idempotency-Key")
        if not key:
            return await call_next(request)

        rkey = f"idem:{key}"
        acquired = await self.redis.set(rkey, "pending", nx=True, ex=self.PENDING_TTL)
        if not acquired:
            cached = await self.redis.get(rkey)
            if cached and cached != "pending":
                return JSONResponse(json.loads(cached))
            return JSONResponse({"error": "in_progress"}, status_code=409)

        response = await call_next(request)

        if response.status_code < 500:
            chunks = [c async for c in response.body_iterator]
            body = b"".join(chunks)
            await self.redis.set(rkey, body.decode(), ex=self.ANSWER_TTL)
            return JSONResponse(
                json.loads(body),
                status_code=response.status_code,
                headers=dict(response.headers),
            )
        return response

    async def aclose(self) -> None:
        await self.redis.aclose()
