"""IdempotencyService — atomic `SET NX` on Redis with a durable PG fallback.

Design (phase-4 spec §3.3, "dual-write", pitfalls #2/#3):
  * Redis is the **fast path** and the **atomicity authority**: `SET NX EX`
    guarantees only one of N concurrent calls with the same key "wins".
  * PG (`intake.idempotency_keys`) is the **durable mirror**: survives a Redis
    restart and is the source for `GET /v1/payments/{id}` + the cached response
    when Redis is down.

Two TTLs (shared/idempotency.py precedent, pitfall #29a):
  * `PENDING_TTL` — short (120s): the "in progress" marker. A crash / 5xx
    between acquire and store must self-heal quickly, else the key wedges.
  * `ANSWER_TTL`  — long (24h): the final cached answer (the contract).

A short-then-final two-phase write is used so a crash never leaves a key
"locked" without an answer: `acquire` writes a `pending` marker (Redis + PG),
`complete` overwrites with the real response, `rollback` deletes on Kafka fail.
"""
from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta

import redis.asyncio as aioredis

from services.intake.infrastructure import IdempotencyRepo

log = logging.getLogger(__name__)

# TTL contract (mirrors shared/idempotency.IdempotencyMiddleware; pitfall #29a):
# a short "in progress" marker so a crash/5xx doesn't wedge the key, and a long
# final answer (the 24h contract, spec §3.3).
PENDING_TTL = 120     # seconds
ANSWER_TTL = 86400    # seconds (24h)


class IdempotencyService:
    PENDING = "pending"

    def __init__(self, redis_url: str, repo: IdempotencyRepo) -> None:
        self.redis = aioredis.from_url(redis_url, decode_responses=True)
        self.repo = repo

    @staticmethod
    def _rkey(key: str) -> str:
        return f"idem:{key}"

    # ------------------------------------------------------------------ #
    # acquire: atomic "are we the first with this key?"
    # ------------------------------------------------------------------ #
    async def acquire(self, key: str) -> bool:
        """True = new request (proceed to publish). False = duplicate (replay).

        Redis `SET NX` is the authority (atomic). On Redis success we also write
        the PG mirror (best-effort — a PG hiccup must NOT block a valid request).
        On Redis failure we fall back to PG `SELECT` (slower, still correct).
        """
        rk = self._rkey(key)
        try:
            acquired = await self.redis.set(rk, self.PENDING, nx=True, ex=PENDING_TTL)
        except Exception as e:  # noqa: BLE001
            log.warning("redis acquire failed for %s (%s); using PG", key, e)
            return await self._pg_acquire(key)
        if not acquired:
            return False
        # We "won" in Redis. But Redis may have been flushed/restarted since the
        # first request while the durable PG copy kept the stored answer — so the
        # key was actually already processed. Cross-check PG to avoid a silent
        # duplicate publish (repair Redis with the real answer; mark as replay).
        prev = await self.repo.get_response(key)
        if prev is not None:
            await self._restore_redis(rk, prev)
            log.info("acquire(%s): redis fresh but PG had answer -> replay", key)
            return False
        # genuinely new — persist a durable marker (best-effort; `complete`
        # overwrites it with the final answer).
        try:
            await self.repo.clear_key(key)
        except Exception:  # noqa: BLE001
            log.warning("PG marker write failed for %s (redis still authoritative)", key)
        return True

    async def _restore_redis(self, rk: str, prev: dict) -> None:
        try:
            await self.redis.set(rk, json.dumps(prev), ex=ANSWER_TTL)
        except Exception as e:  # noqa: BLE001
            log.debug("restore redis for %s failed (best-effort): %s", rk, e)

    async def _pg_acquire(self, key: str) -> bool:
        """Redis-down path: PG is the gate.

        A row with a real `response` is a replay (-> False); a row with `NULL`
        response is a "new/rolled-back" marker (-> True, refresh the marker);
        no row is fresh (-> True, insert a new marker).
        """
        try:
            row_data = await self.repo.get_response(key)
            if row_data is not None:
                return False                    # a real answer was stored -> replay
            await self.repo.clear_key(key)      # fresh marker (insert if absent)
            return True
        except Exception:
            log.exception("PG acquire failed for %s; permitting (degraded)", key)
            return True

    # ------------------------------------------------------------------ #
    # complete: replace the pending marker with the real response
    # ------------------------------------------------------------------ #
    async def complete(
        self, key: str, *, response: dict, payment_id: str = ""
    ) -> None:
        """Store the final answer (long TTL) so an idempotent replay returns it.

        Redis first (fast path); then PG (durable + the GET index). If the Redis
        write fails we still persist to PG — a later replay with Redis up will
        miss the cache and fall back to the PG `in_progress`/409 path, which is
        safer than silently publishing a duplicate.
        """
        rk = self._rkey(key)
        ans_until = datetime.now(UTC) + timedelta(seconds=ANSWER_TTL)
        try:
            await self.redis.set(rk, json.dumps(response), ex=ANSWER_TTL)
        except Exception as e:  # noqa: BLE001
            log.warning("redis store failed for %s (%s); PG-only", key, e)
        try:
            await self.repo.upsert(key=key, response=response, ttl_until=ans_until,
                                   payment_id=payment_id)
        except Exception:
            log.exception("PG response store failed for %s", key)

    # ------------------------------------------------------------------ #
    # get_cached: find the stored answer (redis fast, then PG)
    # ------------------------------------------------------------------ #
    async def get_cached(self, key: str) -> dict | None:
        rk = self._rkey(key)
        try:
            raw = await self.redis.get(rk)
            if raw:
                if raw == self.PENDING:
                    return None            # in-progress, no answer yet
                return json.loads(raw)
        except Exception as e:  # noqa: BLE001
            log.warning("redis get failed for %s (%s); PG fallback", key, e)
        return await self.repo.get_response(key)

    # ------------------------------------------------------------------ #
    # rollback: release the key when the Kafka publish failed
    # ------------------------------------------------------------------ #
    async def rollback(self, key: str) -> None:
        """Remove the marker so the client can retry. Best-effort on both stores."""
        rk = self._rkey(key)
        try:
            await self.redis.delete(rk)
        except Exception as e:  # noqa: BLE001
            log.debug("rollback redis delete failed for %s: %s", key, e)
        try:
            await self.repo.delete(key)
        except Exception as e:  # noqa: BLE001
            log.debug("rollback pg delete failed for %s: %s", key, e)

    # ------------------------------------------------------------------ #
    # cleanup: drop expired PG rows (the durable copy has no automatic TTL)
    # ------------------------------------------------------------------ #
    async def cleanup(self, now: datetime | None = None) -> int:
        now = now or datetime.now(UTC)
        try:
            return await self.repo.delete_expired(now)
        except Exception:
            log.exception("PG idempotency cleanup failed")
            return 0

    async def aclose(self) -> None:
        try:
            await self.redis.aclose()
        except Exception as e:  # noqa: BLE001
            log.debug("redis aclose failed: %s", e)
