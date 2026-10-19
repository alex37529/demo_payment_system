"""Async Postgres repository for the persistent idempotency copy.

The `intake.idempotency_keys` table is the *durable* mirror of the Redis
`idem:{key}` entry. It survives a Redis restart and is the source of truth for
the cached response (spec §3.3 "dual-write"). Redis is the fast path; PG is the
fallback when Redis is down and for GET lookups.

Session ownership: open a short `session_factory()` per call. Each op is small
and idempotent on the `key` PK (`INSERT ... WHERE NOT EXISTS` via a SELECT-then-
INSERT + IntegrityError catch in `IdempotencyRow`'s PK).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError

from services.intake.infrastructure.models import IdempotencyRow


class IdempotencyRepo:
    def __init__(self, session_factory: Any) -> None:
        self.session_factory = session_factory

    async def upsert(
        self, *, key: str, response: dict | None, ttl_until: datetime | None,
        payment_id: str | None = None,
    ) -> None:
        """Insert-or-update the row. PK is `key`; concurrent upserts on the same
        key are safe (Postgres raises IntegrityError, SQLite upserts cleanly).
        """
        async with self.session_factory() as s:
            row = await s.get(IdempotencyRow, key)
            if row is None:
                row = IdempotencyRow(key=key, response=response, ttl_until=ttl_until,
                                    payment_id=payment_id)
                s.add(row)
            else:
                row.response = response
                row.ttl_until = ttl_until
                row.payment_id = payment_id
            try:
                await s.commit()
            except IntegrityError:
                await s.rollback()

    async def get_response(self, key: str) -> dict | None:
        async with self.session_factory() as s:
            row = await s.get(IdempotencyRow, key)
            if row is None:
                return None
            data = row.response
            return dict(data) if isinstance(data, dict) else None

    async def get_by_payment(self, payment_id: str) -> dict | None:
        """The cached response for a `payment_id` (backs GET /v1/payments/{id})."""
        async with self.session_factory() as s:
            r = await s.execute(
                select(IdempotencyRow).where(IdempotencyRow.payment_id == payment_id))
            row = r.scalars().first()
            if row is None or row.response is None:
                return None
            return dict(row.response)

    async def delete(self, key: str) -> bool:
        """Remove the row (best-effort on rollback)."""
        async with self.session_factory() as s:
            res = await s.execute(delete(IdempotencyRow).where(IdempotencyRow.key == key))
            await s.commit()
            return (res.rowcount or 0) > 0

    async def clear_key(self, key: str) -> None:
        """Reset the row's stored response to `NULL` (a 'new' pending marker).

        Used by the PG-fallback gate so that after a Kafka rollback the same key
        can be re-acquired (the row's presence alone would otherwise read as a
        replay). The `payment_id` index is left untouched.
        """
        async with self.session_factory() as s:
            row = await s.get(IdempotencyRow, key)
            if row is None:
                row = IdempotencyRow(key=key, response=None)
                s.add(row)
            else:
                row.response = None
            try:
                await s.commit()
            except IntegrityError:
                await s.rollback()

    async def delete_expired(self, at: datetime) -> int:
        """Delete rows with `ttl_until` in the past (periodic cleanup).

        `ttl_until` is `NULL` for rows written before TTL tracking — those are
        kept (conservative: a NULL TTL is treated as "still valid").
        """
        async with self.session_factory() as s:
            stmt = delete(IdempotencyRow).where(
                IdempotencyRow.ttl_until.is_not(None), IdempotencyRow.ttl_until < at)
            res = await s.execute(stmt)
            await s.commit()
            return int(res.rowcount or 0)
