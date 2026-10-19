"""Circuit Breaker HTTP client (§3.7 TЗ v2).

Library: `aiobreaker`. Its async entry point is `cb.call_async(func)` — there is
NO `__aenter__`/`__aexit__`, so the v1 `async with cb` snippet would not work on
either pybreaker (sync, no dunder) or aiobreaker (no dunder). The correct pattern is
`await cb.call_async(self._post)` (see shared tests / phase 3 fraud step).

`pybreaker` (v1) is sync-only; we moved to aiobreaker because it drives the async
httpx client (ADR-004).

Canonical metric name for Prometheus is `pybreaker_open` (convention kept for
acceptance scripts).
"""
from __future__ import annotations

import datetime
import logging
from typing import Any, Awaitable, Callable

import httpx
from aiobreaker import CircuitBreaker, CircuitBreakerError

log = logging.getLogger(__name__)

Fallback = Callable[[str, dict], Awaitable[Any]]


class _BreakerHttpError(Exception):
    """Wraps non-2xx HTTP responses so aiobreaker counts them as failures."""


class CircuitBreakerHttpClient:
    """Async circuit-breaker wrapper over `httpx.AsyncClient`."""

    def __init__(
        self,
        fallback: Fallback | None = None,
        *,
        fail_max: int = 5,
        reset_timeout: float = 30.0,  # seconds until half-open probe
        timeout: float = 3.0,  # httpx per-request timeout (MUST be < fraud-fail injection 5s)
    ) -> None:
        self.cb = CircuitBreaker(
            fail_max=fail_max,
            timeout_duration=datetime.timedelta(seconds=reset_timeout),
        )
        self.client = httpx.AsyncClient(timeout=timeout)
        self.fallback = fallback

    async def _post(self, url: str, json: dict) -> Any:
        r = await self.client.post(url, json=json)
        if r.status_code >= 500:
            raise _BreakerHttpError(r.status_code)  # count as breaker failure
        r.raise_for_status()
        return r.json()

    async def post(self, url: str, json: dict, *, tags: dict | None = None) -> Any:
        try:
            return await self.cb.call_async(self._post, url, json)
        except CircuitBreakerError:
            log.warning("CB OPEN for %s -> fallback", url, extra=tags)
            if self.fallback:
                return await self.fallback(url, json)
            raise
        except (_BreakerHttpError, httpx.HTTPError) as e:
            log.warning("HTTP error %s: %s -> fallback", url, e, extra=tags)
            if self.fallback:
                return await self.fallback(url, json)
            raise

    def state(self) -> str:
        """'CLOSED' | 'OPEN' | 'HALF_OPEN' — for UI / tests / Prometheus label."""
        return self.cb.current_state.name  # CircuitBreakerState enum

    def is_open(self) -> bool:
        return self.state() == "OPEN"

    async def close(self) -> None:
        await self.client.aclose()
