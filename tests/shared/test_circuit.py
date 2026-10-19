import asyncio

import pytest

from shared.circuit import CircuitBreakerHttpClient


def test_state_initial_closed():
    cb = CircuitBreakerHttpClient(fail_max=3)
    assert cb.state() == "CLOSED"
    assert cb.is_open() is False


def test_breaker_opens_after_fail_max_and_fallback():
    async def main():
        hits = {"n": 0}

        async def fallback(url, payload):
            hits["n"] += 1
            return {"decision": "route_to_dlq"}

        # fail_max=2, timeout very small so connection fails fast.
        cb = CircuitBreakerHttpClient(fallback=fallback, fail_max=2, reset_timeout=60, timeout=0.2)
        for _ in range(4):
            try:
                await cb.post("http://127.0.0.1:9/x", {"a": 1})
            except Exception:
                pass
        assert cb.is_open() is True
        assert hits["n"] >= 1  # fallback was used
    asyncio.run(main())


def test_breaker_counts_5xx_as_failure():
    from unittest import mock

    import httpx

    async def main():
        async def fake_post(self, url, json):
            return httpx.Response(500, request=httpx.Request("POST", url), text="boom")

        cb = CircuitBreakerHttpClient(fail_max=1, timeout=0.2)
        with mock.patch.object(httpx.AsyncClient, "post", fake_post):
            try:
                await cb.post("http://x/y", {})
            except Exception:
                pass
        assert cb.is_open() is True
    asyncio.run(main())
