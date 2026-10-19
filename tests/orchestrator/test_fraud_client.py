"""fraud_client tests — the real `CircuitBreakerHttpClient` + our `make_fraud_client`
factory + the fallback function (the phase-3 §9 acceptance: CB open → route_to_dlq).
"""
from __future__ import annotations

import pytest
from unittest import mock

import httpx

from services.orchestrator.infrastructure.fraud_client import (
    _fraud_fallback,
    fraud_request,
    make_fraud_client,
)


async def test_make_fraud_client_returns_working_cb_client():
    client, url = make_fraud_client("http://mock-fraud:8080/")
    assert url == "http://mock-fraud:8080/fraud/check"
    assert client.fallback is _fraud_fallback
    assert client.cb and client.client  # cb + httpx.AsyncClient are set
    await client.close()


async def test_fallback_returns_route_to_dlq_shape():
    out = await _fraud_fallback("http://x/fraud/check", {"payment_id": "P1"})
    assert out["decision"] == "route_to_dlq"
    assert out["score"] == 100.0
    assert out["reason"] == "circuit_open_or_error"


async def test_fraud_request_includes_payment_account_amount():
    body = fraud_request({"payment_id": "P42", "amount": {"value": 100, "currency": "USD"},
                          "account_id": "A1"})
    assert body == {"payment_id": "P42", "account_id": "A1",
                    "amount": {"value": 100, "currency": "USD"}}


async def test_cb_opens_after_fail_max_under_errors_mode():
    """Real aiobreaker integration: 500 response from httpx → counted as breaker
    failure → CB opens after `fail_max` consecutive failures → fallback used."""
    client, url = make_fraud_client("http://127.0.0.1:9", fail_max=3, reset_timeout=60,
                                     timeout=0.2)

    async def fake_post(self, url, json):
        return httpx.Response(500, request=httpx.Request("POST", url), text="boom")

    with mock.patch.object(httpx.AsyncClient, "post", fake_post):
        for _ in range(5):
            out = await client.post(url, {"payment_id": "P1"})
            # once the CB is open, the fallback is what comes back
        assert client.is_open() is True
        out = await client.post(url, {"payment_id": "P_OPEN"})
        assert out["decision"] == "route_to_dlq"  # fallback fired

    await client.close()


async def test_cb_happy_path_passes_decision_through():
    """When the fraud API responds 200 with a decision, the CB lets it through."""
    client, url = make_fraud_client("http://mock", fail_max=5)

    async def fake_post(self, u, json):
        return httpx.Response(200, request=httpx.Request("POST", u),
                              json={"decision": "approve", "score": 0.2})

    with mock.patch.object(httpx.AsyncClient, "post", fake_post):
        out = await client.post(url, {"payment_id": "P_OK"})
        assert out["decision"] == "approve"
        assert client.is_open() is False

    await client.close()
