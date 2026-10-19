"""Intake HTTP API tests (phase-4 spec §3.4/§5/§6).

Uses the shared `client` fixture (FastAPI app wired to the in-memory PG / fake
Redis / fake publisher from conftest). Asserts the *external* behaviour:
  - 400 when `Idempotency-Key` is missing;
  - 202 + payment_id on the first request;
  - 200 + same payment_id on the same key (the exit criterion);
  - 503 when Kafka is down — and the key is released for a retry;
  - 404 / 405 sanity for unknown paths;
  - 404 for `GET /v1/payments/{unknown}`.
"""
from __future__ import annotations

BODY = {"amount": 120, "currency": "USD", "account_id": "A1"}


async def test_missing_idempotency_key_is_400(client):
    r = await client.http.post("/v1/payments", json=BODY)
    assert r.status_code == 400
    assert r.json()["error"] == "Idempotency-Key required"


async def test_fresh_then_replay(client):
    r1 = await client.http.post("/v1/payments", json=BODY, headers={"Idempotency-Key": "K1"})
    assert r1.status_code == 202
    pid1 = r1.json()["payment_id"]
    assert r1.json()["status"] == "accepted"

    r2 = await client.http.post("/v1/payments", json=BODY, headers={"Idempotency-Key": "K1"})
    assert r2.status_code == 200
    assert r2.json()["payment_id"] == pid1                 # EXIT CRITERION
    assert len(client.publisher.published) == 1            # only 1 Kafka message


async def test_kafka_down_is_503_and_retry_recovers(client):
    client.publisher.fail = True
    r = await client.http.post(
        "/v1/payments", json=BODY, headers={"Idempotency-Key": "K-DOWN"})
    assert r.status_code == 503
    assert r.json()["error"] == "kafka_unavailable"
    assert client.publisher.published == []

    # retry after Kafka recovers -> a fresh request is accepted.
    client.publisher.fail = False
    r2 = await client.http.post(
        "/v1/payments", json=BODY, headers={"Idempotency-Key": "K-DOWN"})
    assert r2.status_code == 202
    assert len(client.publisher.published) == 1


async def test_unknown_method_and_path(client):
    assert (await client.http.get("/v1/payments/XYZ")).status_code == 404
    assert (await client.http.delete("/v1/payments")).status_code == 405
    assert (await client.http.post("/v1/unknown")).status_code == 404


async def test_get_payment_by_id(client):
    r = await client.http.post("/v1/payments",
                               json={**BODY, "payment_id": "P-CURL"},
                               headers={"Idempotency-Key": "K-GET"})
    assert r.status_code == 202
    g = await client.http.get("/v1/payments/P-CURL")
    assert g.status_code == 200 and g.json()["payment_id"] == "P-CURL"
    assert (await client.http.get("/v1/payments/MISSING")).status_code == 404
