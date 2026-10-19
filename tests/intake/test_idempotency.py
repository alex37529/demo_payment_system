"""IdempotencyService + IntakeService behaviour (phase-4 spec §3.3/§3.4).

Drives the service directly with the fakes (no HTTP) to assert the exact
state-machine of acquire / complete / get_cached / rollback, plus the dual-write
and the Redis-down PG fallback path.
"""
from __future__ import annotations

import pytest

from services.intake.application.dtos import CreatePaymentRequest
from services.intake.application.use_case import KafkaUnavailable

REQ = {"amount": 100, "currency": "USD", "account_id": "A1"}


async def test_fresh_request_publishes_once_and_cached(harness):
    svc = harness.service
    status, body = await svc.create(CreatePaymentRequest(**REQ), idem_key="K1")
    assert status == 202
    assert body["status"] == "accepted" and body["payment_id"]
    # exactly one command on the wire
    assert len(harness.publisher.published) == 1
    topic, key, cmd = harness.publisher.published[0]
    assert topic == "payments.commands" and key == body["payment_id"]
    assert cmd["cmd"] == "Create" and cmd["payment_id"] == body["payment_id"]


async def test_replay_returns_cached_no_republish(harness):
    svc = harness.service
    first = await svc.create(CreatePaymentRequest(**REQ), idem_key="K1")
    second = await svc.create(CreatePaymentRequest(**REQ), idem_key="K1")
    assert first[0] == 202 and second[0] == 200
    # same payment_id, and NO second Kafka message (the exit criterion)
    assert first[1]["payment_id"] == second[1]["payment_id"]
    assert len(harness.publisher.published) == 1


async def test_replay_after_cache_lost_is_409(harness):
    """Redis dropped the answer AND PG lost it too — key exists but no answer.

    Models the "acquire succeeded, complete hasn't yet run (or was rolled back
    but the redis marker still lingers)" window. We force that state by writing
    a `pending` value to Redis (simulating the acquire-side state) and clearing
    the PG row. The service should refuse to treat this as fresh (409).
    """
    svc = harness.service
    # First request succeeded (202 + cached), then something reset the state:
    await svc.create(CreatePaymentRequest(**REQ), idem_key="K1")

    # Reset: as if the key was re-issued (rolled back) but the answer is gone.
    harness.redis.store["idem:K1"] = "pending"   # acquire marker, no answer
    await harness.repo.clear_key("K1")           # PG row present, response NULL

    # Fresh request now: acquire -> False (Redis says "already there"), then
    # cross-check PG (response=None, not a real answer) -> treat as in-progress.
    status, body = await svc.create(CreatePaymentRequest(**REQ), idem_key="K1")
    assert status == 409
    # exactly one command was ever published (the original)
    assert len(harness.publisher.published) == 1


async def test_redis_down_acquires_via_pg(harness):
    """If Redis is down, the PG `intake.idempotency_keys` row still gates.

    Note: after `complete`, the PG row holds the real response. A replay is
    detected because the row has a non-NULL response — the same "new / answer
    missing" distinction as the Redis path.
    """
    harness.redis.fail = True
    svc = harness.service
    status, _ = await svc.create(CreatePaymentRequest(**REQ, payment_id="P-PG"),
                                 idem_key="KPG")
    assert status == 202
    # the durable mirror now holds the final answer
    cached = await harness.repo.get_response("KPG")
    assert cached is not None and cached["status"] == "accepted"
    # replay detected via PG (no second publish)
    status2, body2 = await svc.create(CreatePaymentRequest(**REQ), idem_key="KPG")
    assert status2 == 200 and body2["payment_id"] == "P-PG"
    assert len(harness.publisher.published) == 1


async def test_pg_rollback_releases_for_retry(harness):
    """Kafka down on the PG path: rollback clears the marker, retry succeeds."""
    harness.redis.fail = True
    svc = harness.service

    class _FailPublisher:
        async def publish(self, topic, key, value):
            raise RuntimeError("kafka down (test)")

    svc.pub = _FailPublisher()
    from services.intake.application.use_case import KafkaUnavailable as _KA
    with pytest.raises(_KA):
        await svc.create(CreatePaymentRequest(**REQ), idem_key="KPG-RB")
    # the row is back to a "new" marker (response cleared)
    assert await harness.repo.get_response("KPG-RB") is None

    svc.pub = harness.publisher
    status, _ = await svc.create(CreatePaymentRequest(**REQ), idem_key="KPG-RB")
    assert status == 202
    assert len(harness.publisher.published) == 1


async def test_kafka_fail_rolls_back_key(harness):
    """Kafka down -> 503, and the key is released so the client can retry."""
    harness.publisher.fail = True
    svc = harness.service
    with pytest.raises(KafkaUnavailable):
        await svc.create(CreatePaymentRequest(**REQ), idem_key="K503")
    assert harness.publisher.published == []               # nothing published
    # key released: a later (Kafka-recovered) attempt is a fresh request again
    harness.publisher.fail = False
    status, _ = await svc.create(CreatePaymentRequest(**REQ), idem_key="K503")
    assert status == 202
    assert len(harness.publisher.published) == 1


async def test_different_keys_are_independent(harness):
    svc = harness.service
    a = await svc.create(CreatePaymentRequest(**REQ), idem_key="KA")
    b = await svc.create(CreatePaymentRequest(amount=200, account_id="A2"),
                         idem_key="KB")
    assert a[1]["payment_id"] != b[1]["payment_id"]
    assert len(harness.publisher.published) == 2


async def test_get_by_payment_returns_cached(harness):
    srv = harness.repo
    await harness.service.create(CreatePaymentRequest(amount=100, currency="USD",
                                                      account_id="A1",
                                                      payment_id="PP1"),
                                 idem_key="KP1")
    got = await srv.get_by_payment("PP1")
    assert got is not None and got["payment_id"] == "PP1"
    assert (await srv.get_by_payment("NOPE")) is None
