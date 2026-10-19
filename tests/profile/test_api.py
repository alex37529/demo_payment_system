"""GET /v1/users API (phase-5 spec §3.3 + §6 red flag: '20 users seeded')."""
from __future__ import annotations

import pytest_asyncio


@pytest_asyncio.fixture
async def client(harness):
    """A FastAPI app bound to the SAME repo/DB as ``harness``, so the HTTP
    responses reflect the very same state the tests assert on (one shared
    in-memory SQLite engine — the intake conftest precedent)."""
    from httpx import ASGITransport, AsyncClient

    from services.profile.api import attach_profile, start, stop
    from services.shared_app import build_app

    app = build_app("profile-test")
    attach_profile(app, repo=harness["repo"])
    await start(app)
    async with AsyncClient(transport=ASGITransport(app=app),
                           base_url="http://test") as http:
        yield http
    await stop(app)


async def test_list_users_returns_seeded_rows(client):
    r = await client.get("/v1/users")
    assert r.status_code == 200
    body = r.json()
    assert len(body) == 3                       # 3 seeded in conftest
    first = body[0]
    for key in ("id", "name", "email", "currency", "balance"):
        assert key in first
    balances = {row["id"]: row["balance"] for row in body}
    assert balances["U001"] == 620000
    assert balances["U003"] == 350000


async def test_get_single_user(client):
    r = await client.get("/v1/users/U002")
    assert r.status_code == 200
    body = r.json()
    assert body["id"] == "U002"
    assert body["currency"] == "EUR"
    assert body["balance"] == 480000


async def test_get_user_not_found(client):
    r = await client.get("/v1/users/DOESNOTEXIST")
    assert r.status_code == 404
    assert r.json()["error"] == "UserNotFound"


async def test_balance_reflects_applied_event(client, harness):
    """The read API serves the balance AFTER the consumer applied an event."""
    await client.get("/v1/users/U001")           # prime
    await harness["repo"].apply_event(event_id="E-API", account_id="U001",
                                      new_balance=619900, event_type="FundsDebited")
    r = await client.get("/v1/users/U001")
    assert r.json()["balance"] == 619900
