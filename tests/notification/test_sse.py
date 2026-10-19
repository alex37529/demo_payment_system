"""SSE relay + stream tests (phase-5 spec §4.2/§4.3/§4.4, pitfall #6/#7).

What we test (each unit-test-friendly, no socket, no broker):
  * ``format_sse`` emits the right frame for a given event dict.
  * The relay does N-fan-out (1 event -> N subscribers) without blocking.
  * A slow client's queue overflow (QueueFull) does NOT block broadcast.
  * ``SSERelay.subscribe`` / ``unsubscribe`` are symmetric (cleanup works).
  * ``SSEStream._frames`` yields the right frames for an event and an idle
    heartbeat — exercised without needing real HTTP streaming (the
    httpx ASGI transport doesn't handle long-lived streaming in unit tests).

The ``/v1/stream`` HTTP endpoint itself is exercised separately in the
"live smoke" step (curl -N) since it requires a real socket.
"""
from __future__ import annotations

import asyncio
import json

from services.notification.sse import SSERelay, SSEStream, format_sse


def _ev(pid: str, aid: str = "U001") -> dict:
    return {"payment_id": pid, "account_id": aid, "event": "FundsDebited",
            "amount": {"value": 100, "currency": "USD"},
            "new_balance": {"value": 619900, "currency": "USD"},
            "ts": "2026-10-19T12:00:00+00:00", "event_id": f"E-{pid}"}


# --------------------------------------------------------------------------- #
# format_sse (unit, no broker, no server)
# --------------------------------------------------------------------------- #
def test_format_sse_data_frame():
    evt = _ev("P1")
    frame = format_sse(evt)
    assert frame.startswith("data: ")
    assert frame.endswith("\n\n")
    body = frame[len("data: "):-2]
    parsed = json.loads(body)
    assert parsed["payment_id"] == "P1"
    assert parsed["account_id"] == "U001"
    assert parsed["event"] == "FundsDebited"


def test_format_sse_newline_escape():
    # A raw \n inside `data` would corrupt the SSE frame. `format_sse` escapes
    # newlines so the wire stays one frame.
    evt = {"payment_id": "P1", "amount": "100\n200"}
    frame = format_sse(evt)
    assert frame.count("\n\n") == 1            # one trailing blank line
    payload = frame[len("data: "):-2]
    assert "\n" not in payload                  # no raw newlines inside the data
    assert "\\n" in payload                     # escaped newline present


# --------------------------------------------------------------------------- #
# Relay fan-out + QueueFull drop-on-full (pitfall #7)
# --------------------------------------------------------------------------- #
async def test_broadcast_fanout_to_all_subscribers():
    relay = SSERelay()
    q1 = relay.subscribe()
    q2 = relay.subscribe()
    q3 = relay.subscribe()
    assert relay.subscriber_count == 3

    relay.broadcast(_ev("P1"))
    assert q1.qsize() == 1
    assert q2.qsize() == 1
    assert q3.qsize() == 1

    relay.unsubscribe(q1)
    assert relay.subscriber_count == 2

    relay.broadcast(_ev("P2"))
    assert q1.qsize() == 1                       # dropped (unsubscribed)
    assert q2.qsize() == 2                       # appended
    assert q3.qsize() == 2


async def test_broadcast_slow_client_does_not_block_others():
    """QueueFull on ONE slow client must not block the rest (pitfall #7)."""
    relay = SSERelay()
    relay._subscribers.clear()
    slow: asyncio.Queue = asyncio.Queue(maxsize=1)
    fast: asyncio.Queue = asyncio.Queue(maxsize=10)
    relay._subscribers.append(slow)
    relay._subscribers.append(fast)
    slow.put_nowait(_ev("P0"))                    # fill the slow queue
    for i in range(5):
        relay.broadcast(_ev(f"P{i}"))             # slow should DROP each
    assert slow.qsize() == 1                      # did NOT block; 1 stays there
    assert fast.qsize() == 5                      # all delivered to fast


# --------------------------------------------------------------------------- #
# SSEStream._frames (the async generator behind /v1/stream, tested directly)
# --------------------------------------------------------------------------- #
async def test_sse_stream_event_then_keepalive():
    """The frame generator yields an event frame, then a keepalive on idle."""
    q: asyncio.Queue = asyncio.Queue(maxsize=5)
    relay = SSERelay()
    relay._subscribers.append(q)                  # bypass relay.subscribe()
    stream = SSEStream(q, relay, heartbeat_s=0.05)
    gen = stream.frames()

    # First: yield an event frame
    q.put_nowait(_ev("P1"))
    first = await _anext(gen)
    assert first.startswith("data: ")
    assert "P1" in first

    # Then: after a heartbeat timeout, a keepalive comment (pitfall #6)
    second = await _anext(gen, timeout=2.0)
    assert second == ": keepalive\n\n"

    # Generator cleanup drops the subscription.
    await gen.aclose()
    assert relay.subscriber_count == 0            # dropped on aclose


async def test_sse_stream_unsubscribe_idempotent():
    """Direct unsubscribe() on a stream must be idempotent (no leak, no error)."""
    relay = SSERelay()
    q = relay.subscribe()
    stream = SSEStream(q, relay, heartbeat_s=0.05)
    assert relay.subscriber_count == 1
    stream.unsubscribe()
    assert relay.subscriber_count == 0            # first call drops it
    stream.unsubscribe()                          # second call is a no-op
    assert relay.subscriber_count == 0


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
async def _anext(aiter, timeout: float = 5.0):
    """Await the next item from an async iterator with a timeout."""
    try:
        return await asyncio.wait_for(aiter.__anext__(), timeout=timeout)
    except StopAsyncIteration:
        raise AssertionError("stream raised StopAsyncIteration unexpectedly")
