"""SSE reconnect + idempotent replay (phase-5 spec §4.4, §6, §7; commit #7).

Verifies the *server-side* contract that makes the browser ``EventSource``
auto-reconnect work (spec §4.4 pitfall #9):

  * A client that disconnects then reconnects can still receive events —
    the relay drops the stale subscription and accepts a fresh one without
    leak or stuck queue.
  * A slow client that missed events while disconnected does not slow down
    the newly-connected clients (QueueFull drop-on-full, spec pitfall #7).
  * ``SSEStream.unsubscribe()`` is idempotent: disconnecting twice (browser
    ``onclose`` + manual ``close()``) drops the subscription only once.

Note: Phase 6's frontend is React/Vite (not in this repo). The browser's
built-in ``EventSource`` auto-reconnect is a *client* behaviour that depends
on this *server* contract being correct — which is what these tests pin down.
"""
from __future__ import annotations

import asyncio

from services.notification.sse import SSERelay, SSEStream


def _ev(pid: str, aid: str = "U001") -> dict:
    return {"payment_id": pid, "account_id": aid, "event": "FundsDebited",
            "amount": {"value": 100, "currency": "USD"},
            "new_balance": {"value": 619900, "currency": "USD"},
            "ts": "2026-10-19T12:00:00+00:00", "event_id": f"E-{pid}"}


async def test_reconnect_after_disconnect_sees_new_events():
    """A client that disconnects and reconnects still receives events.

    This is the server-side contract that makes the browser EventSource's
    built-in auto-reconnect (spec §4.4, pitfall #9) work end-to-end: the relay
    must accept a *new* subscription after an *old* one is dropped.
    """
    relay = SSERelay()

    # first subscriber connects, then disconnects (browser close / network drop)
    q1 = relay.subscribe()
    s1 = SSEStream(q1, relay, heartbeat_s=0.05)
    assert relay.subscriber_count == 1
    s1.unsubscribe()                              # client disconnect → drop
    assert relay.subscriber_count == 0

    # new subscriber connects (EventSource auto-reconnect)
    q2 = relay.subscribe()
    s2 = SSEStream(q2, relay, heartbeat_s=0.05)
    assert relay.subscriber_count == 1

    # push an event — only the *new* subscriber should get it
    relay.broadcast(_ev("P-NEW"))
    assert q2.qsize() == 1
    # and the old (disconnected) subscriber is gone (no leak, no stale send)
    assert relay.subscriber_count == 1

    # cleanup
    s2.unsubscribe()
    assert relay.subscriber_count == 0


async def test_reconnect_does_not_leak_subscriptions():
    """100 disconnect/reconnect cycles leave ZERO subscribers (no leak)."""
    relay = SSERelay()
    for i in range(100):
        q = relay.subscribe()
        s = SSEStream(q, relay, heartbeat_s=0.05)
        assert relay.subscriber_count == 1
        s.unsubscribe()
        assert relay.subscriber_count == 0
    assert relay.subscriber_count == 0            # the EXIT: no leak


async def test_reconnect_after_slow_client_drop_is_clean():
    """A slow client that dropped (queue full) must not stall a new subscriber."""
    relay = SSERelay()
    relay._subscribers.clear()
    # Simulate a slow client whose queue overflowed (still subscribed, full).
    slow: asyncio.Queue = asyncio.Queue(maxsize=1)
    fast: asyncio.Queue = asyncio.Queue(maxsize=10)
    relay._subscribers.append(slow)
    relay._subscribers.append(fast)
    slow.put_nowait(_ev("SLOW-0"))                # fill the slow queue

    # New "reconnected" events — the slow one should drop, the fast one gets them.
    for i in range(5):
        relay.broadcast(_ev(f"E-{i}"))
    assert slow.qsize() == 1                       # did NOT block; still exactly 1
    assert fast.qsize() == 5                       # all 5 delivered (spec §7 pitfall #7)


async def test_unsubscribe_is_idempotent():
    """A double unsubscribe (browser onclose + manual close) must not error."""
    relay = SSERelay()
    q = relay.subscribe()
    s = SSEStream(q, relay, heartbeat_s=0.05)
    s.unsubscribe()                                # first call: actually drop
    s.unsubscribe()                                # second call: no-op
    assert relay.subscriber_count == 0
    s.unsubscribe()                                # third call: still no-op
    assert relay.subscriber_count == 0
