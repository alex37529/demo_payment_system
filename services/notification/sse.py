"""SSE relay — fan-out of 1 event to N subscribers (phase-5 spec §4.2/§4.3).

Protocol (RFC 6202 / WHATWG EventSource):
  * ``text/event-stream``; each message is ``data: <json>\\n\\n``.
  * Idle streams must emit a comment (``: keepalive\\n\\n``) at least every
    ``heartbeat_s`` (15s by default) so proxies / browsers don't idle-close
    (pitfall #6 — proxy idle timeout ~30-60s).
  * ``X-Accel-Buffering: no`` disables nginx response buffering (pitfall #8).
  * Slow clients (queue full) drop their *newest* event, not the rest of the
    broadcast (pitfall #7 — head-of-line blocking protection: one slow
    subscriber must not stall every other subscriber).

Cancellation safety: when the client disconnects uvicorn closes the async
generator; ``GeneratorExit`` / ``CancelledError`` must unwind ``stream`` so
the ``finally`` block drops the subscription. Any other exception (e.g. a
consumer bug) also cleans up.
"""
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any


class SSERelay:
    """Fan-out: one event -> N async-queue subscribers (with safe cleanup)."""

    def __init__(self) -> None:
        self._subscribers: list[asyncio.Queue[dict[str, Any]]] = []

    def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=100)
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue[dict[str, Any]]) -> None:
        if q in self._subscribers:
            self._subscribers.remove(q)

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    def broadcast(self, event: dict[str, Any]) -> None:
        """Push ``event`` to every subscriber. Slow subscribers (queue full)
        lose their *latest* event but do not block the broadcast loop — see
        pitfall #7 in the phase-5 spec.
        """
        for q in list(self._subscribers):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass  # drop this event for this subscriber; keep going


def shape_hint(event: dict[str, Any]) -> str:
    """Best-effort canonical event name, used only for the read-side feed.

    Prefers the explicit ``event_type`` discriminator (``shared/domain/events.py
    DomainEvent.to_json``); otherwise infers by shape (mirrors the phase-3
    orchestrator heuristic) so old rows still classify correctly.
    """
    et = event.get("event_type")
    if et:
        return str(et)
    if "new_balance" in event:
        return "FundsDebited"
    if "reason" in event:
        return "CancelReservationPosted"
    if "amount" in event:
        return "FundsReserved"
    return ""


def format_sse(event: dict[str, Any]) -> str:
    """Encode one event as an SSE message: ``data: <json>\\n\\n``.

    Newlines inside ``data`` would terminate the frame early and corrupt the
    stream — replace them with their unicode escapes before serializing.
    """
    safe = {k: (v.replace("\n", "\\n").replace("\r", "\\r") if isinstance(v, str) else v)
            for k, v in event.items()}
    return f"data: {json.dumps(safe, default=str, ensure_ascii=False)}\n\n"


class SSEStream:
    """Async generator over a subscriber queue with periodic keepalive.

    Yields SSE frames — either an event (``data: ...``) or a heartbeat
    (``: keepalive``) — until the client disconnects (uvicorn closes the
    generator) or an exception unwinds it. Cleanup is unconditional in the
    ``finally`` block so a disconnect never leaks a subscription.
    """

    def __init__(self, queue: asyncio.Queue[dict[str, Any]],
                 relay: SSERelay,
                 heartbeat_s: float = 15.0) -> None:
        self._q = queue
        self._relay = relay
        # Floor: never busy-wait below 0.05s. The tests set SSE_HEARTBEAT_S=0.2
        # to run fast — the floor keeps them from looping tight.
        self._hb = max(0.05, heartbeat_s)
        self._unsubscribed = False

    def unsubscribe(self) -> None:
        """Idempotently drop the subscription (public cleanup entry point).

        Called by the generator ``finally`` on disconnect, OR by the caller
        directly (tests, or an API layer that closes without consuming a
        frame). Idempotent: repeated calls are no-ops.
        """
        if not self._unsubscribed:
            self._relay.unsubscribe(self._q)
            self._unsubscribed = True

    def frames(self) -> AsyncIterator[str]:
        """Async generator yielding one SSE frame at a time.

        ``frames()`` returns an async iterator; ``for await frame in
        SSEStream(...).frames(): ...`` pulls frames as events / heartbeats
        arrive. Cleanup (unsubscribe) is unconditional in the finally block,
        so a disconnect / cancel / error never leaks a subscription.
        """
        return self._frames()

    async def _frames(self) -> AsyncIterator[str]:
        try:
            while True:
                try:
                    # ``wait_for(q.get(), hb)``: blocks until an event arrives,
                    # or times out → keepalive. No latency loss, no busy-wait.
                    evt = await asyncio.wait_for(self._q.get(), timeout=self._hb)
                except TimeoutError:
                    yield ": keepalive\n\n"   # pitfall #6: keep the line alive
                    continue
                yield format_sse(evt)
        finally:
            self.unsubscribe()
