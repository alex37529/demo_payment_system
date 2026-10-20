"""The orchestrator's event dispatch must call each handler with the shape it
accepts.

A live finding (phase 9): the dispatch called EVERY handler as
``method(key, payload)``, but `on_compensation_applied` also requires the
`event_type` keyword. The resulting `TypeError` was swallowed by the
consumer's `except Exception` — and because the service's own log records
were being dropped entirely (see the phase-7 logging fix), the failure was
INVISIBLE: a confirmed compensation was never recorded, for the whole life
of the project.

These tests pin the two call shapes so the two cannot be confused again.
"""
from __future__ import annotations

import inspect

import pytest

from services.orchestrator.application.saga_handler import SagaHandler
from services.orchestrator.kafka_consumer import EVENT_DISPATCH


def _handlers_needing_event_type() -> set[str]:
    """The handlers whose signature requires the `event_type` keyword."""
    out = set()
    for name in EVENT_DISPATCH.values():
        fn = getattr(SagaHandler, name, None)
        if fn is None:
            continue
        params = inspect.signature(fn).parameters
        if "event_type" in params and params["event_type"].kind is inspect.Parameter.KEYWORD_ONLY:
            out.add(name)
    return out


def test_only_the_compensation_handler_needs_the_event_type():
    assert _handlers_needing_event_type() == {"on_compensation_applied"}


@pytest.mark.parametrize("event_type", ["CancelReservationPosted", "RefundPosted"])
def test_compensation_handler_requires_the_event_type(event_type: str):
    """The regression: calling it without `event_type` raised a TypeError."""
    fn = SagaHandler.on_compensation_applied
    with pytest.raises(TypeError):
        inspect.signature(fn).bind(None, "key", {})  # no event_type -> TypeError
    # and WITH it, the call is valid
    inspect.signature(fn).bind(None, "key", {}, event_type=event_type)


def test_dispatch_covers_the_compensation_events():
    assert EVENT_DISPATCH["CancelReservationPosted"] == "on_compensation_applied"
    assert EVENT_DISPATCH["RefundPosted"] == "on_compensation_applied"
