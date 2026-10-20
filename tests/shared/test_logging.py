"""The JSON log formatter (phase-7: the logs pillar).

The formatter is what makes the application's OWN log records reach stdout.
Before it, uvicorn's default logging config left the root logger at
WARNING, so every `log.info(...)` of a service was silently dropped — the
logs pillar carried nothing but the HTTP access lines.
"""
from __future__ import annotations

import json
import logging

from shared.observability import JsonLogFormatter


def _record(msg: str, level: int = logging.INFO, **extra) -> logging.LogRecord:
    rec = logging.LogRecord("svc.mod", level, __file__, 1, msg, None, None)
    for k, v in extra.items():
        setattr(rec, k, v)
    return rec


def test_formatter_emits_the_canonical_json_shape():
    out = JsonLogFormatter("intake").format(_record("hello"))
    data = json.loads(out)
    assert data["level"] == "INFO"
    assert data["service"] == "intake"
    assert data["msg"] == "hello"
    assert "ts" in data


def test_formatter_keeps_the_level():
    data = json.loads(JsonLogFormatter("x").format(_record("boom", logging.ERROR)))
    assert data["level"] == "ERROR"


def test_formatter_carries_application_extras():
    """`extra={...}` (e.g. the alert fields) must ride along — otherwise the
    alert sink in the gateway would lose the alertname/severity."""
    rec = _record("ALERT X", alertname="CircuitBreakerOpen", severity="critical")
    data = json.loads(JsonLogFormatter("gateway").format(rec))
    assert data["alertname"] == "CircuitBreakerOpen"
    assert data["severity"] == "critical"


def test_formatter_does_not_leak_standard_attributes():
    data = json.loads(JsonLogFormatter("x").format(_record("m")))
    # The LogRecord internals must not pollute the log line.
    assert "module" not in data
    assert "pathname" not in data
    assert "msecs" not in data


def test_formatter_is_json_serialisable_for_exotic_values():
    rec = _record("weird", obj=object())
    data = json.loads(JsonLogFormatter("x").format(rec))  # must not raise
    assert data["msg"] == "weird"


def test_setup_logging_adds_the_handler_once():
    """A second app (two service test suites in one pytest process) must not
    add a second handler — that would double every log line."""
    from shared.observability import _setup_logging

    root = logging.getLogger()
    before = len(root.handlers)
    _setup_logging("svc-a")
    _setup_logging("svc-b")
    after = len(root.handlers)
    assert after - before <= 1
    assert root.level <= logging.INFO
