"""Orchestrator application layer.

`SagaHandler` is the single coordination point — it consumes `payments.commands`
+ `ledger.events`, drives the saga state machine, and publishes commands/events +
compensations. `run_watchdog` catches sagas that get stuck in an in-flight state
(see ADR-005).
"""
from .saga_handler import (
    TERMINAL_STATES,
    TOPIC_DLQ,
    TOPIC_LEDGER_COMMANDS,
    TOPIC_LEDGER_EVENTS,
    TOPIC_PAYMENTS_COMMANDS,
    TOPIC_PAYMENTS_EVENTS,
    LedgerPublisher,
    SagaHandler,
)
from .watchdog import IN_FLIGHT_STATES, run_watchdog

__all__ = [
    "IN_FLIGHT_STATES",
    "TERMINAL_STATES",
    "TOPIC_DLQ",
    "TOPIC_LEDGER_COMMANDS",
    "TOPIC_LEDGER_EVENTS",
    "TOPIC_PAYMENTS_COMMANDS",
    "TOPIC_PAYMENTS_EVENTS",
    "LedgerPublisher",
    "SagaHandler",
    "run_watchdog",
]
