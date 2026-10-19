"""Orchestrator bounded-context domain (saga state machine + aggregate).

No FastAPI / SQLAlchemy / httpx imports here — the domain is pure:
a python-statemachine FSM (the single authority on state transitions),
the Saga aggregate (id + history + payload + compensation plan), and the
compensation map (which reverse-commands each step needs on failure).
"""
from .compensation import COMPENSATIONS, compensation_for
from .saga import (
    Saga,
    SagaError,
    SagaNotFound,
    UnsupportedCommand,
    normalize_amount,
)
from .saga_fsm import SagaState, SagaStateMachine

__all__ = [
    "COMPENSATIONS",
    "Saga",
    "SagaError",
    "SagaNotFound",
    "SagaState",
    "SagaStateMachine",
    "UnsupportedCommand",
    "compensation_for",
    "normalize_amount",
]
