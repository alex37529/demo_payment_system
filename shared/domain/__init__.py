"""shared.domain — DDD base primitives + canonical event catalog.

`Aggregate` is a backward-compat alias for `AggregateRoot` (v2 exit criterion
requires `from shared.domain import Aggregate`).
"""
from .base import AggregateRoot, Entity, ValueObject
from .events import (
    CancelReservationPosted,
    DebitFailed,
    DomainEvent,
    FraudVerdict,
    FundsDebited,
    FundsReserved,
    LimitsChecked,
    PaymentCreated,
    RefundPosted,
    SagaFailed,
    SagaSucceeded,
)

Aggregate = AggregateRoot  # backward-compat alias

__all__ = [
    "Aggregate",
    "AggregateRoot",
    "CancelReservationPosted",
    "DebitFailed",
    "DomainEvent",
    "Entity",
    "FraudVerdict",
    "FundsDebited",
    "FundsReserved",
    "LimitsChecked",
    "PaymentCreated",
    "RefundPosted",
    "SagaFailed",
    "SagaSucceeded",
    "ValueObject",
]
