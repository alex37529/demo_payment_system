"""shared.domain — DDD base primitives + canonical event catalog.

`Aggregate` is a backward-compat alias for `AggregateRoot` (v2 exit criterion
requires `from shared.domain import Aggregate`).
"""
from .base import AggregateRoot, Entity, ValueObject
from .events import (
    DomainEvent,
    CancelReservationPosted,
    DebitFailed,
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
    "Entity",
    "ValueObject",
    "DomainEvent",
    "PaymentCreated",
    "LimitsChecked",
    "FraudVerdict",
    "DebitFailed",
    "FundsReserved",
    "FundsDebited",
    "CancelReservationPosted",
    "RefundPosted",
    "SagaFailed",
    "SagaSucceeded",
]
