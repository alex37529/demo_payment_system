"""Core DDD building blocks: ValueObject, Entity, AggregateRoot.

Invariants live on the aggregate; state changes only through its methods.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass


@dataclass(frozen=True)
class ValueObject:
    """Immutable value object; equality by value (not identity).

    Required for Money/Currency so that Money(100,'USD') == Money(100,'USD').
    """

    def __hash__(self) -> int:  # type: ignore[override]
        return hash(tuple(getattr(self, f) for f in self.__dataclass_fields__))

    def __eq__(self, other: object) -> bool:
        if type(self) is not type(other):
            return NotImplemented
        return self.__dict__ == other.__dict__


class Entity:
    """Entity with identity (id). Equality by id."""

    def __init__(self, entity_id: uuid.UUID | None = None) -> None:
        self.id = entity_id or uuid.uuid4()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Entity):
            return NotImplemented
        return self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)


class AggregateRoot(Entity):
    """Aggregate root: transaction + invariant boundary, collects domain events.

    Events are produced by business methods and drained with `pull_events()`
    (typically into the Outbox within the same transaction).
    """

    def __init__(self, entity_id: uuid.UUID | None = None) -> None:
        super().__init__(entity_id)
        self._events: list[object] = []

    def add_event(self, event: object) -> None:
        self._events.append(event)

    def pull_events(self) -> list[object]:
        out, self._events = self._events, []
        return out
