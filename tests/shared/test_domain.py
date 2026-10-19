from dataclasses import dataclass

from shared.domain import Aggregate, AggregateRoot, Entity, ValueObject


@dataclass(frozen=True)
class Money(ValueObject):
    value: int = 0
    currency: str = "USD"


def test_value_object_eq_by_value():
    assert Money(100, "USD") == Money(100, "USD")
    assert Money(100, "USD") != Money(100, "EUR")


def test_value_object_hash_consistent():
    assert hash(Money(1, "USD")) == hash(Money(1, "USD"))


def test_entity_identity():
    a, b = Entity(), Entity()
    assert a != b
    assert a == Entity(a.id)


def test_aggregate_alias():
    assert Aggregate is AggregateRoot


def test_aggregate_collect_and_pull_events():
    agg = Aggregate()
    agg.add_event("e1")
    agg.add_event("e2")
    assert agg.pull_events() == ["e1", "e2"]
    assert agg.pull_events() == []


def test_all_event_classes_importable():
    from shared.domain import events as ev

    names = [
        "DomainEvent", "PaymentCreated", "LimitsChecked", "FraudVerdict", "DebitFailed",
        "FundsReserved", "FundsDebited", "CancelReservationPosted", "RefundPosted",
        "SagaFailed", "SagaSucceeded",
    ]
    for n in names:
        assert getattr(ev, n, None) is not None, f"missing event class: {n}"


def test_event_to_json_serializable(event):
    import json

    d = event.to_json()
    assert d["account_id"] == "A1"
    assert json.dumps(d)  # must be JSON-serializable (datetime handled)


def test_event_frozen(event):
    import dataclasses

    assert dataclasses.is_dataclass(event)
    # frozen -> assignment should fail
    try:
        event.account_id = "X"  # type: ignore
        raise AssertionError("event should be frozen")
    except dataclasses.FrozenInstanceError:
        pass
