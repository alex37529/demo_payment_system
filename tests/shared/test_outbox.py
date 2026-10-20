from shared.outbox import OutboxEvent, OutboxMixin


def test_outbox_event_from_event(event):
    row = OutboxEvent(event=event, topic="ledger.events")
    assert row.topic == "ledger.events"
    assert row.event_id == str(event.event_id)
    assert row.payload["payment_id"] == "P1"


def test_outbox_event_dict(event):
    row = OutboxEvent(event={"type": "X"}, topic="t")
    assert row.payload == {"type": "X"}
    assert len(row.event_id) == 36  # generated uuid


def test_outbox_event_is_append_only(event):
    """The outbox row must NOT carry a delivery state.

    Every UPDATE of a captured table is another change event for Debezium —
    i.e. another message in `ledger.events` for the SAME event. The delivery
    state therefore lives in `outbox_delivery` (ADR-008).
    """
    from shared.outbox import FAILED, PENDING, PUBLISHED, STATUS_VALUES

    assert STATUS_VALUES == (PENDING, PUBLISHED, FAILED)
    row = OutboxEvent(event=event, topic="ledger.events")
    for tracking_field in ("status", "published_at", "last_error"):
        assert not hasattr(row, tracking_field), (
            f"outbox row must not carry {tracking_field} — it is append-only"
        )
    assert hasattr(row, "created_at")


def test_outbox_delivery_carries_the_state():
    from shared.outbox import PENDING, OutboxDelivery

    delivery = OutboxDelivery(event_id="e-1", status=PENDING)
    assert delivery.event_id == "e-1"
    assert delivery.status == PENDING
    assert delivery.published_at is None
    assert delivery.last_error is None


class _UC(OutboxMixin):
    pass


def test_mixin_publish_adds_to_session(event):
    class FakeSession:
        def __init__(self):
            self.added = []

        def add(self, obj):
            self.added.append(obj)

    sess = FakeSession()
    _UC()._publish_outbox(sess, "ledger.events", event)
    assert len(sess.added) == 1
    assert isinstance(sess.added[0], OutboxEvent)
