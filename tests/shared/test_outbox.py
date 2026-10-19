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
