from shared.kafka import KafkaConsumer, KafkaProducer


def test_producer_consumer_importable():
    p = KafkaProducer("kafka:9092")
    c = KafkaConsumer("kafka:9092", group="g1", topics=["ledger.events"])
    assert p is not None
    assert c is not None


def test_producer_dlq_naming():
    p = KafkaProducer("kafka:9092")
    assert f"{p.dlq_prefix or 'ledger.commands'}.dlq" == "ledger.commands.dlq"


def test_idempotency_middleware_importable():
    from shared.idempotency import IdempotencyMiddleware

    app = object()
    m = IdempotencyMiddleware(app, "redis://localhost:6379/0")
    assert m.PENDING_TTL == 120
    assert m.ANSWER_TTL == 86400
