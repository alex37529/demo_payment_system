"""CreatePaymentRequest DTO tests (phase-4 spec §2, "validation = protection
before Kafka").

Covers the happy path plus each rejection class: non-positive amount, over-max
amount, bad currency, malformed account_id — and that `to_command()` always
carries a `payment_id` (client-supplied or a generated stable one).
"""
from __future__ import annotations

import pytest

from services.intake.application.dtos import CreatePaymentRequest


def test_valid_request_builds_command():
    req = CreatePaymentRequest(amount=100, currency="USD", account_id="A1")
    cmd = req.to_command()
    assert cmd["cmd"] == "Create"
    assert cmd["amount"] == {"value": 100, "currency": "USD"}
    assert cmd["account_id"] == "A1"
    assert cmd["client_ref"] is None
    assert cmd["payment_id"]                      # generated, non-empty


def test_client_supplied_payment_id_is_stable():
    a = CreatePaymentRequest(amount=100, account_id="A1", payment_id="P-123").to_command()
    b = CreatePaymentRequest(amount=100, account_id="A1", payment_id="P-123").to_command()
    # stable across retries with the same explicit id (idempotent GET lookup)
    assert a["payment_id"] == b["payment_id"] == "P-123"



def test_eur_currency_ok():
    cmd = CreatePaymentRequest(amount=250, currency="EUR", account_id="A-1_x").to_command()
    assert cmd["amount"]["currency"] == "EUR"
    assert cmd["account_id"] == "A-1_x"


def test_zero_amount_rejected():
    with pytest.raises(ValueError):
        CreatePaymentRequest(amount=0, account_id="A1")


def test_negative_amount_rejected():
    with pytest.raises(ValueError):
        CreatePaymentRequest(amount=-5, account_id="A1")


def test_over_max_amount_rejected():
    with pytest.raises(ValueError):
        CreatePaymentRequest(amount=100_001, account_id="A1")


def test_bad_currency_rejected():
    with pytest.raises(ValueError):
        CreatePaymentRequest(amount=100, currency="USD ", account_id="A1")


def test_lowercase_currency_rejected():
    """Strict scheme (pitfall #1): 'usd' is not 'USD' -> 422."""
    with pytest.raises(ValueError):
        CreatePaymentRequest(amount=100, currency="usd", account_id="A1")


@pytest.mark.parametrize("bad", ["", "A 1", "A/1", "A.1", "A$1"])
def test_bad_account_id_rejected(bad: str):
    with pytest.raises(ValueError):
        CreatePaymentRequest(amount=100, account_id=bad)


def test_generated_payment_ids_are_unique():
    ids = {
        CreatePaymentRequest(amount=1, account_id="A1").to_command()["payment_id"]
        for _ in range(20)
    }
    assert len(ids) == 20
