import pytest


@pytest.fixture
def event():
    from shared.domain.events import FundsDebited

    return FundsDebited(account_id="A1", payment_id="P1", saga_id="S1", amount={"value": 100, "currency": "USD"}, new_balance={"value": 900, "currency": "USD"})
