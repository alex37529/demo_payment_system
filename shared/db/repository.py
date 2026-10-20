"""Repository interfaces — hide the ORM, allow mocking in unit tests (§3.5/3.6 TЗ).

These are abstract contracts; concrete impls live in each service's
`infrastructure` layer (bound to SQLAlchemy 2.0 async).
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class IAccountRepository(ABC):
    @abstractmethod
    async def get(self, account_id: str) -> Any: ...

    @abstractmethod
    async def get_for_update(self, account_id: str, *, session: Any) -> Any:
        """Load the row under a row lock (Postgres: ``SELECT ... FOR UPDATE``).

        The lock is held by the calling TX (session) until that session
        commits or rolls back — callers that own the session release it
        implicitly. Used by write-side use-cases to serialize concurrent
        mutations on the same account so their ``balance_after`` journal
        rows always equal the committed ``accounts.balance``.
        """
        raise NotImplementedError

    @abstractmethod
    async def save(self, account: Any) -> None: ...

    @abstractmethod
    async def debit(self, account_id: str, amount: int, currency: str) -> Any: ...

    @abstractmethod
    async def reserve(self, account_id: str, amount: int, currency: str) -> Any: ...

    @abstractmethod
    async def cancel_reservation(self, account_id: str, ref: str) -> None: ...

    @abstractmethod
    async def refund(self, account_id: str, ref: str, amount: int) -> None: ...


class IPaymentRepository(ABC):
    @abstractmethod
    async def get(self, payment_id: str) -> Any: ...

    @abstractmethod
    async def save(self, payment: Any) -> None: ...

    @abstractmethod
    async def set_state(self, payment_id: str, state: str) -> None: ...


class IOutboxRepository(ABC):
    @abstractmethod
    async def publish(self, event: Any) -> None:
        """Append an event to the outbox IN THE CURRENT transaction.

        Debezium (CDC) picks up the insert and publishes it to Kafka.
        A commit therefore atomically applies the data change + the event.
        """
