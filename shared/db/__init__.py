"""shared.db — async SQLAlchemy base + repository interfaces."""
from .base import Base, make_engine, make_session_factory
from .repository import IAccountRepository, IPaymentRepository, IOutboxRepository

__all__ = [
    "Base",
    "make_engine",
    "make_session_factory",
    "IAccountRepository",
    "IPaymentRepository",
    "IOutboxRepository",
]
