"""profile — read-side users/balances (CQRS read model; phase 5).

Port 8004. Endpoints:
  GET /v1/users            -> the 20 seeded users (denormalized balance)
  GET /v1/users/{id}       -> one user

Balance is kept fresh by the ``ledger.events`` consumer
(``services/profile/kafka_consumer.py``), NOT by reading the ledger DB
(pitfall #4: no cross-schema JOIN).
"""
from services.profile.api import attach_profile
from services.shared_app import build_app

app = build_app("profile")
attach_profile(app)
