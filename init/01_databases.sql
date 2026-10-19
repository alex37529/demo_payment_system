-- Create the 4 bounded-context databases. Run by the postgres entrypoint (initdb.d).
-- Bounded contexts == DB boundaries (TЗ v2 §2.2): no cross-context reads.

CREATE ROLE fintech LOGIN PASSWORD 'fintech' REPLICATION;

CREATE DATABASE ledger OWNER fintech;
CREATE DATABASE orchestrator OWNER fintech;
CREATE DATABASE intake OWNER fintech;
CREATE DATABASE profile OWNER fintech;

-- The outbox table lives in the `ledger` context (write-side, phase 2). Debezium
-- reads this table's inserts (WAL) and publishes to Kafka (topic `ledger.events`).
\connect ledger
CREATE TABLE IF NOT EXISTS outbox_events (
    id           BIGSERIAL PRIMARY KEY,
    event_id     VARCHAR(36) NOT NULL UNIQUE,
    topic        VARCHAR(128) NOT NULL,
    payload      JSONB NOT NULL,
    status       VARCHAR(32) NOT NULL DEFAULT 'pending',
    published_at TIMESTAMPTZ,
    last_error   VARCHAR(512),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_outbox_topic ON outbox_events (topic);
CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox_events (status);
CREATE INDEX IF NOT EXISTS idx_outbox_created_at ON outbox_events (created_at);

-- Upgrade already-provisioned volumes (created before status tracking existed).
-- ADD COLUMN IF NOT EXISTS is a no-op when the column is already present.
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS status       VARCHAR(32) NOT NULL DEFAULT 'pending';
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ;
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS last_error   VARCHAR(512);
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS updated_at   TIMESTAMPTZ NOT NULL DEFAULT now();

-- Hand ownership to the app role so the Ledger service can write (and Debezium
-- can read). The init script runs as the `postgres` superuser.
ALTER TABLE outbox_events OWNER TO fintech;
GRANT ALL ON ALL TABLES IN SCHEMA public TO fintech;

-- Debezium (pgoutput) needs a publication covering the outbox table.
CREATE PUBLICATION outbox_publication FOR TABLE outbox_events;

-- Phase 2 — write-side: ledger aggregate + idempotency guard.
-- accounts: A1 seeded for the phase-2 exit test (debit -> balance 400, outbox+1).
CREATE TABLE IF NOT EXISTS accounts (
    id         VARCHAR(64) PRIMARY KEY,
    owner_name VARCHAR(255) NOT NULL DEFAULT '',
    currency   VARCHAR(8)  NOT NULL,
    balance    BIGINT      NOT NULL DEFAULT 0,
    reserved   BIGINT      NOT NULL DEFAULT 0,
    version    INTEGER     NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO accounts (id, owner_name, currency, balance, reserved, version)
VALUES ('A1', 'John', 'USD', 500, 0, 1)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS applied_commands (
    idem_key   VARCHAR(191) PRIMARY KEY,
    op         VARCHAR(32) NOT NULL,
    account_id VARCHAR(64) NOT NULL,
    payment_id VARCHAR(64) NOT NULL DEFAULT '',
    saga_id    VARCHAR(64) NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_applied_account ON applied_commands (account_id);

-- payments (ledger-context view; orchestrator owns lifecycle in phase 3).
CREATE TABLE IF NOT EXISTS payments (
    id         VARCHAR(64) PRIMARY KEY,
    account_id VARCHAR(64) NOT NULL,
    amount     BIGINT      NOT NULL,
    currency   VARCHAR(8)  NOT NULL,
    idem_key   VARCHAR(191) NOT NULL UNIQUE,
    saga_id    VARCHAR(64) NOT NULL DEFAULT '',
    state      VARCHAR(32) NOT NULL DEFAULT 'Created',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_payments_account ON payments (account_id);

ALTER TABLE accounts         OWNER TO fintech;
ALTER TABLE applied_commands OWNER TO fintech;
ALTER TABLE payments         OWNER TO fintech;

-- Phase 3 — saga state (the `orchestrator` bounded context owns this table; the
-- ledger DB above stays write-side only). State is the saga state name;
-- `history`/`payload`/`compensation` are JSON (SQLAlchemy writes via `JSON`).
\connect orchestrator
CREATE TABLE IF NOT EXISTS sagas (
    saga_id      VARCHAR(64)  PRIMARY KEY,
    payment_id   VARCHAR(64)  NOT NULL UNIQUE,
    state        VARCHAR(32)  NOT NULL DEFAULT 'Created',
    history      JSON         NOT NULL DEFAULT '[]',
    payload      JSON         NOT NULL DEFAULT '{}',
    compensation JSON         NOT NULL DEFAULT '[]',
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sagas_payment ON sagas (payment_id);
CREATE INDEX IF NOT EXISTS idx_sagas_state   ON sagas (state);
ALTER TABLE sagas OWNER TO fintech;
GRANT ALL ON ALL TABLES IN SCHEMA public TO fintech;

-- Phase 4 — intake idempotency (the `intake` bounded context owns this table).
-- `idempotency_keys`: persistent mirror of the Redis `idem:{key}` entry
-- (spec §3.3 "dual-write"): survives Redis restart; source for cached response
-- and GET /v1/payments/{id}. `payment_id` is indexed for that lookup.
\connect intake
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key         VARCHAR(191)  PRIMARY KEY,
    payment_id  VARCHAR(64),
    response    JSON,
    ttl_until   TIMESTAMPTZ,
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_idem_payment ON idempotency_keys (payment_id);
CREATE INDEX IF NOT EXISTS idx_idem_ttl     ON idempotency_keys (ttl_until);
ALTER TABLE idempotency_keys OWNER TO fintech;
GRANT ALL ON ALL TABLES IN SCHEMA public TO fintech;

-- ---------------------------------------------------------------------
-- Phase 5 — Read-Side: seed 20 users **dual** (ledger + profile)
-- ---------------------------------------------------------------------
-- TЗ v2 §5: 20 seeded accounts, balance $100..$10_000 (minor units = cents
-- as BigInteger, no float — pitfall #3).
--
-- Postgres isolation: `ledger` and `profile` are **separate databases**;
-- you can't `SELECT ... FROM ledger.accounts` inside the `profile` db. So
-- the "dual seed" is two separate `\connect` blocks — not one atomic TX.
-- The *content* is deterministic (same 20 names / balances) and both halves
-- run in the same `initdb.d` script at first postgres boot, so a crash
-- mid-initdb is effectively impossible (the script is idempotent — each
-- half is `ON CONFLICT DO NOTHING` and `CREATE TABLE IF NOT EXISTS`).
--
-- `ON CONFLICT (id) DO NOTHING` keeps the seed re-runnable: a fresh volume
-- gets the full 20; an existing volume is untouched.
--
-- Deterministic, no `random()` — tests + demos + the UI rely on a stable
-- state. The 20 accounts cover both USD and EUR currencies (phase-2
-- "own-currency" invariant, ADR-003).

\connect ledger
INSERT INTO accounts (id, owner_name, currency, balance, reserved, version)
VALUES
  ('U001', 'Alice Alvarez',   'USD',  620000, 0, 1),
  ('U002', 'Benjamin Baker',  'EUR',  480000, 0, 1),
  ('U003', 'Carla Chen',      'USD',  350000, 0, 1),
  ('U004', 'Daniel Davis',    'EUR',  520000, 0, 1),
  ('U005', 'Emma Evans',      'USD',  710000, 0, 1),
  ('U006', 'Felix Fischer',   'EUR',  230000, 0, 1),
  ('U007', 'Grace Gardner',   'USD',  940000, 0, 1),
  ('U008', 'Hugo Hoffman',    'EUR',  410000, 0, 1),
  ('U009', 'Isabel Ingram',   'USD',  125000, 0, 1),
  ('U010', 'Jonas Jansen',    'EUR',  680000, 0, 1),
  ('U011', 'Klara Kowalski',  'USD',  530000, 0, 1),
  ('U012', 'Lars Lindqvist',  'EUR',  175000, 0, 1),
  ('U013', 'Marta Moretti',   'USD',  820000, 0, 1),
  ('U014', 'Nina Novak',      'EUR',  265000, 0, 1),
  ('U015', 'Oscar O''Brien',  'USD',  470000, 0, 1),
  ('U016', 'Petra Petrov',    'EUR',  390000, 0, 1),
  ('U017', 'Quentin Quinn',   'USD',  145000, 0, 1),
  ('U018', 'Rosa Rodriguez',  'EUR',  760000, 0, 1),
  ('U019', 'Samuel Santos',   'USD',  610000, 0, 1),
  ('U020', 'Tina Turner',     'EUR',  990000, 0, 1)
ON CONFLICT (id) DO NOTHING;

\connect profile
-- The `profile` bounded context lives in its OWN database (TЗ §2.2, DB
-- boundary = context boundary). Tables use the public schema, exactly like
-- ledger.accounts / orchestrator.sagas / intake.idempotency_keys.
CREATE TABLE IF NOT EXISTS users (
    id         VARCHAR(64)  PRIMARY KEY,
    name       VARCHAR(255) NOT NULL DEFAULT '',
    email      VARCHAR(255) NOT NULL DEFAULT '',
    currency   VARCHAR(8)   NOT NULL DEFAULT 'USD',
    balance    BIGINT       NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_users_currency ON users (currency);

-- Read-side denormalized copy (the same 20 rows, same balance — the two
-- models MUST agree at t=0, else CQRS consistency at start breaks).
INSERT INTO users (id, name, email, currency, balance)
VALUES
  ('U001', 'Alice Alvarez',   'alice.alvarez@example.com',    'USD',  620000),
  ('U002', 'Benjamin Baker',  'benjamin.baker@example.com',   'EUR',  480000),
  ('U003', 'Carla Chen',      'carla.chen@example.com',       'USD',  350000),
  ('U004', 'Daniel Davis',    'daniel.davis@example.com',     'EUR',  520000),
  ('U005', 'Emma Evans',      'emma.evans@example.com',       'USD',  710000),
  ('U006', 'Felix Fischer',   'felix.fischer@example.com',    'EUR',  230000),
  ('U007', 'Grace Gardner',   'grace.gardner@example.com',    'USD',  940000),
  ('U008', 'Hugo Hoffman',    'hugo.hoffman@example.com',     'EUR',  410000),
  ('U009', 'Isabel Ingram',   'isabel.ingram@example.com',    'USD',  125000),
  ('U010', 'Jonas Jansen',    'jonas.jansen@example.com',     'EUR',  680000),
  ('U011', 'Klara Kowalski',  'klara.kowalski@example.com',   'USD',  530000),
  ('U012', 'Lars Lindqvist',  'lars.lindqvist@example.com',   'EUR',  175000),
  ('U013', 'Marta Moretti',   'marta.moretti@example.com',    'USD',  820000),
  ('U014', 'Nina Novak',      'nina.novak@example.com',       'EUR',  265000),
  ('U015', 'Oscar O''Brien',  'oscar.obrien@example.com',     'USD',  470000),
  ('U016', 'Petra Petrov',    'petra.petrov@example.com',     'EUR',  390000),
  ('U017', 'Quentin Quinn',   'quentin.quinn@example.com',    'USD',  145000),
  ('U018', 'Rosa Rodriguez',  'rosa.rodriguez@example.com',   'EUR',  760000),
  ('U019', 'Samuel Santos',   'samuel.santos@example.com',    'USD',  610000),
  ('U020', 'Tina Turner',     'tina.turner@example.com',      'EUR',  990000)
ON CONFLICT (id) DO NOTHING;

-- Idempotency guard (phase-5 spec §3.1, pitfall #5): one row per already-
-- applied ``ledger.events`` ``event_id``. Kafka at-least-once redelivery is a
-- guarded-INSERT conflict => no double balance update.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id      VARCHAR(64)  PRIMARY KEY,
    event_type    VARCHAR(64)  NOT NULL DEFAULT '',
    account_id    VARCHAR(64)  NOT NULL DEFAULT '',
    first_seen_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_processed_events_account
    ON processed_events (account_id);

-- Profile DB boundary: grant the app role ownership (consistent with the
-- other 3 bounded-context DBs above).
GRANT ALL ON ALL TABLES IN SCHEMA public TO fintech;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO fintech;
ALTER TABLE users           OWNER TO fintech;
ALTER TABLE processed_events OWNER TO fintech;

-- wal_level=logical is set via the container `command` (postgres -c wal_level=logical).
