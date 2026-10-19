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

-- wal_level=logical is set via the container `command` (postgres -c wal_level=logical).
