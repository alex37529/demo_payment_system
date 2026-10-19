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
    id         BIGSERIAL PRIMARY KEY,
    event_id   VARCHAR(36) NOT NULL UNIQUE,
    topic      VARCHAR(128) NOT NULL,
    payload    JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_outbox_topic ON outbox_events (topic);
CREATE INDEX IF NOT EXISTS idx_outbox_created_at ON outbox_events (created_at);

-- Hand ownership to the app role so the Ledger service can write (and Debezium
-- can read). The init script runs as the `postgres` superuser.
ALTER TABLE outbox_events OWNER TO fintech;
GRANT ALL ON ALL TABLES IN SCHEMA public TO fintech;

-- Debezium (pgoutput) needs a publication covering the outbox table.
CREATE PUBLICATION outbox_publication FOR TABLE outbox_events;

-- wal_level=logical is set via the container `command` (postgres -c wal_level=logical).
