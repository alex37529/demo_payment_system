-- Phase 2.5 (added) — seed 20 users on top of the 1 test account (A1).
-- ТЗ v2 §5: "20 пользователей, Имя, Фамилия, Email, валюта (USD/EUR),
--           Балансы — случайные реалистичные ($100 – $10 000)."
-- Балансы — в minor units (центах для USD/EUR): 10000 .. 1000000.
-- Имена/фамилии — стилизация «вашингтонских политиков» (без реальных имён).
--
-- Idempotent: ON CONFLICT (id) DO UPDATE — повторный `make seed` сбрасывает
-- балансы к изначальным значениям (это осознанное поведение seed = reset).
-- A1 (test account) НЕ трогается — он из init/01_databases.sql.
--
-- DB `ledger` выбирается через PGDATABASE (docker-compose `seed` service).

INSERT INTO accounts (id, owner_name, currency, balance, reserved, version)
VALUES
    ('A2',  'John Smith',      'USD',  1000000, 0, 1),  -- $10 000
    ('A3',  'Susan Davis',     'USD',   850000, 0, 1),  -- $ 8 500
    ('A4',  'Michael Johnson', 'EUR',   750000, 0, 1),  -- € 7 500
    ('A5',  'William Brown',   'USD',   650000, 0, 1),  -- $ 6 500
    ('A6',  'Sarah Miller',    'USD',   550000, 0, 1),  -- $ 5 500
    ('A7',  'David Wilson',    'EUR',   450000, 0, 1),  -- € 4 500
    ('A8',  'Margaret Taylor', 'USD',   380000, 0, 1),  -- $ 3 800
    ('A9',  'Robert Anderson', 'USD',   320000, 0, 1),  -- $ 3 200
    ('A10', 'Linda Thomas',    'EUR',   260000, 0, 1),  -- € 2 600
    ('A11', 'James Jackson',   'USD',   210000, 0, 1),  -- $ 2 100
    ('A12', 'Patricia White',  'USD',   175000, 0, 1),  -- $ 1 750
    ('A13', 'Charles Harris',  'EUR',   150000, 0, 1),  -- € 1 500
    ('A14', 'Dorothy Martin',  'USD',   130000, 0, 1),  -- $ 1 300
    ('A15', 'Thomas Lewis',    'USD',   115000, 0, 1),  -- $ 1 150
    ('A16', 'Barbara Walker',  'EUR',   100000, 0, 1),  -- € 1 000
    ('A17', 'Richard Hall',    'USD',    90000, 0, 1),  -- $   900
    ('A18', 'Nancy Allen',     'USD',    75000, 0, 1),  -- $   750
    ('A19', 'George Young',    'EUR',    60000, 0, 1),  -- €   600
    ('A20', 'Patricia Green',  'USD',    45000, 0, 1),  -- $   450
    ('A21', 'Edward King',     'USD',    30000, 0, 1)   -- $   300
ON CONFLICT (id) DO UPDATE
SET
    owner_name = EXCLUDED.owner_name,
    currency   = EXCLUDED.currency,
    balance    = EXCLUDED.balance,
    reserved   = 0,
    version    = 1,
    updated_at = now();
