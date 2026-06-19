#!/bin/bash
# Configurable large-dataset seed for the sample monitored database.
#
# Row counts are driven by env vars so you can scale the dataset without
# editing SQL. Defaults produce a "large" ~15.5M-row dataset that exercises
# slow sequential scans and missing-index scenarios. Set these higher (e.g.
# SEED_ORDERS=100000000) in docker-compose.yml to reach the 100M+ scale from
# the project document — expect longer init time and more disk.
set -e

SEED_USERS="${SEED_USERS:-100000}"
SEED_ORDERS="${SEED_ORDERS:-1000000}"
SEED_ORDER_ITEMS="${SEED_ORDER_ITEMS:-1500000}"

echo "[seed] sampledb: users=${SEED_USERS} orders=${SEED_ORDERS} order_items=${SEED_ORDER_ITEMS}"
START=$(date +%s)

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v users="$SEED_USERS" -v orders="$SEED_ORDERS" -v items="$SEED_ORDER_ITEMS" <<'EOSQL'
-- Required extension for per-query statistics (library preloaded via compose command).
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

CREATE TABLE IF NOT EXISTS users (
    id          BIGSERIAL PRIMARY KEY,
    email       TEXT UNIQUE NOT NULL,
    name        TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    last_login  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS orders (
    id          BIGSERIAL PRIMARY KEY,
    user_id     BIGINT,
    total_cents INT NOT NULL,
    status      TEXT DEFAULT 'pending',
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_items (
    id          BIGSERIAL PRIMARY KEY,
    order_id    BIGINT,
    product_id  INT NOT NULL,
    quantity    INT NOT NULL,
    price_cents INT NOT NULL
);

-- Faster bulk load for the large inserts below.
SET synchronous_commit = off;
SET maintenance_work_mem = '512MB';

-- ── Users ────────────────────────────────────────────────────────────
INSERT INTO users (email, name)
SELECT 'user' || i || '@example.com', 'User ' || i
FROM generate_series(1, :users) AS i
ON CONFLICT DO NOTHING;

-- ── Orders ───────────────────────────────────────────────────────────
INSERT INTO orders (user_id, total_cents, status, created_at)
SELECT
    (random() * (:users - 1) + 1)::bigint,
    (random() * 50000 + 100)::int,
    (ARRAY['pending','processing','shipped','completed'])[(random() * 3 + 1)::int],
    NOW() - (random() * INTERVAL '90 days')
FROM generate_series(1, :orders);

-- ── Order items ──────────────────────────────────────────────────────
INSERT INTO order_items (order_id, product_id, quantity, price_cents)
SELECT
    (random() * (:orders - 1) + 1)::bigint,
    (random() * 1000 + 1)::int,
    (random() * 5 + 1)::int,
    (random() * 10000 + 100)::int
FROM generate_series(1, :items);

-- Index orders.user_id, but intentionally NONE on orders.status so the agent
-- detects the unindexed sequential scan as a slow-query / missing-index case.
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);

ANALYZE;

GRANT pg_read_all_stats TO dba_agent;

-- ── Workload: populate pg_stat_statements with slow, repeated queries ──
DO $$
DECLARE
    i   INT;
    cnt BIGINT;
BEGIN
    FOR i IN 1..40 LOOP
        SELECT count(*) INTO cnt FROM orders WHERE status = 'pending';
        SELECT count(*) INTO cnt
            FROM orders o JOIN users u ON o.user_id = u.id
            WHERE o.status = 'processing';
        SELECT count(*) INTO cnt
            FROM orders WHERE total_cents > 25000 AND status = 'shipped';
        PERFORM status, count(*) FROM orders GROUP BY status;
    END LOOP;
END $$;
EOSQL

END=$(date +%s)
echo "[seed] complete in $((END - START))s"
