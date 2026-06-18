-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Create sample schema for testing
CREATE TABLE IF NOT EXISTS users (
    id          SERIAL PRIMARY KEY,
    email       TEXT UNIQUE NOT NULL,
    name        TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    last_login  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS orders (
    id          SERIAL PRIMARY KEY,
    user_id     INT REFERENCES users(id),
    total_cents INT NOT NULL,
    status      TEXT DEFAULT 'pending',
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_items (
    id          SERIAL PRIMARY KEY,
    order_id    INT REFERENCES orders(id),
    product_id  INT NOT NULL,
    quantity    INT NOT NULL,
    price_cents INT NOT NULL
);

-- Index for demonstration (intentionally missing some)
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
-- Note: intentionally NO index on orders.status to demo slow query detection

-- Seed sample data
INSERT INTO users (email, name)
SELECT
    'user' || i || '@example.com',
    'User ' || i
FROM generate_series(1, 10000) AS i
ON CONFLICT DO NOTHING;

INSERT INTO orders (user_id, total_cents, status, created_at)
SELECT
    (random() * 9999 + 1)::int,
    (random() * 50000 + 100)::int,
    CASE (random() * 4)::int
        WHEN 0 THEN 'pending'
        WHEN 1 THEN 'processing'
        WHEN 2 THEN 'shipped'
        ELSE 'completed'
    END,
    NOW() - (random() * INTERVAL '90 days')
FROM generate_series(1, 100000);

-- Grant monitoring access
GRANT pg_read_all_stats TO dba_agent;

-- ── Generate workload so the Slow Queries tab has data to show ──────────
-- Runs representative queries repeatedly (calls > 5) against the unindexed
-- orders.status column. Each one is a sequential scan over 100k rows, so it
-- registers measurable execution time in pg_stat_statements for the agent to
-- pick up. This runs once at container init; pg_stat_statements retains the
-- counts afterward.
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