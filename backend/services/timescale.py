"""
TimescaleDB metric storage service.

All monitoring data lands here with automatic compression,
continuous aggregates, and retention policies.
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import asyncpg
import threading

from backend.utils.config import settings
from backend.utils.logging import get_logger

log = get_logger(__name__)

SETUP_SQL = """
-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Health snapshots hypertable
CREATE TABLE IF NOT EXISTS db_health_metrics (
    time                    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    database_id             TEXT NOT NULL,
    active_connections      INT,
    max_connections         INT,
    cache_hit_ratio         DOUBLE PRECISION,
    qps                     DOUBLE PRECISION,
    p50_latency_ms          DOUBLE PRECISION,
    p95_latency_ms          DOUBLE PRECISION,
    p99_latency_ms          DOUBLE PRECISION,
    replication_lag_seconds DOUBLE PRECISION,
    lock_waits              INT,
    deadlocks               INT,
    disk_read_iops          DOUBLE PRECISION,
    disk_write_iops         DOUBLE PRECISION,
    cpu_pct                 DOUBLE PRECISION,
    memory_pct              DOUBLE PRECISION
);

SELECT create_hypertable('db_health_metrics', 'time', if_not_exists => TRUE);

-- Continuous aggregate: hourly rollup
CREATE MATERIALIZED VIEW IF NOT EXISTS db_health_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS hour,
    database_id,
    avg(active_connections)      AS avg_connections,
    avg(cache_hit_ratio)         AS avg_cache_hit_ratio,
    avg(qps)                     AS avg_qps,
    avg(p99_latency_ms)          AS avg_p99_latency_ms,
    max(p99_latency_ms)          AS max_p99_latency_ms,
    avg(replication_lag_seconds) AS avg_replication_lag,
    sum(deadlocks)               AS total_deadlocks
FROM db_health_metrics
GROUP BY hour, database_id
WITH NO DATA;

-- Continuous aggregate: daily rollup
CREATE MATERIALIZED VIEW IF NOT EXISTS db_health_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) AS day,
    database_id,
    avg(active_connections)      AS avg_connections,
    avg(qps)                     AS avg_qps,
    max(p99_latency_ms)          AS max_p99_latency_ms,
    avg(cache_hit_ratio)         AS avg_cache_hit_ratio
FROM db_health_metrics
GROUP BY day, database_id
WITH NO DATA;

-- Retention policies
SELECT add_retention_policy('db_health_metrics', INTERVAL '7 days', if_not_exists => TRUE);

-- Compression
ALTER TABLE db_health_metrics SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'database_id'
);
SELECT add_compression_policy('db_health_metrics', INTERVAL '2 days', if_not_exists => TRUE);

-- Query snapshots table (non-hypertable, managed by agent)
CREATE TABLE IF NOT EXISTS query_snapshots_store (
    fingerprint     TEXT NOT NULL,
    database_id     TEXT NOT NULL,
    normalized_sql  TEXT,
    sample_sql      TEXT,
    call_count      BIGINT DEFAULT 0,
    mean_time_ms    DOUBLE PRECISION DEFAULT 0,
    p99_time_ms     DOUBLE PRECISION DEFAULT 0,
    total_time_ms   DOUBLE PRECISION DEFAULT 0,
    access_pattern  TEXT,
    last_seen_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (fingerprint, database_id)
);

-- Table stats (bloat tracking)
CREATE TABLE IF NOT EXISTS table_stats (
    time            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    database_id     TEXT NOT NULL,
    table_name      TEXT NOT NULL,
    total_bytes     BIGINT,
    dead_tuples     BIGINT,
    live_tuples     BIGINT,
    dead_ratio_pct  DOUBLE PRECISION
);

SELECT create_hypertable('table_stats', 'time', if_not_exists => TRUE);
SELECT add_retention_policy('table_stats', INTERVAL '90 days', if_not_exists => TRUE);

-- Index stats
CREATE TABLE IF NOT EXISTS index_stats (
    time            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    database_id     TEXT NOT NULL,
    table_name      TEXT NOT NULL,
    index_name      TEXT NOT NULL,
    index_scans     BIGINT,
    index_bytes     BIGINT
);

SELECT create_hypertable('index_stats', 'time', if_not_exists => TRUE);
SELECT add_retention_policy('index_stats', INTERVAL '90 days', if_not_exists => TRUE);
"""


class MetricStore:
    def __init__(self):
        self._pool: Optional[asyncpg.Pool] = None

    # ── Connection lifecycle ───────────────────

    async def connect(self):
        """Initialise the connection pool and set up the schema."""
        if self._pool is not None:
            return  # already connected; no-op

        self._pool = await asyncpg.create_pool(
            settings.timescale_url,
            min_size=2,
            max_size=10,
        )
        await self._setup_schema()
        log.info("timescaledb_connected")

    async def disconnect(self):
        """Close the pool and reset state so reconnect works cleanly."""
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def _ensure_connected(self):
        """Lazily initialise the pool on first use if connect() was not called."""
        if self._pool is None:
            await self.connect()

    def _get_conn(self):
        """
        Return an async context manager that yields a pool connection.

        Raises RuntimeError if the pool is still None after _ensure_connected()
        (e.g. connect() raised internally but was swallowed upstream).
        """
        if self._pool is None:
            raise RuntimeError(
                "MetricStore pool is not initialised. "
                "Await MetricStore.connect() during app startup, "
                "or let _ensure_connected() handle lazy init."
            )
        return self._pool.acquire()

    async def _setup_schema(self):
        """Create tables and hypertables if they don't exist."""
        async with self._get_conn() as conn:
            # Run statement by statement to handle partial failures gracefully
            for statement in SETUP_SQL.split(";"):
                stmt = statement.strip()
                if stmt:
                    try:
                        await conn.execute(stmt)
                    except Exception as e:
                        if "already exists" not in str(e).lower():
                            log.warning("schema_setup_stmt_failed", error=str(e)[:100])

    # ── Writes ────────────────────────────────

    async def write_health_snapshot(self, snapshot: Dict):
        await self._ensure_connected()
        async with self._get_conn() as conn:
            await conn.execute(
                """
                INSERT INTO db_health_metrics (
                    time, database_id, active_connections, max_connections,
                    cache_hit_ratio, qps, p50_latency_ms, p95_latency_ms, p99_latency_ms,
                    replication_lag_seconds, lock_waits, deadlocks
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                """,
                snapshot.get("timestamp", datetime.now(timezone.utc)),
                snapshot["database_id"],
                snapshot.get("active_connections"),
                snapshot.get("max_connections"),
                snapshot.get("cache_hit_ratio"),
                snapshot.get("qps"),
                snapshot.get("p50_latency_ms"),
                snapshot.get("p95_latency_ms"),
                snapshot.get("p99_latency_ms"),
                snapshot.get("replication_lag_seconds"),
                snapshot.get("lock_waits"),
                snapshot.get("deadlocks"),
            )

    async def upsert_query_snapshot(self, data: Dict):
        await self._ensure_connected()
        async with self._get_conn() as conn:
            await conn.execute(
                """
                INSERT INTO query_snapshots_store (
                    fingerprint, database_id, normalized_sql, sample_sql,
                    call_count, mean_time_ms, total_time_ms, access_pattern, last_seen_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
                ON CONFLICT (fingerprint, database_id) DO UPDATE SET
                    call_count    = EXCLUDED.call_count,
                    mean_time_ms  = EXCLUDED.mean_time_ms,
                    total_time_ms = EXCLUDED.total_time_ms,
                    last_seen_at  = NOW()
                """,
                data["fingerprint"],
                data["database_id"],
                data.get("normalized_sql"),
                data.get("sample_sql"),
                data.get("call_count", 0),
                data.get("mean_time_ms", 0),
                data.get("total_time_ms", 0),
                data.get("access_pattern", "unknown"),
            )

    async def write_table_stats(self, database_id: str, rows: List[Dict]):
        await self._ensure_connected()
        async with self._get_conn() as conn:
            await conn.executemany(
                """
                INSERT INTO table_stats (database_id, table_name, total_bytes, dead_tuples, live_tuples, dead_ratio_pct)
                VALUES ($1,$2,$3,$4,$5,$6)
                """,
                [
                    (
                        database_id,
                        r.get("full_table_name", ""),
                        r.get("total_bytes", 0),
                        r.get("n_dead_tup", 0),
                        r.get("n_live_tup", 0),
                        r.get("dead_ratio_pct", 0),
                    )
                    for r in rows
                ],
            )

    async def write_index_stats(self, database_id: str, rows: List[Dict]):
        await self._ensure_connected()
        async with self._get_conn() as conn:
            await conn.executemany(
                """
                INSERT INTO index_stats (database_id, table_name, index_name, index_scans, index_bytes)
                VALUES ($1,$2,$3,$4,$5)
                """,
                [
                    (
                        database_id,
                        r.get("table_name", ""),
                        r.get("index_name", ""),
                        r.get("index_scans", 0),
                        r.get("index_bytes", 0),
                    )
                    for r in rows
                ],
            )

    # ── Reads ─────────────────────────────────

    async def get_health_timeseries(
        self,
        database_id: str,
        metric: str,
        hours: int = 24,
        bucket: str = "5 minutes",
    ) -> List[Dict]:
        await self._ensure_connected()
        async with self._get_conn() as conn:
            rows = await conn.fetch(
                f"""
                SELECT
                    time_bucket($1, time) AS ts,
                    avg({metric}) AS value
                FROM db_health_metrics
                WHERE database_id = $2
                  AND time >= NOW() - INTERVAL '{hours} hours'
                GROUP BY ts
                ORDER BY ts
                """,
                bucket,
                database_id,
            )
            return [
                {"time": r["ts"].isoformat(), "value": float(r["value"] or 0)}
                for r in rows
            ]

    async def get_latest_snapshot(self, database_id: str) -> Optional[Dict]:
        await self._ensure_connected()
        async with self._get_conn() as conn:
            row = await conn.fetchrow(
                """
                SELECT * FROM db_health_metrics
                WHERE database_id = $1
                ORDER BY time DESC
                LIMIT 1
                """,
                database_id,
            )
            return dict(row) if row else None

    async def get_slow_queries(self, database_id: str, limit: int = 20) -> List[Dict]:
        await self._ensure_connected()
        async with self._get_conn() as conn:
            rows = await conn.fetch(
                """
                SELECT fingerprint, normalized_sql, sample_sql, call_count,
                       mean_time_ms, total_time_ms, access_pattern, last_seen_at
                FROM query_snapshots_store
                WHERE database_id = $1
                ORDER BY mean_time_ms DESC
                LIMIT $2
                """,
                database_id,
                limit,
            )
            return [dict(r) for r in rows]

    async def execute_monitoring_query(
        self, sql: str, params: Optional[List[Any]] = None
    ) -> List[Dict]:
        """Execute arbitrary SQL against the monitoring store (for NL chat)."""
        await self._ensure_connected()
        async with self._get_conn() as conn:
            rows = await conn.fetch(sql, *(params or []))
            return [dict(r) for r in rows]

    # Columns that may be forecast. Used to safely interpolate the metric name
    # into the aggregate query (avoids SQL injection from the `metric` arg).
    _FORECASTABLE_METRICS = {
        "active_connections",
        "max_connections",
        "cache_hit_ratio",
        "qps",
        "p50_latency_ms",
        "p95_latency_ms",
        "p99_latency_ms",
        "replication_lag_seconds",
        "lock_waits",
        "deadlocks",
    }

    async def get_raw_metrics_for_forecast(
        self, database_id: str, metric: str, days: int = 90
    ) -> List[Dict]:
        """
        Fetch a per-minute time series of `metric` straight from the raw
        health-metrics hypertable.

        Reading the raw table (rather than the hourly continuous aggregate) means
        a forecast can be produced within minutes of startup instead of waiting
        for the aggregate to fill.
        """
        await self._ensure_connected()
        if metric not in self._FORECASTABLE_METRICS:
            metric = "active_connections"

        async with self._get_conn() as conn:
            rows = await conn.fetch(
                f"""
                SELECT time_bucket('1 minute', time) AS ds, avg({metric}) AS y
                FROM db_health_metrics
                WHERE database_id = $1
                  AND time >= NOW() - INTERVAL '{int(days)} days'
                GROUP BY ds
                ORDER BY ds
                """,
                database_id,
            )
            return [{"ds": r["ds"], "y": float(r["y"] or 0)} for r in rows]
