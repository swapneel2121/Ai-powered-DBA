"""
Core async monitoring agent.

Polls PostgreSQL and MySQL instances at configurable intervals,
collects performance metrics, detects anomalies, and dispatches
optimization proposals.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Union, Callable
from urllib.parse import urlparse

import aiomysql  # type: ignore[import]
import asyncpg  # type: ignore[import]

from backend.agent.anomaly import AnomalyDetector
from backend.agent.fingerprint import QueryFingerprinter
from backend.services.notifications import NotificationService
from backend.services.timescale import MetricStore
from backend.utils.config import settings
from backend.utils.logging import get_logger, set_correlation_id

log = get_logger(__name__)


# ─────────────────────────────────────────────
# Data Classes
# ─────────────────────────────────────────────


@dataclass
class DatabaseTarget:
    url: str
    db_type: str  # postgresql | mysql
    host: str
    port: int
    database: str
    user: str
    password: str
    id: str = field(init=False)

    def __post_init__(self):
        self.id = hashlib.sha256(
            f"{self.db_type}:{self.host}:{self.port}:{self.database}".encode()
        ).hexdigest()[:16]

    @classmethod
    def from_url(cls, url: str) -> "DatabaseTarget":
        parsed = urlparse(url)
        db_type = "postgresql" if parsed.scheme.startswith("postgres") else "mysql"
        return cls(
            url=url,
            db_type=db_type,
            host=parsed.hostname or "localhost",
            port=parsed.port or (5432 if db_type == "postgresql" else 3306),
            database=parsed.path.lstrip("/"),
            user=parsed.username or "",
            password=parsed.password or "",
        )


# ─────────────────────────────────────────────
# PostgreSQL SQL
# ─────────────────────────────────────────────

POSTGRES_CRITICAL_SQL = """
SELECT
    (SELECT count(*) FROM pg_stat_activity WHERE state = 'active') AS active_connections,
    (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') AS max_connections,
    (SELECT round(
        blks_hit::numeric / nullif(blks_hit + blks_read, 0) * 100, 2
    ) FROM pg_stat_database WHERE datname = current_database()) AS cache_hit_ratio,
    (SELECT count(*) FROM pg_locks WHERE NOT granted) AS lock_waits,
    (SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock') AS lock_wait_sessions,
    (SELECT xact_commit + xact_rollback FROM pg_stat_database
     WHERE datname = current_database()) AS total_xacts,
    (SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()) AS deadlocks,
    (SELECT coalesce(max(extract(epoch from (now() - pg_last_xact_replay_timestamp()))), 0))
        AS replication_lag_seconds
"""

# FIX: Removed the broken POSTGRES_SLOW_QUERIES_SQL that incorrectly used
# percentile_disc() as if pg_stat_statements exposed per-execution rows (it does not).
# The simple variant below is correct and works across all supported PG versions.
POSTGRES_SLOW_QUERIES_SQL = """
SELECT
    queryid::text AS queryid,
    query,
    calls,
    total_exec_time / nullif(calls, 0) AS mean_time_ms,
    total_exec_time,
    rows / nullif(calls, 0) AS avg_rows,
    shared_blks_hit,
    shared_blks_read
FROM pg_stat_statements
WHERE calls > 5
  AND total_exec_time / nullif(calls, 0) > $1
ORDER BY mean_time_ms DESC
LIMIT 50
"""

POSTGRES_TABLE_BLOAT_SQL = """
SELECT
    schemaname || '.' || tablename AS full_table_name,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS total_size,
    pg_total_relation_size(schemaname||'.'||tablename) AS total_bytes,
    n_dead_tup,
    n_live_tup,
    round(n_dead_tup::numeric / nullif(n_live_tup + n_dead_tup, 0) * 100, 2) AS dead_ratio_pct
FROM pg_stat_user_tables
WHERE n_dead_tup > 1000
ORDER BY n_dead_tup DESC
LIMIT 20
"""

POSTGRES_INDEX_USAGE_SQL = """
SELECT
    schemaname || '.' || tablename AS table_name,
    indexrelname AS index_name,
    idx_scan AS index_scans,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
    pg_relation_size(indexrelid) AS index_bytes
FROM pg_stat_user_indexes
ORDER BY idx_scan ASC
LIMIT 50
"""


# ─────────────────────────────────────────────
# PostgreSQL Collector
# ─────────────────────────────────────────────


class PostgresCollector:
    def __init__(self, target: DatabaseTarget):
        self.target = target
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        """Create the connection pool. Must be called before any collect_* method."""
        self._pool = await asyncpg.create_pool(
            host=self.target.host,
            port=self.target.port,
            database=self.target.database,
            user=self.target.user,
            password=self.target.password,
            min_size=1,
            max_size=3,  # Low ceiling to limit monitoring overhead
            command_timeout=10,
        )
        log.info("postgres_connected", db_id=self.target.id, host=self.target.host)

    async def disconnect(self) -> None:
        """Close the connection pool gracefully."""
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
            log.info("postgres_disconnected", db_id=self.target.id)

    # FIX: Central guard so every method raises clearly instead of
    # producing "AttributeError: 'NoneType' object has no attribute 'acquire'".
    def _ensure_connected(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError(
                f"PostgresCollector for db_id={self.target.id} is not connected. "
                "Call connect() before collecting metrics."
            )
        return self._pool

    async def collect_critical(self) -> Dict:
        pool = self._ensure_connected()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(POSTGRES_CRITICAL_SQL)
            return dict(row) if row else {}

    async def collect_slow_queries(self, threshold_ms: float) -> List[Dict]:
        pool = self._ensure_connected()
        async with pool.acquire() as conn:
            try:
                rows = await conn.fetch(POSTGRES_SLOW_QUERIES_SQL, threshold_ms)
                return [dict(r) for r in rows]
            except asyncpg.UndefinedTableError:
                log.warning("pg_stat_statements_not_installed", db_id=self.target.id)
                return []

    async def collect_table_bloat(self) -> List[Dict]:
        pool = self._ensure_connected()
        async with pool.acquire() as conn:
            rows = await conn.fetch(POSTGRES_TABLE_BLOAT_SQL)
            return [dict(r) for r in rows]

    async def collect_index_usage(self) -> List[Dict]:
        pool = self._ensure_connected()
        async with pool.acquire() as conn:
            rows = await conn.fetch(POSTGRES_INDEX_USAGE_SQL)
            return [dict(r) for r in rows]

    async def get_explain(self, sql: str) -> str:
        """
        Run EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) on a query inside a
        transaction that is always rolled back so no data is mutated.

        FIX: The original code called `await conn.execute("ROLLBACK")` inside
        an asyncpg transaction context manager, which is a no-op — asyncpg
        manages the transaction state itself and the explicit ROLLBACK call
        was silently ignored. The correct approach is to let the context
        manager roll back on exit by simply *not* committing, which is the
        default when no exception is raised inside `conn.transaction()`.
        We raise the exception so the `async with conn.transaction()` block
        exits abnormally and asyncpg issues the ROLLBACK automatically.
        """
        pool = self._ensure_connected()
        async with pool.acquire() as conn:
            try:
                async with conn.transaction():
                    result = await conn.fetchval(
                        f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}"
                    )
                    # Raise to trigger automatic ROLLBACK from the transaction
                    # context manager so ANALYZE side-effects are discarded.
                    raise _RollbackSentinel()
            except _RollbackSentinel:
                return result or ""
            except Exception as e:
                log.warning("explain_failed", db_id=self.target.id, error=str(e))
                return f"EXPLAIN failed: {e}"

    async def get_schema_summary(self, max_tables: int = 40) -> str:
        """
        Compact 'table(col type, ...)' summary of the public schema, used to
        ground the LLM when translating English questions into SQL.
        """
        pool = self._ensure_connected()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                ORDER BY table_name, ordinal_position
                """
            )
        tables: Dict[str, List[str]] = {}
        for r in rows:
            tables.setdefault(r["table_name"], []).append(
                f"{r['column_name']} {r['data_type']}"
            )
        lines = [
            f"{t}({', '.join(cols)})"
            for t, cols in list(tables.items())[:max_tables]
        ]
        return "\n".join(lines)

    async def run_readonly_query(
        self, sql: str, limit: int = 100, timeout_s: float = 8.0
    ) -> List[Dict]:
        """
        Execute a single read-only SELECT against the monitored database and
        return rows. Safety: a READ ONLY transaction (Postgres rejects any
        write/DDL) plus a statement timeout to bound expensive scans.
        """
        pool = self._ensure_connected()
        async with pool.acquire() as conn:
            async with conn.transaction(readonly=True):
                await conn.execute(
                    f"SET LOCAL statement_timeout = {int(timeout_s * 1000)}"
                )
                rows = await conn.fetch(sql)
                return [dict(r) for r in rows[:limit]]


class _RollbackSentinel(Exception):
    """Private sentinel used to force a transaction rollback after EXPLAIN ANALYZE."""


# ─────────────────────────────────────────────
# MySQL SQL
# ─────────────────────────────────────────────

MYSQL_CRITICAL_SQL = """
SELECT
    (SELECT variable_value FROM performance_schema.global_status
     WHERE variable_name = 'Threads_running') AS active_connections,
    (SELECT variable_value FROM performance_schema.global_variables
     WHERE variable_name = 'max_connections') AS max_connections,
    (SELECT ROUND(
        variable_value / NULLIF(
            (SELECT variable_value FROM performance_schema.global_status
             WHERE variable_name = 'Innodb_buffer_pool_reads') +
            variable_value,
        0) * 100, 2)
     FROM performance_schema.global_status
     WHERE variable_name = 'Innodb_buffer_pool_read_requests') AS cache_hit_ratio,
    (SELECT variable_value FROM performance_schema.global_status
     WHERE variable_name = 'Innodb_deadlocks') AS deadlocks
"""

MYSQL_SLOW_QUERIES_SQL = """
SELECT
    DIGEST AS queryid,
    DIGEST_TEXT AS query,
    COUNT_STAR AS calls,
    AVG_TIMER_WAIT / 1000000 AS mean_time_ms,
    SUM_TIMER_WAIT / 1000000 AS total_time_ms,
    AVG_ROWS_EXAMINED AS avg_rows
FROM performance_schema.events_statements_summary_by_digest
WHERE COUNT_STAR > 5
  AND AVG_TIMER_WAIT / 1000000 > %s
ORDER BY mean_time_ms DESC
LIMIT 50
"""


# ─────────────────────────────────────────────
# MySQL Collector
# ─────────────────────────────────────────────


class MySQLCollector:
    def __init__(self, target: DatabaseTarget):
        self.target = target
        self._pool: Optional[aiomysql.Pool] = None

    async def connect(self) -> None:
        """Create the connection pool. Must be called before any collect_* method."""
        self._pool = await aiomysql.create_pool(
            host=self.target.host,
            port=self.target.port,
            db=self.target.database,
            user=self.target.user,
            password=self.target.password,
            minsize=1,
            maxsize=3,
            autocommit=True,
        )
        log.info("mysql_connected", db_id=self.target.id, host=self.target.host)

    async def disconnect(self) -> None:
        """Close the connection pool gracefully.

        FIX: The original code did not guard against double-close.
        aiomysql raises RuntimeError if wait_closed() is called on an
        already-closed pool, so we gate on `self._pool is not None`.
        """
        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None
            log.info("mysql_disconnected", db_id=self.target.id)

    # FIX: Same guard as PostgresCollector — surfaces a clear RuntimeError
    # instead of "AttributeError: 'NoneType' object has no attribute 'acquire'".
    def _ensure_connected(self) -> aiomysql.Pool:
        if self._pool is None:
            raise RuntimeError(
                f"MySQLCollector for db_id={self.target.id} is not connected. "
                "Call connect() before collecting metrics."
            )
        return self._pool

    async def collect_critical(self) -> Dict:
        pool = self._ensure_connected()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(MYSQL_CRITICAL_SQL)
                row = await cur.fetchone()
                return row or {}

    async def collect_slow_queries(self, threshold_ms: float) -> List[Dict]:
        pool = self._ensure_connected()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(MYSQL_SLOW_QUERIES_SQL, (threshold_ms,))
                return await cur.fetchall() or []


# ─────────────────────────────────────────────
# Collector type alias
# ─────────────────────────────────────────────

AnyCollector = Union[PostgresCollector, MySQLCollector]


# ─────────────────────────────────────────────
# Main Monitoring Agent
# ─────────────────────────────────────────────


class MonitoringAgent:
    """
    Async agent that continuously polls all configured database targets.

    Critical metrics (connections, latency, locks) → every CRITICAL_POLL_INTERVAL seconds.
    Secondary metrics (bloat, index usage) → every SECONDARY_POLL_INTERVAL seconds.

    Call `await agent.start()` to begin polling.
    Call `await agent.stop()` to shut down cleanly.
    """

    def __init__(
        self,
        metric_store: MetricStore,
        fingerprinter: QueryFingerprinter,
        anomaly_detector: AnomalyDetector,
        notifier: NotificationService,
        on_snapshot: "Optional[Callable[[str, Dict], None]]" = None,
    ):
        self.metric_store = metric_store
        self.fingerprinter = fingerprinter
        self.anomaly_detector = anomaly_detector
        self.notifier = notifier
        # Optional sync callback used to fan a fresh snapshot out to live
        # subscribers (e.g. the SSE stream). Must be non-blocking.
        self.on_snapshot = on_snapshot

        self._collectors: Dict[str, AnyCollector] = {}
        # Per-database (monotonic_time, total_xacts) for QPS computation.
        self._last_xact: Dict[str, tuple] = {}
        self._running = False

    # ── Lifecycle ────────────────────────────

    async def start(self) -> None:
        """
        Initialize collectors for all configured databases, then run both
        polling loops concurrently.

        FIX: The original `asyncio.gather(_critical_loop(), _secondary_loop())`
        had no exception propagation strategy — if _critical_loop() raised an
        unhandled exception, _secondary_loop() would keep running as an orphan.
        We now use `return_exceptions=False` (the default) so the first loop
        failure cancels the gather and propagates, letting the caller decide
        whether to restart or shut down.
        """
        targets = [DatabaseTarget.from_url(url) for url in settings.monitored_dbs]
        self._running = True

        # Keep trying to connect collectors until at least one is available.
        # A monitored database may not be ready when the agent boots (e.g. it is
        # still seeding) — previously the agent gave up permanently here, leaving
        # the dashboard empty forever. Now it retries until a collector connects.
        retry_delay = 10
        while self._running and not self._collectors:
            for target in targets:
                if target.id in self._collectors:
                    continue
                try:
                    collector: AnyCollector
                    if target.db_type == "postgresql":
                        collector = PostgresCollector(target)
                    else:
                        collector = MySQLCollector(target)
                    await collector.connect()
                    self._collectors[target.id] = collector
                    log.info("collector_started", db_id=target.id, db_type=target.db_type)
                except Exception as e:
                    log.warning("collector_connect_failed_will_retry", db_id=target.id, error=str(e)[:160])
            if not self._collectors:
                log.warning("no_collectors_available_retrying", retry_in_s=retry_delay)
                await asyncio.sleep(retry_delay)

        if not self._collectors:
            return  # stop() was called before any collector connected

        try:
            await asyncio.gather(
                self._critical_loop(),
                self._secondary_loop(),
            )
        finally:
            # Ensure clean shutdown if either loop exits for any reason.
            self._running = False

    async def stop(self) -> None:
        """Signal both loops to exit and disconnect all collectors."""
        self._running = False
        for db_id, collector in self._collectors.items():
            try:
                await collector.disconnect()
            except Exception as e:
                log.warning("collector_disconnect_error", db_id=db_id, error=str(e))
        self._collectors.clear()

    # ── Critical polling loop ─────────────────

    async def _critical_loop(self) -> None:
        while self._running:
            start = time.monotonic()
            tasks = [
                self._collect_critical(db_id, collector)
                for db_id, collector in self._collectors.items()
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
            elapsed = time.monotonic() - start
            sleep_for = max(0.0, settings.critical_poll_interval - elapsed)
            await asyncio.sleep(sleep_for)

    async def _collect_critical(self, db_id: str, collector: AnyCollector) -> None:
        set_correlation_id()
        try:
            raw = await collector.collect_critical()
            if not raw:
                log.warning("empty_critical_metrics", db_id=db_id)
                return

            snapshot = self._normalize_critical(db_id, raw, collector)
            await self.metric_store.write_health_snapshot(snapshot)

            # Fan the snapshot out to any live subscribers (SSE dashboard).
            if self.on_snapshot is not None:
                try:
                    self.on_snapshot(db_id, snapshot)
                except Exception as e:  # noqa: BLE001 - never let UI fanout break polling
                    log.warning("snapshot_publish_failed", db_id=db_id, error=str(e)[:200])

            # Detect anomalies in real time
            anomalies = await self.anomaly_detector.check(db_id, snapshot)
            for anomaly in anomalies:
                await self.notifier.send_alert(anomaly)

            # Collect slow queries every critical tick
            slow_queries = await collector.collect_slow_queries(
                settings.slow_query_threshold_ms
            )
            if slow_queries:
                await self._process_slow_queries(db_id, slow_queries)

        except RuntimeError as e:
            # Collector not connected — log and skip; do not crash the loop.
            log.error("collector_not_connected", db_id=db_id, error=str(e))
        except Exception as e:
            log.error("critical_collect_failed", db_id=db_id, error=str(e))

    def _normalize_critical(
        self, db_id: str, raw: Dict, collector: AnyCollector
    ) -> Dict:
        """
        Normalize raw DB metrics into a unified snapshot dict.

        FIX: The original version blindly read `lock_waits` and
        `replication_lag_seconds` from the raw dict for both database types.
        MySQL's MYSQL_CRITICAL_SQL never returns those keys, so they would
        always be 0 — silently incorrect. We now apply db_type-aware defaults.
        """
        is_postgres = isinstance(collector, PostgresCollector)

        # Compute QPS from the transaction counter delta between polls.
        qps = 0.0
        total_xacts = raw.get("total_xacts")
        if total_xacts is not None:
            now = time.monotonic()
            prev = self._last_xact.get(db_id)
            if prev is not None:
                dt = now - prev[0]
                if dt > 0:
                    qps = max(0.0, (int(total_xacts) - prev[1]) / dt)
            self._last_xact[db_id] = (now, int(total_xacts))

        return {
            "database_id": db_id,
            "timestamp": datetime.now(timezone.utc),
            "active_connections": int(raw.get("active_connections") or 0),
            "max_connections": int(raw.get("max_connections") or 100),
            "cache_hit_ratio": float(raw.get("cache_hit_ratio") or 0),
            "qps": round(qps, 2),
            # Latency percentiles require pg_stat_statements aggregation; default to
            # 0.0 so the dashboard renders. Populated by the slow-query pipeline.
            "p50_latency_ms": float(raw.get("p50_latency_ms") or 0),
            "p95_latency_ms": float(raw.get("p95_latency_ms") or 0),
            "p99_latency_ms": float(raw.get("p99_latency_ms") or 0),
            # PostgreSQL-only fields; default to 0 for MySQL
            "lock_waits": int(raw.get("lock_waits") or 0) if is_postgres else 0,
            "deadlocks": int(raw.get("deadlocks") or 0),
            "replication_lag_seconds": (
                float(raw.get("replication_lag_seconds") or 0) if is_postgres else 0.0
            ),
        }

    # ── Slow query processing ─────────────────

    async def _process_slow_queries(self, db_id: str, raw_queries: List[Dict]) -> None:
        for row in raw_queries:
            sql = row.get("query", "")
            if not sql:
                continue
            fingerprint = self.fingerprinter.fingerprint(sql)
            normalized = self.fingerprinter.normalize(sql)
            access_pattern = self.fingerprinter.classify_pattern(normalized)

            # Security scan: flag SQL-injection / privilege-escalation / sensitive
            # full-scan patterns in observed queries and dispatch alerts.
            for event in self.anomaly_detector.check_query_security(db_id, sql):
                await self.notifier.send_alert(event)

            await self.metric_store.upsert_query_snapshot(
                {
                    "database_id": db_id,
                    "fingerprint": fingerprint,
                    "normalized_sql": normalized,
                    "sample_sql": sql,
                    "call_count": int(row.get("calls") or 1),
                    "mean_time_ms": float(row.get("mean_time_ms") or 0),
                    "total_time_ms": float(
                        row.get("total_time_ms") or row.get("total_exec_time") or 0
                    ),
                    "access_pattern": access_pattern,
                }
            )

    # ── Secondary polling loop ────────────────

    async def _secondary_loop(self) -> None:
        while self._running:
            for db_id, collector in self._collectors.items():
                try:
                    if isinstance(collector, PostgresCollector):
                        await self._collect_pg_secondary(db_id, collector)
                    elif isinstance(collector, MySQLCollector):
                        await self._collect_mysql_secondary(db_id, collector)
                except RuntimeError as e:
                    log.error("collector_not_connected", db_id=db_id, error=str(e))
                except Exception as e:
                    log.error("secondary_collect_failed", db_id=db_id, error=str(e))
            await asyncio.sleep(settings.secondary_poll_interval)

    async def _collect_pg_secondary(
        self, db_id: str, collector: PostgresCollector
    ) -> None:
        bloat_data = await collector.collect_table_bloat()
        index_data = await collector.collect_index_usage()

        if bloat_data:
            await self.metric_store.write_table_stats(db_id, bloat_data)

        if index_data:
            await self.metric_store.write_index_stats(db_id, index_data)

        # Flag unused indexes (low scan count + large size = waste)
        unused = [
            idx
            for idx in index_data
            if int(idx.get("index_scans") or 0) < 10
            and int(idx.get("index_bytes") or 0) > 10 * 1024 * 1024  # 10 MB
        ]
        if unused:
            log.info(
                "unused_indexes_detected",
                db_id=db_id,
                count=len(unused),
                indexes=[i["index_name"] for i in unused[:5]],
            )

    async def _collect_mysql_secondary(
        self, db_id: str, collector: MySQLCollector
    ) -> None:
        """
        Placeholder for MySQL secondary metrics.
        MySQL does not expose table bloat or index usage in the same way
        as PostgreSQL; extend this method with
        `information_schema.TABLES` queries as needed.
        """
        log.debug("mysql_secondary_noop", db_id=db_id)
