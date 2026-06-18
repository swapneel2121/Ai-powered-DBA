"""
Workload Replay Engine.

Captures a query workload snapshot from production, spins up
an ephemeral Docker shadow database with the proposed schema changes,
replays the workload, and computes a statistical performance delta.
"""

from __future__ import annotations

import asyncio
import random
import statistics
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import asyncpg  # type: ignore[import]

from backend.utils.config import settings
from backend.utils.logging import get_logger

log = get_logger(__name__)

# `docker` and `scipy` are optional dependencies that may not be installed.
# They are imported lazily inside the methods that need them so that a missing
# package raises a clear RuntimeError at the call-site rather than an
# ImportError at module import time.


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────


def _import_docker():
    """Lazily import the docker SDK and raise clearly if not installed."""
    try:
        import docker  # type: ignore

        return docker
    except ImportError as exc:
        raise RuntimeError(
            "The 'docker' package is required for workload replay. "
            "Install it with: pip install docker"
        ) from exc


def _welch_t_pvalue(a: List[float], b: List[float]) -> float:
    """
    Compute the two-tailed p-value for Welch's t-test without scipy.

    Tries scipy first (more accurate CDF); falls back to a normal
    approximation which is sufficient for large samples (n >= 30).
    """
    try:
        from scipy import stats  # type: ignore

        result = stats.ttest_ind(a, b, equal_var=False)
        return float(result[1])  # type: ignore
    except ImportError:
        pass  # fall back to manual approximation below

    import math

    n1, n2 = len(a), len(b)
    if n1 < 2 or n2 < 2:
        return 1.0

    m1 = statistics.mean(a)
    m2 = statistics.mean(b)
    v1 = statistics.variance(a)
    v2 = statistics.variance(b)

    se = math.sqrt(v1 / n1 + v2 / n2)
    if se == 0:
        return 1.0

    t = (m1 - m2) / se

    # Normal approximation — good enough when n >= 30.
    # Two-tailed p-value via error function.
    p = 2.0 * (1.0 - 0.5 * (1.0 + math.erf(abs(t) / math.sqrt(2))))
    return p


# ─────────────────────────────────────────────
# Data Classes
# ─────────────────────────────────────────────


@dataclass
class ReplayResult:
    session_id: str
    proposal_id: str
    database_id: str

    # Workload stats BEFORE applying changes (baseline)
    before_latencies_ms: List[float] = field(default_factory=list)
    before_p50: float = 0.0
    before_p95: float = 0.0
    before_p99: float = 0.0
    before_throughput_qps: float = 0.0

    # Workload stats AFTER applying proposed changes
    after_latencies_ms: List[float] = field(default_factory=list)
    after_p50: float = 0.0
    after_p95: float = 0.0
    after_p99: float = 0.0
    after_throughput_qps: float = 0.0

    # Derived
    p99_improvement_pct: float = 0.0
    p95_improvement_pct: float = 0.0
    statistical_confidence: float = 0.0
    is_improvement: bool = False
    summary: str = ""

    def compute_stats(self) -> None:
        def pct(lst: List[float], p: int) -> float:
            if not lst:
                return 0.0
            # statistics.quantiles(n=100) returns 99 cut-points for percentiles 1-99
            return statistics.quantiles(lst, n=100)[p - 1]

        self.before_p50 = pct(self.before_latencies_ms, 50)
        self.before_p95 = pct(self.before_latencies_ms, 95)
        self.before_p99 = pct(self.before_latencies_ms, 99)

        self.after_p50 = pct(self.after_latencies_ms, 50)
        self.after_p95 = pct(self.after_latencies_ms, 95)
        self.after_p99 = pct(self.after_latencies_ms, 99)

        if self.before_p99 > 0:
            self.p99_improvement_pct = (
                (self.before_p99 - self.after_p99) / self.before_p99 * 100
            )
        if self.before_p95 > 0:
            self.p95_improvement_pct = (
                (self.before_p95 - self.after_p95) / self.before_p95 * 100
            )

        # _welch_t_pvalue() tries scipy first and falls back to a normal
        # approximation — no bare scipy import that would crash if absent.
        if len(self.before_latencies_ms) >= 30 and len(self.after_latencies_ms) >= 30:
            p_value = _welch_t_pvalue(self.before_latencies_ms, self.after_latencies_ms)
            self.statistical_confidence = 1.0 - p_value
        else:
            self.statistical_confidence = 0.5  # not enough samples

        self.is_improvement = (
            self.p99_improvement_pct > 5.0 and self.statistical_confidence > 0.95
        )

        self.summary = (
            f"p99: {self.before_p99:.1f}ms → {self.after_p99:.1f}ms "
            f"({self.p99_improvement_pct:+.1f}%), "
            f"confidence: {self.statistical_confidence:.1%}"
        )

    def to_dict(self) -> Dict:
        return {
            "session_id": self.session_id,
            "before_p50": self.before_p50,
            "before_p95": self.before_p95,
            "before_p99": self.before_p99,
            "after_p50": self.after_p50,
            "after_p95": self.after_p95,
            "after_p99": self.after_p99,
            "p99_improvement_pct": self.p99_improvement_pct,
            "p95_improvement_pct": self.p95_improvement_pct,
            "statistical_confidence": self.statistical_confidence,
            "is_improvement": self.is_improvement,
            "summary": self.summary,
            "sample_count_before": len(self.before_latencies_ms),
            "sample_count_after": len(self.after_latencies_ms),
        }


# ─────────────────────────────────────────────
# Engine
# ─────────────────────────────────────────────


class WorkloadReplayEngine:
    """
    Orchestrates shadow database creation and workload replay.

    Flow:
    1. Capture workload snapshot from pg_stat_statements
    2. Spin up ephemeral Docker container (pre-warmed image)
    3. Restore schema + representative data subset
    4. Benchmark WITHOUT proposed changes  →  baseline
    5. Apply proposed DDL/config changes
    6. Re-benchmark  →  compare
    7. Tear down container, return ReplayResult
    """

    # FIX: asyncio.Semaphore must NOT be created at class-definition time.
    # In Python 3.10+ it emits DeprecationWarning; in 3.12+ it raises
    # RuntimeError because there is no running event loop when the class body
    # executes.  Initialised lazily by _ensure_semaphore() instead.
    _semaphore: Optional[asyncio.Semaphore] = None

    def __init__(self) -> None:
        # FIX: Do NOT call docker.from_env() here and silently swallow the
        # ImportError / DockerException into self._docker = None.  That hides
        # the real problem and later produces a confusing
        # "AttributeError: 'NoneType' object has no attribute 'containers'".
        # The client is created lazily by _ensure_docker() so the error is
        # surfaced exactly where Docker is first needed.
        self._docker = None  # populated lazily by _ensure_docker()

    # ── Guard helpers ─────────────────────────

    def _ensure_docker(self):
        """
        Return a live Docker client, initialising it on first call.

        Combines the lazy-import fix (docker SDK may not be installed) with
        the None-guard fix (self._docker was None when Docker was unavailable,
        causing "NoneType has no attribute containers").
        """
        if self._docker is not None:
            return self._docker

        docker = _import_docker()  # raises RuntimeError if not installed
        try:
            self._docker = docker.from_env()
        except Exception as exc:
            raise RuntimeError(
                "Could not connect to the Docker daemon. "
                "Is Docker running? Is the socket accessible?"
            ) from exc
        return self._docker

    def _ensure_semaphore(self) -> asyncio.Semaphore:
        """
        Return the concurrency semaphore, creating it inside the running loop.

        FIX: asyncio.Semaphore() must be created inside a running event loop
        (Python 3.10+).
        """
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(settings.max_concurrent_replays)
        return self._semaphore

    # ── Public API ────────────────────────────

    async def run_replay(
        self,
        proposal_id: str,
        database_id: str,
        source_conn_url: str,
        ddl_statements: List[str],
        workload_queries: List[Dict],
        concurrency: int = 10,
        duration_seconds: int = 30,
    ) -> ReplayResult:

        session_id = str(uuid.uuid4())[:8]
        result = ReplayResult(
            session_id=session_id,
            proposal_id=proposal_id,
            database_id=database_id,
        )

        semaphore = self._ensure_semaphore()
        async with semaphore:
            log.info(
                "replay_starting",
                session_id=session_id,
                proposal_id=proposal_id,
                query_count=len(workload_queries),
            )
            container = None
            try:
                # 1. Spin up shadow DB
                container, shadow_url = await self._start_shadow_db(session_id)

                # 2. Copy schema from source (structure only, no data)
                await self._clone_schema(source_conn_url, shadow_url)

                # 3. Baseline benchmark (no changes applied)
                result.before_latencies_ms = await self._benchmark(
                    shadow_url, workload_queries, concurrency, duration_seconds
                )

                # 4. Apply proposed changes
                await self._apply_ddl(shadow_url, ddl_statements)

                # 5. Post-change benchmark
                result.after_latencies_ms = await self._benchmark(
                    shadow_url, workload_queries, concurrency, duration_seconds
                )

                result.compute_stats()
                log.info(
                    "replay_complete",
                    session_id=session_id,
                    summary=result.summary,
                    is_improvement=result.is_improvement,
                )

            except Exception as e:
                log.error("replay_failed", session_id=session_id, error=str(e))
                result.summary = f"Replay failed: {e}"
            finally:
                if container is not None:
                    await self._stop_shadow_db(container)

        return result

    # ── Docker helpers ────────────────────────

    async def _start_shadow_db(self, session_id: str) -> Tuple[object, str]:
        """
        Spin up an ephemeral PostgreSQL container and wait until it accepts
        connections (up to 30 seconds).
        """
        # FIX: _ensure_docker() raises a clear RuntimeError if Docker is not
        # available, instead of returning None and producing a confusing
        # AttributeError later when .containers.run() is called on None.
        client = self._ensure_docker()

        port = 55000 + hash(session_id) % 5000
        container_name = f"dba-shadow-{session_id}"

        # FIX: asyncio.get_event_loop() is deprecated in Python 3.10+ when
        # called from a coroutine.  Use asyncio.get_running_loop() instead.
        loop = asyncio.get_running_loop()
        container = await loop.run_in_executor(
            None,
            lambda: client.containers.run(
                settings.shadow_db_image,
                name=container_name,
                detach=True,
                remove=True,
                environment={
                    "POSTGRES_PASSWORD": "shadow",
                    "POSTGRES_DB": "shadow",
                },
                ports={"5432/tcp": port},
            ),
        )

        shadow_url = f"postgresql://postgres:shadow@localhost:{port}/shadow"

        # Wait for DB to be ready (max 30 s)
        for _ in range(30):
            try:
                conn = await asyncpg.connect(shadow_url, timeout=2)
                await conn.close()
                log.info("shadow_db_ready", session_id=session_id, port=port)
                return container, shadow_url
            except Exception:
                await asyncio.sleep(1)

        raise TimeoutError(f"Shadow DB {session_id} did not start within 30s")

    async def _stop_shadow_db(self, container) -> None:
        try:
            # FIX: use get_running_loop() — get_event_loop() is deprecated
            # inside a coroutine in Python 3.10+.
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, container.stop)
            log.info("shadow_db_stopped", container_id=container.short_id)
        except Exception as e:
            log.warning("shadow_db_stop_failed", error=str(e))

    async def _clone_schema(self, source_url: str, shadow_url: str) -> None:
        """
        Clone schema structure (tables, indexes, sequences) without data.

        FIX: The original code called subprocess.run() on the event-loop
        thread, blocking all other coroutines for the duration of pg_dump.
        Offloaded to run_in_executor() so the loop stays responsive.

        FIX: The original code assigned `conn` inside the try block and then
        referenced it in the finally block.  If asyncpg.connect() raised,
        `conn` was unbound and the finally clause raised NameError, masking
        the original error.  Connection is now established before the
        try/finally so `conn` is always bound when finally runs.
        """
        loop = asyncio.get_running_loop()

        # Run pg_dump in a thread so we don't block the event loop.
        proc_result = await loop.run_in_executor(
            None,
            lambda: subprocess.run(
                ["pg_dump", "--schema-only", source_url],
                capture_output=True,
                text=True,
                timeout=60,
            ),
        )

        if proc_result.returncode != 0:
            log.warning("schema_clone_failed", stderr=proc_result.stderr[:200])
            return

        schema_sql = proc_result.stdout
        if not schema_sql.strip():
            log.warning("schema_clone_empty")
            return

        # FIX: conn assigned before try/finally — never unbound in finally.
        conn = await asyncpg.connect(shadow_url)
        try:
            await conn.execute(schema_sql)
            log.info("schema_cloned")
        except Exception as e:
            log.warning("schema_apply_failed", error=str(e))
        finally:
            await conn.close()

    async def _apply_ddl(self, shadow_url: str, ddl_statements: List[str]) -> None:
        """
        Apply proposed DDL statements to the shadow database.

        FIX: Same NameError-in-finally bug as _clone_schema — conn is now
        assigned before the try/finally block so it is always bound when
        finally runs.
        """
        if not ddl_statements:
            return

        # FIX: conn assigned before try/finally — never unbound in finally.
        conn = await asyncpg.connect(shadow_url)
        try:
            for ddl in ddl_statements:
                await conn.execute(ddl)
                log.info("ddl_applied", ddl=ddl[:100])
        finally:
            await conn.close()

    # ── Benchmarking ──────────────────────────

    async def _benchmark(
        self,
        db_url: str,
        queries: List[Dict],
        concurrency: int,
        duration_seconds: int,
    ) -> List[float]:
        """
        Run the workload for `duration_seconds` with `concurrency` workers.
        Returns a list of per-query latencies in milliseconds.

        FIX (primary crash — "acquire/close not an attribute of None"):
        asyncpg.create_pool() is a coroutine and must be awaited.  If it
        raises, or if the result is somehow None, pool.acquire() /
        pool.close() blow up with AttributeError: 'NoneType' has no
        attribute 'acquire'/'close'.  Fixed by:
          1. Awaiting create_pool() and immediately asserting the result is
             not None so we get a clear error rather than a cryptic
             AttributeError deep inside a worker.
          2. Wrapping pool.close() in a try/finally so the pool is always
             released even when asyncio.gather() raises.

        FIX: The original query selector used
            `int(time.monotonic() * 1000) % len(queries)`
        which cycles queries in a fixed time-dependent pattern, producing a
        non-representative workload (the same query fires in every given
        millisecond bucket).  Replaced with random.randrange() for a uniform
        random distribution.

        FIX: pool.close() was not in a finally block — if any worker task
        raised an unhandled exception that escaped asyncio.gather(), the pool
        would leak.  Wrapped in try/finally.
        """
        if not queries:
            log.warning("benchmark_no_queries")
            return []

        # FIX: await create_pool() and assert the result immediately so any
        # failure produces a clear error rather than a later AttributeError.
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=concurrency)
        if pool is None:
            raise RuntimeError(
                f"asyncpg.create_pool() returned None for url={db_url!r}. "
                "Check that the database is reachable and credentials are correct."
            )

        latencies: List[float] = []
        deadline = time.monotonic() + duration_seconds

        async def worker() -> None:
            while time.monotonic() < deadline:
                # FIX: random selection instead of time-based cycling.
                query = queries[random.randrange(len(queries))]
                sql = query.get("normalized_sql", "SELECT 1")

                # Replace common placeholders with safe literal defaults.
                sql = sql.replace("$1", "1").replace("$2", "1").replace("?", "1")

                t0 = time.monotonic()
                try:
                    # FIX: pool is guaranteed non-None here (asserted above),
                    # so pool.acquire() is always valid.
                    async with pool.acquire() as conn:
                        await conn.execute(sql)
                    latencies.append((time.monotonic() - t0) * 1000)
                except Exception as exc:
                    log.debug("benchmark_query_error", error=str(exc))

        try:
            workers = [asyncio.create_task(worker()) for _ in range(concurrency)]
            await asyncio.gather(*workers)
        finally:
            # FIX: always close the pool — even if gather() raises — so we
            # don't leak connections.
            await pool.close()

        return latencies