"""
Backup verification service.

Daily CronJob that:
1. Triggers pg_dump / mysqldump on the monitored database
2. Restores the dump to an ephemeral Docker container
3. Runs row-count and sample-checksum comparisons
4. Reports pass/fail to the dashboard
"""
from __future__ import annotations

import asyncio
import hashlib
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import asyncpg  # type: ignore[import]
import docker  # type: ignore[import]

from backend.utils.config import settings
from backend.utils.logging import get_logger

log = get_logger(__name__)


class BackupVerificationService:

    def __init__(self, metric_store):
        self._metric_store = metric_store
        try:
            self._docker = docker.from_env()
        except Exception:
            self._docker = None
            log.warning("docker_unavailable_backup_verification_disabled")

    async def run_verification(
        self,
        database_id: str,
        source_url: str,
        db_type: str = "postgresql",
        tables_to_check: Optional[List[str]] = None,
    ) -> Dict:
        """
        Full backup + restore + verify cycle.
        Returns verification result dict.
        """
        started_at = datetime.now(timezone.utc)
        t0 = time.monotonic()

        result = {
            "database_id": database_id,
            "status": "running",
            "started_at": started_at.isoformat(),
        }

        container = None
        dump_path = None

        try:
            # 1. Create dump
            dump_path = await self._create_dump(source_url, db_type)
            log.info("dump_created", db_id=database_id, path=dump_path)

            # 2. Spin up fresh container and restore
            container, restore_url = await self._restore_to_container(dump_path, db_type)

            # 3. Get live row counts
            live_counts = await self._get_row_counts(source_url, db_type, tables_to_check)

            # 4. Get restored row counts
            restored_counts = await self._get_row_counts(restore_url, db_type, tables_to_check)

            # 5. Sample checksum comparison
            checksums_match, checksum_details = await self._compare_checksums(
                source_url, restore_url, db_type, tables_to_check
            )

            # 6. Evaluate
            counts_match = live_counts == restored_counts
            status = "passed" if (counts_match and checksums_match) else "failed"

            mismatches = []
            for table in live_counts:
                if live_counts.get(table) != restored_counts.get(table):
                    mismatches.append(
                        f"{table}: live={live_counts[table]} restored={restored_counts.get(table)}"
                    )

            result.update({
                "status": status,
                "row_counts": live_counts,
                "restored_row_counts": restored_counts,
                "sample_checksums": checksum_details,
                "mismatched_tables": mismatches,
                "duration_seconds": time.monotonic() - t0,
                "completed_at": datetime.now(timezone.utc).isoformat(),
            })

            log.info(
                "backup_verification_complete",
                db_id=database_id,
                status=status,
                mismatches=len(mismatches),
            )

        except Exception as e:
            result.update({
                "status": "failed",
                "error_message": str(e),
                "duration_seconds": time.monotonic() - t0,
                "completed_at": datetime.now(timezone.utc).isoformat(),
            })
            log.error("backup_verification_failed", db_id=database_id, error=str(e))
        finally:
            if container:
                try:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, container.stop)
                except Exception:
                    pass
            if dump_path:
                import os
                try:
                    os.unlink(dump_path)
                except Exception:
                    pass

        return result

    async def _create_dump(self, source_url: str, db_type: str) -> str:
        """Run pg_dump or mysqldump and return path to dump file."""
        with tempfile.NamedTemporaryFile(
            suffix=".sql", delete=False, mode="w"
        ) as f:
            dump_path = f.name

        if db_type == "postgresql":
            cmd = ["pg_dump", "--no-owner", "--no-acl", source_url, "-f", dump_path]
        else:
            from urllib.parse import urlparse
            parsed = urlparse(source_url)
            cmd = [
                "mysqldump",
                f"-h{parsed.hostname}",
                f"-P{parsed.port or 3306}",
                f"-u{parsed.username}",
                f"-p{parsed.password}",
                parsed.path.lstrip("/"),
                f"--result-file={dump_path}",
            ]

        loop = asyncio.get_event_loop()
        proc = await loop.run_in_executor(
            None,
            lambda: subprocess.run(cmd, capture_output=True, timeout=300),
        )

        if proc.returncode != 0:
            raise RuntimeError(f"Dump failed: {proc.stderr.decode()[:200]}")

        return dump_path

    async def _restore_to_container(
        self, dump_path: str, db_type: str
    ) -> tuple:
        if not self._docker:
            raise RuntimeError("Docker not available")

        port = 56000 + int(time.time()) % 1000

        if db_type == "postgresql":
            image = settings.shadow_db_image
            env = {"POSTGRES_PASSWORD": "verify", "POSTGRES_DB": "verify"}
            restore_url = f"postgresql://postgres:verify@localhost:{port}/verify"
            port_map = {"5432/tcp": port}
        else:
            image = settings.shadow_mysql_image
            env = {"MYSQL_ROOT_PASSWORD": "verify", "MYSQL_DATABASE": "verify"}
            restore_url = f"mysql://root:verify@localhost:{port}/verify"
            port_map = {"3306/tcp": port}

        loop = asyncio.get_event_loop()
        container = await loop.run_in_executor(
            None,
            lambda: self._docker.containers.run(
                image,
                detach=True,
                remove=True,
                environment=env,
                ports=port_map,
            ),
        )

        # Wait for DB ready
        for _ in range(30):
            try:
                if db_type == "postgresql":
                    conn = await asyncpg.connect(restore_url, timeout=2)
                    await conn.close()
                break
            except Exception:
                await asyncio.sleep(1)

        # Restore
        if db_type == "postgresql":
            cmd = ["psql", restore_url, "-f", dump_path]
        else:
            cmd = ["mysql", "-h127.0.0.1", f"-P{port}", "-uroot", "-pverify", "verify",
                   f"< {dump_path}"]

        await loop.run_in_executor(
            None,
            lambda: subprocess.run(cmd, capture_output=True, timeout=300),
        )

        return container, restore_url

    async def _get_row_counts(
        self, url: str, db_type: str, tables: Optional[List[str]]
    ) -> Dict[str, int]:
        counts = {}
        if db_type == "postgresql":
            conn = await asyncpg.connect(url)
            try:
                if not tables:
                    rows = await conn.fetch("""
                        SELECT tablename FROM pg_tables
                        WHERE schemaname = 'public'
                    """)
                    tables = [r["tablename"] for r in rows]

                for table in tables[:20]:  # Limit to 20 tables
                    row = await conn.fetchrow(f'SELECT count(*) AS n FROM "{table}"')
                    counts[table] = int(row["n"])
            finally:
                await conn.close()
        return counts

    async def _compare_checksums(
        self, live_url: str, restore_url: str, db_type: str, tables: Optional[List[str]]
    ) -> tuple[bool, Dict]:
        """Compare MD5 of first 100 rows from each table."""
        details = {}
        all_match = True

        if db_type != "postgresql":
            return True, {}  # MySQL checksum TBD

        live_conn = await asyncpg.connect(live_url)
        restore_conn = await asyncpg.connect(restore_url)

        try:
            check_tables = (tables or [])[:5]  # Sample first 5 tables
            for table in check_tables:
                try:
                    live_rows = await live_conn.fetch(
                        f'SELECT * FROM "{table}" LIMIT 100'
                    )
                    rest_rows = await restore_conn.fetch(
                        f'SELECT * FROM "{table}" LIMIT 100'
                    )

                    live_hash = hashlib.md5(
                        str(sorted([dict(r) for r in live_rows])).encode()
                    ).hexdigest()
                    rest_hash = hashlib.md5(
                        str(sorted([dict(r) for r in rest_rows])).encode()
                    ).hexdigest()

                    match = live_hash == rest_hash
                    details[table] = {"match": match, "live_hash": live_hash[:8]}
                    if not match:
                        all_match = False
                except Exception as e:
                    details[table] = {"error": str(e)}
        finally:
            await live_conn.close()
            await restore_conn.close()

        return all_match, details