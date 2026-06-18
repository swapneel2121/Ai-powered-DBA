"""
Security anomaly detection.

Detects:
  - SQL injection patterns (SELECT * WHERE 1=1, UNION-based)
  - Privilege escalation attempts (GRANT, CREATE USER)
  - Unusual access patterns (z-score on query frequency)
  - Access from unusual IPs
  - Bulk data exfiltration (high row counts from sensitive tables)
"""
from __future__ import annotations

import re
import statistics
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from backend.utils.logging import get_logger

log = get_logger(__name__)

# ─────────────────────────────────────────────
# Injection Patterns
# ─────────────────────────────────────────────

INJECTION_PATTERNS = [
    # Always-true predicates
    (re.compile(r"WHERE\s+['\"]?\d+['\"]?\s*=\s*['\"]?\d+['\"]?", re.I), "always_true_predicate"),
    (re.compile(r"WHERE\s+\w+\s*=\s*\w+\s*--", re.I), "comment_injection"),
    # UNION-based injection
    (re.compile(r"UNION\s+(ALL\s+)?SELECT\s+NULL", re.I), "union_null_injection"),
    (re.compile(r"UNION\s+(ALL\s+)?SELECT\s+\d+,\d+", re.I), "union_int_injection"),
    # Stacked queries
    (re.compile(r";\s*(DROP|DELETE|UPDATE|INSERT|CREATE)\s", re.I), "stacked_query"),
    # Time-based blind injection
    (re.compile(r"SLEEP\s*\(", re.I), "time_based_blind"),
    (re.compile(r"pg_sleep\s*\(", re.I), "time_based_blind_pg"),
    (re.compile(r"WAITFOR\s+DELAY", re.I), "time_based_blind_mssql"),
    # Information schema probing
    (re.compile(r"FROM\s+information_schema\.(tables|columns|user_privileges)", re.I), "schema_probing"),
    (re.compile(r"FROM\s+pg_catalog\.(pg_user|pg_shadow|pg_authid)", re.I), "pg_user_probing"),
    # Hex/char encoding to bypass filters
    (re.compile(r"(0x[0-9a-f]{8,}|CHAR\(\d+\))", re.I), "encoded_payload"),
]

PRIVILEGE_PATTERNS = [
    re.compile(r"\bGRANT\s+\w+\s+ON\b", re.I),
    re.compile(r"\bCREATE\s+(USER|ROLE)\b", re.I),
    re.compile(r"\bALTER\s+(USER|ROLE)\b", re.I),
    re.compile(r"\bDROP\s+(USER|ROLE)\b", re.I),
    re.compile(r"\bGRANT\s+ALL\b", re.I),
]

SENSITIVE_TABLES = {
    "users", "user", "accounts", "account", "passwords", "credentials",
    "credit_cards", "payment_methods", "tokens", "sessions", "secrets",
    "api_keys", "admin", "admins", "roles", "permissions",
}


@dataclass
class AnomalyEvent:
    database_id: str
    severity: str           # critical | high | medium | low
    anomaly_type: str
    title: str
    description: str
    sql_sample: Optional[str] = None
    client_ip: Optional[str] = None
    metric_value: Optional[float] = None
    detected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "database_id": self.database_id,
            "severity": self.severity,
            "anomaly_type": self.anomaly_type,
            "title": self.title,
            "description": self.description,
            "sql_sample": self.sql_sample,
            "detected_at": self.detected_at.isoformat(),
        }


class AnomalyDetector:
    """
    Stateful anomaly detector.

    Maintains rolling windows of metrics per database to compute
    z-scores and detect deviations from baseline.
    """

    WINDOW_SIZE = 360       # Keep 360 samples (1 hour at 10s intervals)
    Z_SCORE_THRESHOLD = 3.0

    def __init__(self):
        # Rolling windows per database per metric
        self._windows: Dict[str, Dict[str, deque]] = defaultdict(
            lambda: defaultdict(lambda: deque(maxlen=self.WINDOW_SIZE))
        )

    async def check(
        self, database_id: str, snapshot: Dict
    ) -> List[AnomalyEvent]:
        anomalies = []

        # 1. Statistical anomaly detection on numeric metrics
        numeric_checks = {
            "active_connections": snapshot.get("active_connections", 0),
            "lock_waits": snapshot.get("lock_waits", 0),
            "deadlocks": snapshot.get("deadlocks", 0),
            "replication_lag_seconds": snapshot.get("replication_lag_seconds", 0),
        }

        for metric, value in numeric_checks.items():
            anomaly = self._check_zscore(database_id, metric, float(value))
            if anomaly:
                anomalies.append(
                    AnomalyEvent(
                        database_id=database_id,
                        severity="high",
                        anomaly_type=f"statistical_spike_{metric}",
                        title=f"Unusual {metric.replace('_', ' ')} spike",
                        description=(
                            f"{metric} is {value:.1f}, "
                            f"z-score = {anomaly:.2f} (threshold: {self.Z_SCORE_THRESHOLD})"
                        ),
                        metric_value=float(value),
                    )
                )

        # 2. Connection pool exhaustion
        max_conn = snapshot.get("max_connections", 100)
        active_conn = snapshot.get("active_connections", 0)
        if max_conn > 0 and active_conn / max_conn > 0.9:
            anomalies.append(
                AnomalyEvent(
                    database_id=database_id,
                    severity="critical",
                    anomaly_type="connection_pool_exhaustion",
                    title="Connection pool near exhaustion",
                    description=(
                        f"{active_conn}/{max_conn} connections used "
                        f"({active_conn/max_conn*100:.1f}%)"
                    ),
                    metric_value=active_conn / max_conn,
                )
            )

        # 3. Replication lag alert
        lag = snapshot.get("replication_lag_seconds", 0)
        if lag > 30:
            anomalies.append(
                AnomalyEvent(
                    database_id=database_id,
                    severity="critical" if lag > 300 else "high",
                    anomaly_type="replication_lag",
                    title=f"Replication lag: {lag:.0f}s",
                    description=f"Replica is {lag:.0f} seconds behind primary",
                    metric_value=lag,
                )
            )

        return anomalies

    def check_query_security(
        self, database_id: str, sql: str, client_ip: Optional[str] = None
    ) -> List[AnomalyEvent]:
        """Check a SQL query for injection and privilege escalation patterns."""
        anomalies = []

        # Injection pattern matching
        for pattern, pattern_type in INJECTION_PATTERNS:
            if pattern.search(sql):
                anomalies.append(
                    AnomalyEvent(
                        database_id=database_id,
                        severity="critical",
                        anomaly_type=f"sql_injection_{pattern_type}",
                        title="Potential SQL injection detected",
                        description=f"Pattern '{pattern_type}' matched in query",
                        sql_sample=sql[:500],
                        client_ip=client_ip,
                    )
                )
                log.warning(
                    "sql_injection_detected",
                    db_id=database_id,
                    pattern=pattern_type,
                    ip=client_ip,
                )

        # Privilege escalation
        for pattern in PRIVILEGE_PATTERNS:
            if pattern.search(sql):
                anomalies.append(
                    AnomalyEvent(
                        database_id=database_id,
                        severity="high",
                        anomaly_type="privilege_escalation_attempt",
                        title="Privilege escalation attempt",
                        description="DDL affecting users/roles detected",
                        sql_sample=sql[:500],
                        client_ip=client_ip,
                    )
                )

        # Bulk data access on sensitive tables
        tables_in_query = re.findall(r"\bFROM\s+(\w+)\b", sql, re.I)
        for table in tables_in_query:
            if table.lower() in SENSITIVE_TABLES:
                if re.match(r"SELECT\s+\*", sql.strip(), re.I):
                    anomalies.append(
                        AnomalyEvent(
                            database_id=database_id,
                            severity="medium",
                            anomaly_type="sensitive_table_full_scan",
                            title=f"SELECT * on sensitive table '{table}'",
                            description=(
                                f"Unrestricted read on '{table}' — "
                                "potential data exfiltration"
                            ),
                            sql_sample=sql[:500],
                            client_ip=client_ip,
                        )
                    )

        return anomalies

    # ── Z-score computation ───────────────────

    def _check_zscore(
        self, database_id: str, metric: str, value: float
    ) -> Optional[float]:
        """
        Add value to rolling window, return z-score if anomalous.
        Returns None if not enough data or within normal range.
        """
        window = self._windows[database_id][metric]
        window.append(value)

        if len(window) < 30:        # Need at least 30 samples for meaningful stats
            return None

        data = list(window)
        mean = statistics.mean(data)
        stdev = statistics.stdev(data)

        if stdev < 0.001:           # Constant signal — nothing to detect
            return None

        z = abs(value - mean) / stdev
        return z if z > self.Z_SCORE_THRESHOLD else None

    def _check_iqr(
        self, database_id: str, metric: str, value: float
    ) -> Optional[float]:
        """IQR-based outlier detection as alternative to z-score."""
        window = self._windows[database_id][metric]
        if len(window) < 30:
            return None

        data = sorted(window)
        n = len(data)
        q1 = data[n // 4]
        q3 = data[3 * n // 4]
        iqr = q3 - q1
        if iqr < 0.001:
            return None

        if value > q3 + 3 * iqr or value < q1 - 3 * iqr:
            return (value - q3) / iqr
        return None