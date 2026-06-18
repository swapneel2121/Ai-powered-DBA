"""SQLAlchemy ORM models for the DBA agent's internal state."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from enum import Enum as PyEnum

from sqlalchemy import (
    Column, String, Integer, Float, Boolean, DateTime,
    Text, Enum, JSON, ForeignKey, Index, event
)
from sqlalchemy.orm import DeclarativeBase, relationship
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


# ─────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────

class DatabaseType(str, PyEnum):
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"


class ProposalState(str, PyEnum):
    PROPOSED = "proposed"
    REVIEWED = "reviewed"
    APPROVED = "approved"
    TESTING = "testing"
    DEPLOYING = "deploying"
    MONITORING = "monitoring"
    COMPLETED = "completed"
    ROLLED_BACK = "rolled_back"
    REJECTED = "rejected"


class AlertSeverity(str, PyEnum):
    P1 = "p1"   # Critical: page immediately
    P2 = "p2"   # High: Slack
    P3 = "p3"   # Info: email digest


class AlertStatus(str, PyEnum):
    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    SILENCED = "silenced"


# ─────────────────────────────────────────────
# Database Registry
# ─────────────────────────────────────────────

class MonitoredDatabase(Base):
    __tablename__ = "monitored_databases"

    id = Column(String(64), primary_key=True)  # SHA256 of connection URL
    name = Column(String(255), nullable=False)
    db_type = Column(Enum(DatabaseType), nullable=False)
    host = Column(String(255), nullable=False)
    port = Column(Integer, nullable=False)
    database_name = Column(String(255), nullable=False)
    username = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    last_seen_at = Column(DateTime(timezone=True))

    proposals = relationship("OptimizationProposal", back_populates="database")
    alerts = relationship("Alert", back_populates="database")


# ─────────────────────────────────────────────
# Query Snapshots
# ─────────────────────────────────────────────

class QuerySnapshot(Base):
    """Normalized query fingerprint with aggregated stats."""
    __tablename__ = "query_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    database_id = Column(String(64), ForeignKey("monitored_databases.id"), nullable=False)
    fingerprint = Column(String(64), nullable=False)    # LSH hash of normalized SQL
    normalized_sql = Column(Text, nullable=False)
    sample_sql = Column(Text)                           # One real example with literals
    call_count = Column(Integer, default=0)
    total_time_ms = Column(Float, default=0)
    mean_time_ms = Column(Float, default=0)
    p95_time_ms = Column(Float, default=0)
    p99_time_ms = Column(Float, default=0)
    rows_examined = Column(Integer, default=0)
    rows_sent = Column(Integer, default=0)
    access_pattern = Column(String(32))                 # oltp_point, olap_scan, batch_insert, ddl
    captured_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("ix_qs_db_fingerprint", "database_id", "fingerprint"),
        Index("ix_qs_captured_at", "captured_at"),
    )


# ─────────────────────────────────────────────
# Optimization Proposals (State Machine)
# ─────────────────────────────────────────────

class OptimizationProposal(Base):
    __tablename__ = "optimization_proposals"

    id = Column(String(64), primary_key=True)           # UUID
    database_id = Column(String(64), ForeignKey("monitored_databases.id"), nullable=False)
    title = Column(String(500), nullable=False)
    proposal_type = Column(String(64))                  # index, query_rewrite, config, partition, vacuum
    state = Column(Enum(ProposalState), default=ProposalState.PROPOSED, nullable=False)

    # Content
    original_sql = Column(Text)
    optimized_sql = Column(Text)
    ddl_statements = Column(JSON)                       # List of DDL to execute
    config_changes = Column(JSON)                       # Dict of config key→value
    llm_rationale = Column(Text)
    estimated_improvement_pct = Column(Float)
    estimated_impact_score = Column(Float)              # 0-100

    # Replay results
    replay_summary = Column(JSON)
    replay_before_p99_ms = Column(Float)
    replay_after_p99_ms = Column(Float)
    replay_confidence = Column(Float)

    # Approval workflow
    reviewed_by = Column(String(255))
    review_comment = Column(Text)
    approved_by = Column(String(255))
    approved_at = Column(DateTime(timezone=True))

    # Deployment tracking
    deployed_at = Column(DateTime(timezone=True))
    rolled_back_at = Column(DateTime(timezone=True))
    rollback_reason = Column(Text)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    database = relationship("MonitoredDatabase", back_populates="proposals")
    audit_entries = relationship("AuditLog", back_populates="proposal")

    __table_args__ = (
        Index("ix_op_state", "state"),
        Index("ix_op_database_created", "database_id", "created_at"),
    )


# ─────────────────────────────────────────────
# Immutable Audit Log
# ─────────────────────────────────────────────

class AuditLog(Base):
    """Append-only audit trail with row-level checksums."""
    __tablename__ = "audit_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    proposal_id = Column(String(64), ForeignKey("optimization_proposals.id"))
    database_id = Column(String(64))
    action = Column(String(128), nullable=False)        # e.g. "proposal.approved"
    actor = Column(String(255))                         # User or "agent"
    details = Column(JSON)
    old_state = Column(String(64))
    new_state = Column(String(64))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    row_checksum = Column(String(64))                   # SHA256 of row content

    proposal = relationship("OptimizationProposal", back_populates="audit_entries")


@event.listens_for(AuditLog, "before_insert")
def compute_audit_checksum(mapper, connection, target):
    """Compute SHA256 checksum before insert for tamper detection."""
    content = json.dumps({
        "proposal_id": target.proposal_id,
        "database_id": target.database_id,
        "action": target.action,
        "actor": target.actor,
        "details": target.details,
        "created_at": str(target.created_at),
    }, sort_keys=True)
    target.row_checksum = hashlib.sha256(content.encode()).hexdigest()


# ─────────────────────────────────────────────
# Alerts
# ─────────────────────────────────────────────

class AlertRule(Base):
    __tablename__ = "alert_rules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    metric = Column(String(128), nullable=False)
    condition = Column(String(32), nullable=False)      # gt, lt, eq
    threshold = Column(Float, nullable=False)
    duration_seconds = Column(Integer, default=60)
    severity = Column(Enum(AlertSeverity), nullable=False)
    notification_channels = Column(JSON, default=list)  # ["slack", "email"]
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Alert(Base):
    __tablename__ = "alerts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    database_id = Column(String(64), ForeignKey("monitored_databases.id"))
    rule_id = Column(Integer, ForeignKey("alert_rules.id"))
    severity = Column(Enum(AlertSeverity), nullable=False)
    status = Column(Enum(AlertStatus), default=AlertStatus.ACTIVE)
    title = Column(String(500), nullable=False)
    description = Column(Text)
    metric_value = Column(Float)
    fired_at = Column(DateTime(timezone=True), server_default=func.now())
    resolved_at = Column(DateTime(timezone=True))
    acknowledged_by = Column(String(255))
    silenced_until = Column(DateTime(timezone=True))

    database = relationship("MonitoredDatabase", back_populates="alerts")

    __table_args__ = (
        Index("ix_alerts_status_fired", "status", "fired_at"),
    )


# ─────────────────────────────────────────────
# Backup Verification
# ─────────────────────────────────────────────

class BackupVerification(Base):
    __tablename__ = "backup_verifications"

    id = Column(Integer, primary_key=True, autoincrement=True)
    database_id = Column(String(64), nullable=False)
    backup_path = Column(String(1024))
    status = Column(String(32), nullable=False)         # running, passed, failed
    row_counts = Column(JSON)                           # {table: count}
    sample_checksums = Column(JSON)                     # {table: checksum}
    duration_seconds = Column(Float)
    error_message = Column(Text)
    started_at = Column(DateTime(timezone=True), server_default=func.now())
    completed_at = Column(DateTime(timezone=True))