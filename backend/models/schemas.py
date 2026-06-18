"""Pydantic schemas for API request/response validation."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


# ─────────────────────────────────────────────
# Database
# ─────────────────────────────────────────────

class DatabaseInfo(BaseModel):
    id: str
    name: str
    db_type: str
    host: str
    port: int
    database_name: str
    is_active: bool
    last_seen_at: Optional[datetime] = None


# ─────────────────────────────────────────────
# Metrics
# ─────────────────────────────────────────────

class MetricPoint(BaseModel):
    time: datetime
    value: float


class DatabaseHealthSnapshot(BaseModel):
    database_id: str
    timestamp: datetime
    qps: float = 0.0
    active_connections: int = 0
    max_connections: int = 0
    cache_hit_ratio: float = 0.0
    p50_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    replication_lag_seconds: float = 0.0
    disk_read_iops: float = 0.0
    disk_write_iops: float = 0.0
    cpu_pct: float = 0.0
    memory_pct: float = 0.0
    slow_query_count: int = 0
    lock_waits: int = 0
    deadlocks: int = 0
    table_bloat_bytes: int = 0


# ─────────────────────────────────────────────
# Queries
# ─────────────────────────────────────────────

class SlowQuery(BaseModel):
    fingerprint: str
    normalized_sql: str
    sample_sql: Optional[str] = None
    call_count: int
    mean_time_ms: float
    p99_time_ms: float
    total_time_ms: float
    rows_examined: int
    access_pattern: str
    captured_at: datetime


class ExplainNode(BaseModel):
    node_type: str
    relation_name: Optional[str] = None
    alias: Optional[str] = None
    startup_cost: float = 0.0
    total_cost: float = 0.0
    plan_rows: int = 0
    actual_rows: int = 0
    actual_time_ms: float = 0.0
    loops: int = 1
    shared_hit_blocks: int = 0
    shared_read_blocks: int = 0
    children: List["ExplainNode"] = []


# ─────────────────────────────────────────────
# Optimization Proposals
# ─────────────────────────────────────────────

class ProposalCreate(BaseModel):
    database_id: str
    title: str
    proposal_type: str
    original_sql: Optional[str] = None
    optimized_sql: Optional[str] = None
    ddl_statements: Optional[List[str]] = None
    llm_rationale: Optional[str] = None
    estimated_improvement_pct: Optional[float] = None


class ProposalReview(BaseModel):
    comment: str = Field(..., min_length=10, description="Mandatory review comment")
    approve: bool


class ProposalResponse(BaseModel):
    id: str
    database_id: str
    title: str
    proposal_type: str
    state: str
    original_sql: Optional[str] = None
    optimized_sql: Optional[str] = None
    ddl_statements: Optional[List[str]] = None
    llm_rationale: Optional[str] = None
    estimated_improvement_pct: Optional[float] = None
    estimated_impact_score: Optional[float] = None
    replay_summary: Optional[Dict[str, Any]] = None
    replay_before_p99_ms: Optional[float] = None
    replay_after_p99_ms: Optional[float] = None
    created_at: datetime
    approved_by: Optional[str] = None
    approved_at: Optional[datetime] = None


# ─────────────────────────────────────────────
# Capacity Planning
# ─────────────────────────────────────────────

class ForecastPoint(BaseModel):
    ds: datetime
    yhat: float
    yhat_lower: float
    yhat_upper: float


class CapacityForecast(BaseModel):
    database_id: str
    metric: str
    unit: str
    current_value: float
    forecast: List[ForecastPoint]
    breach_date: Optional[datetime] = None
    days_until_breach: Optional[int] = None
    recommendation: Optional[str] = None


# ─────────────────────────────────────────────
# Alerts
# ─────────────────────────────────────────────

class AlertRuleCreate(BaseModel):
    name: str
    metric: str
    condition: str = Field(..., pattern="^(gt|lt|eq)$")
    threshold: float
    duration_seconds: int = 60
    severity: str
    notification_channels: List[str] = ["slack"]


class AlertResponse(BaseModel):
    id: int
    database_id: str
    severity: str
    status: str
    title: str
    description: Optional[str] = None
    metric_value: Optional[float] = None
    fired_at: datetime
    resolved_at: Optional[datetime] = None


# ─────────────────────────────────────────────
# Chat / NL Interface
# ─────────────────────────────────────────────

class ChatMessage(BaseModel):
    role: str   # user | assistant
    content: str


class ChatRequest(BaseModel):
    database_id: str
    messages: List[ChatMessage]
    include_charts: bool = True


class ChatResponse(BaseModel):
    answer: str
    sql_executed: Optional[str] = None
    chart_data: Optional[Dict[str, Any]] = None
    suggested_questions: List[str] = []


# ─────────────────────────────────────────────
# Backup Verification
# ─────────────────────────────────────────────

class BackupVerificationResult(BaseModel):
    id: int
    database_id: str
    status: str
    row_counts: Optional[Dict[str, int]] = None
    duration_seconds: Optional[float] = None
    error_message: Optional[str] = None
    started_at: datetime
    completed_at: Optional[datetime] = None