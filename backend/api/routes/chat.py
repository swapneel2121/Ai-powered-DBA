"""Natural language chat interface routes."""
import re
from typing import Any, Dict, List, Optional

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    database_id: str
    messages: List[ChatMessage]


class ChatResponse(BaseModel):
    answer: str
    sql_executed: Optional[str] = None
    chart_data: Optional[Dict[str, Any]] = None
    suggested_questions: List[str] = []


SUGGESTED_QUESTIONS = [
    "What are the top 5 slowest queries?",
    "How many active connections are there?",
    "What is the cache hit ratio?",
    "Are there any lock waits or deadlocks?",
    "Give me a health summary",
    "What indexes are unused?",
]


def _fmt_pct(n: Any) -> str:
    try:
        return f"{float(n):.1f}%"
    except (TypeError, ValueError):
        return "n/a"


async def _answer_from_data(
    question: str, state: Any, database_id: str, snap: Dict[str, Any]
) -> Optional[str]:
    """
    Deterministic, data-backed answers for the most common DBA questions.

    Runs BEFORE the LLM so the chat is useful even with no model loaded. Returns
    a formatted answer string, or None if the question isn't a known intent
    (in which case the caller falls back to the LLM).
    """
    q = question.lower()

    # ── Slowest queries ──────────────────────────────────────────
    if ("slow" in q and ("quer" in q or "sql" in q)) or "slowest" in q:
        m = re.search(r"\b(\d+)\b", q)
        limit = int(m.group(1)) if m else 5
        limit = max(1, min(limit, 25))
        try:
            rows = await state.metric_store.get_slow_queries(database_id, limit)
        except Exception:  # noqa: BLE001
            rows = []
        if not rows:
            return (
                "No slow queries have been captured yet. The agent records them "
                "from pg_stat_statements once there is query activity above the "
                "configured threshold."
            )
        lines = [f"Top {len(rows)} slowest queries by average time:\n"]
        for i, r in enumerate(rows, 1):
            mean = r.get("mean_time_ms") or 0
            calls = r.get("call_count") or 0
            sql = (r.get("normalized_sql") or r.get("sample_sql") or "").strip()
            if len(sql) > 140:
                sql = sql[:140] + "…"
            lines.append(f"{i}. {float(mean):.1f} ms avg · {calls} calls\n   {sql}")
        return "\n".join(lines)

    # ── Connections ──────────────────────────────────────────────
    if "connection" in q:
        active = snap.get("active_connections")
        mx = snap.get("max_connections")
        if active is None:
            return "No connection data has been collected yet."
        pct = (active / mx * 100) if mx else 0
        return (
            f"There are currently {active} active connections out of a maximum of "
            f"{mx} ({pct:.0f}% utilised)."
        )

    # ── Cache hit ratio ──────────────────────────────────────────
    if "cache" in q:
        chr_ = snap.get("cache_hit_ratio")
        if chr_ is None:
            return "No cache statistics have been collected yet."
        note = "" if float(chr_) >= 99 else " — values below ~99% can indicate memory pressure."
        return f"The current cache hit ratio is {_fmt_pct(chr_)}{note}"

    # ── Locks / deadlocks ────────────────────────────────────────
    if "lock" in q or "deadlock" in q:
        locks = snap.get("lock_waits", 0) or 0
        deadlocks = snap.get("deadlocks", 0) or 0
        return (
            f"Right now there are {locks} lock wait(s) and {deadlocks} deadlock(s) "
            f"recorded. Sustained lock waits usually point to long-running "
            f"transactions or contention on a hot row."
        )

    # ── Alerts / security ────────────────────────────────────────
    if any(w in q for w in ("alert", "security", "injection", "anomal", "suspicious")):
        security_only = any(
            w in q for w in ("security", "injection", "suspicious")
        )
        alerts = state.notifier.recent_alerts(50)
        if security_only:
            alerts = [
                a
                for a in alerts
                if str(a.get("anomaly_type", "")).startswith(
                    ("sql_injection", "privilege_escalation", "sensitive_table")
                )
            ]
        if not alerts:
            kind = "security " if security_only else ""
            return f"No {kind}alerts have fired recently. The agent is monitoring continuously."
        lines = [f"{len(alerts)} recent alert(s):\n"]
        for a in alerts[:10]:
            lines.append(
                f"• [{str(a.get('severity', '')).upper()}] {a.get('title', '')}"
                f" — {a.get('anomaly_type', '')}"
            )
        return "\n".join(lines)

    # ── Health summary ───────────────────────────────────────────
    if any(w in q for w in ("health", "summary", "status", "how is", "overview")):
        if not snap:
            return "No health snapshot is available yet — the agent is still collecting metrics."
        return (
            "Database health snapshot:\n"
            f"• Active connections: {snap.get('active_connections', 'n/a')} / "
            f"{snap.get('max_connections', 'n/a')}\n"
            f"• Cache hit ratio: {_fmt_pct(snap.get('cache_hit_ratio'))}\n"
            f"• QPS: {snap.get('qps', 'n/a')}\n"
            f"• Lock waits: {snap.get('lock_waits', 0)} · Deadlocks: {snap.get('deadlocks', 0)}\n"
            f"• Replication lag: {snap.get('replication_lag_seconds', 0)} s"
        )

    return None


@router.post("/", response_model=ChatResponse)
async def chat(req: ChatRequest):
    from backend.api.main import get_state

    state = get_state()
    last_message = req.messages[-1].content if req.messages else ""

    # Get current DB stats for context (degrade gracefully if the store is down)
    try:
        snap = await state.metric_store.get_latest_snapshot(req.database_id) or {}
    except Exception:  # noqa: BLE001
        snap = {}

    # 1) Try a fast, deterministic answer straight from the monitoring data.
    direct = await _answer_from_data(last_message, state, req.database_id, snap)
    if direct is not None:
        return ChatResponse(
            answer=direct,
            suggested_questions=SUGGESTED_QUESTIONS,
        )

    # 2) Free-form question → translate to SQL against the MONITORED database
    #    and fetch real data. Requires an LLM (Ollama/Groq).
    collector = getattr(state.agent, "_collectors", {}).get(req.database_id)
    if collector is None or not hasattr(collector, "run_readonly_query"):
        return ChatResponse(answer=_no_db_message(), suggested_questions=SUGGESTED_QUESTIONS)

    try:
        schema = await collector.get_schema_summary()
    except Exception:  # noqa: BLE001
        schema = ""

    sql, explanation = await state.optimizer.english_to_sql(
        question=last_message, schema=schema, db_type="postgresql"
    )
    if not sql:
        return ChatResponse(answer=_llm_off_message(), suggested_questions=SUGGESTED_QUESTIONS)

    safe_sql = _safe_select(sql)
    if safe_sql is None:
        return ChatResponse(
            answer=(
                "I could only generate a query that isn't a safe read-only SELECT, "
                "so I won't run it. Try rephrasing your question."
            ),
            sql_executed=sql,
            suggested_questions=SUGGESTED_QUESTIONS,
        )

    try:
        rows = await collector.run_readonly_query(safe_sql, limit=100)
    except Exception as e:  # noqa: BLE001
        return ChatResponse(
            answer=f"I generated a query but it failed to run: {str(e)[:300]}",
            sql_executed=safe_sql,
            suggested_questions=SUGGESTED_QUESTIONS,
        )

    columns = list(rows[0].keys()) if rows else []
    answer = (f"{explanation}\n\n" if explanation else "") + (
        f"Returned {len(rows)} row(s)." if rows else "The query returned no rows."
    )
    return ChatResponse(
        answer=answer,
        sql_executed=safe_sql,
        chart_data={"rows": rows, "columns": columns},
        suggested_questions=SUGGESTED_QUESTIONS,
    )


# Statements that must never appear in a generated data query.
_FORBIDDEN_SQL = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|grant|revoke|copy|merge|"
    r"vacuum|reindex|call)\b",
    re.I,
)


def _safe_select(sql: str) -> Optional[str]:
    """
    Validate that `sql` is a single read-only SELECT and enforce a LIMIT.
    Returns the (possibly LIMIT-augmented) SQL, or None if it isn't safe.
    The read-only transaction at execution time is the real guard; this is
    defense-in-depth and gives a clean rejection before running anything.
    """
    s = sql.strip().rstrip(";").strip()
    if not s or ";" in s:  # reject empty or multiple statements
        return None
    low = s.lower()
    if not (low.startswith("select") or low.startswith("with")):
        return None
    if _FORBIDDEN_SQL.search(s):
        return None
    if not re.search(r"\blimit\s+\d+", low):
        s += " LIMIT 100"
    return s


def _no_db_message() -> str:
    return (
        "I can't reach the monitored database right now, so I can't run a data "
        "query. Once the agent is connected, ask things like \"show 10 orders with "
        "status pending\" or \"count orders per status\"."
    )


def _llm_off_message() -> str:
    return (
        "Free-form data queries need a language model, which isn't reachable right "
        "now. Enable one locally:\n"
        "docker exec -it dba-ollama ollama pull llama3.2:1b\n"
        "then set OLLAMA_MODEL=llama3.2:1b in the backend service and restart it.\n\n"
        "Meanwhile I can still answer health questions with no model — try "
        "\"top 5 slowest queries\" or \"give me a health summary\"."
    )
