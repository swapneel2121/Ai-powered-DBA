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

    # 2) Otherwise defer to the LLM (which itself degrades gracefully if offline).
    answer, sql = await state.optimizer.answer_nl_question(
        question=last_message,
        database_id=req.database_id,
        db_stats=snap,
    )

    chart_data = None
    if sql:
        try:
            rows = await state.metric_store.execute_monitoring_query(sql)
            chart_data = {"rows": rows[:500]}
        except Exception as e:  # noqa: BLE001
            answer += f"\n\n(Note: Could not execute generated SQL: {e})"

    return ChatResponse(
        answer=answer,
        sql_executed=sql,
        chart_data=chart_data,
        suggested_questions=SUGGESTED_QUESTIONS,
    )
