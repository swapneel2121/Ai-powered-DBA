"""Query analysis API routes."""
from fastapi import APIRouter
from fastapi import Query as QParam
from pydantic import BaseModel

router = APIRouter()


class AnalyzeRequest(BaseModel):
    database_id: str
    sql: str
    run_explain: bool = True


@router.get("/slow/{database_id}")
async def get_slow_queries(
    database_id: str,
    limit: int = QParam(20, ge=1, le=100),
):
    """Top N slowest queries for a database."""
    from backend.api.main import get_state
    try:
        return await get_state().metric_store.get_slow_queries(database_id, limit)
    except Exception:  # noqa: BLE001 - store unavailable -> empty list
        return []


@router.post("/analyze")
async def analyze_query(req: AnalyzeRequest):
    """
    Run LLM analysis on a single SQL query.
    Optionally runs EXPLAIN ANALYZE on the source DB first.
    """
    from backend.agent.explain_parser import ExplainParser
    from backend.api.main import get_state

    state = get_state()
    explain_result = None

    # Try to get EXPLAIN from the monitored DB
    if req.run_explain:
        collector = state.agent._collectors.get(req.database_id)
        if collector and callable(getattr(collector, "get_explain", None)):
            try:
                raw_explain = await collector.get_explain(req.sql)
                explain_result = ExplainParser().parse(raw_explain)
            except Exception:
                # Proceed without explain on any failure
                explain_result = None

    result = await state.optimizer.analyze_query(
        sql=req.sql,
        db_type="postgresql",
        explain_result=explain_result,
    )
    return result


@router.get("/fingerprint")
async def get_query_fingerprint(sql: str):
    """Return the normalized form and fingerprint of a SQL query."""
    from backend.agent.fingerprint import QueryFingerprinter
    fp = QueryFingerprinter()
    return {
        "fingerprint": fp.fingerprint(sql),
        "normalized": fp.normalize(sql),
        "access_pattern": fp.classify_pattern(fp.normalize(sql)),
    }