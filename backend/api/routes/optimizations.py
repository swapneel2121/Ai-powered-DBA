"""Optimization proposal lifecycle routes."""
from typing import Optional

from fastapi import APIRouter, Header
from pydantic import BaseModel, Field

router = APIRouter()


class CreateProposalRequest(BaseModel):
    database_id: str
    sql: str
    title: Optional[str] = None
    run_replay: bool = True


class ReviewRequest(BaseModel):
    comment: str = Field(..., min_length=10)
    approve: bool


@router.post("/")
async def create_proposal(req: CreateProposalRequest):
    """
    Trigger LLM analysis on a SQL query and create an optimization proposal.
    Optionally runs workload replay for statistical validation.
    """
    import asyncio

    from backend.agent.explain_parser import ExplainParser
    from backend.agent.replay import WorkloadReplayEngine
    from backend.api.main import get_state

    state = get_state()

    # Get LLM analysis
    analysis = await state.optimizer.analyze_query(
        sql=req.sql,
        db_type="postgresql",
    )

    ddl = [r["ddl"] for r in analysis.get("index_recommendations", [])]
    optimized = analysis.get("rewritten_query")
    title = req.title or f"Optimization for query (impact {analysis.get('overall_impact_score', 0):.0f}/100)"

    # Create proposal
    proposal = await state.approval_service.create_proposal(
        database_id=req.database_id,
        title=title,
        proposal_type="index" if ddl else "query_rewrite",
        original_sql=req.sql,
        optimized_sql=optimized,
        ddl_statements=ddl,
        llm_rationale=analysis.get("rewrite_explanation"),
        estimated_improvement_pct=analysis.get("index_recommendations", [{}])[0].get(
            "estimated_improvement_pct"
        ) if analysis.get("index_recommendations") else None,
        estimated_impact_score=analysis.get("overall_impact_score"),
    )

    # Optionally kick off replay in the background
    if req.run_replay and ddl:
        async def _run_replay():
            engine = WorkloadReplayEngine()
            slow_queries = await state.metric_store.get_slow_queries(req.database_id, 50)
            replay_result = await engine.run_replay(
                proposal_id=proposal["id"],
                database_id=req.database_id,
                source_conn_url=_get_source_url(req.database_id),
                ddl_statements=ddl,
                workload_queries=slow_queries,
            )
            # Update proposal with replay results
            await state.approval_service.start_testing(proposal["id"])

        asyncio.create_task(_run_replay())

    return proposal


@router.get("/")
async def list_proposals(
    database_id: Optional[str] = None,
    state_filter: Optional[str] = None,
    limit: int = 50,
):
    """List optimization proposals, optionally filtered by database/state."""
    from backend.api.main import get_state

    proposals = get_state().approval_service.list_proposals(database_id, limit)
    if state_filter:
        proposals = [p for p in proposals if p.get("state") == state_filter]
    return {"proposals": proposals, "total": len(proposals)}


@router.get("/{proposal_id}")
async def get_proposal(proposal_id: str):
    from fastapi import HTTPException

    from backend.api.main import get_state

    proposal = get_state().approval_service.get_proposal(proposal_id)
    if proposal is None:
        raise HTTPException(status_code=404, detail="Proposal not found")
    return proposal


@router.post("/{proposal_id}/review")
async def review_proposal(
    proposal_id: str,
    req: ReviewRequest,
    x_user: str = Header(default="anonymous"),
):
    from backend.api.main import get_state
    if req.approve:
        return await get_state().approval_service.approve_proposal(
            proposal_id, x_user, req.comment
        )
    else:
        return await get_state().approval_service.reject_proposal(
            proposal_id, x_user, req.comment
        )


@router.post("/{proposal_id}/rollback")
async def rollback_proposal(
    proposal_id: str,
    reason: str = "Manual rollback requested",
    x_user: str = Header(default="anonymous"),
):
    from backend.api.main import get_state
    return await get_state().approval_service.rollback(proposal_id, reason)


def _get_source_url(database_id: str) -> str:
    """Look up the connection URL for a database_id from config."""
    from backend.agent.monitor import DatabaseTarget
    from backend.utils.config import settings
    for url in settings.monitored_dbs:
        t = DatabaseTarget.from_url(url)
        if t.id == database_id:
            return url
    return ""