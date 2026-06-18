"""Backup verification API routes."""
from fastapi import APIRouter, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, List

router = APIRouter()


class BackupVerifyRequest(BaseModel):
    database_id: str
    source_url: str
    db_type: str = "postgresql"
    tables_to_check: Optional[List[str]] = None


@router.post("/verify")
async def trigger_backup_verification(
    req: BackupVerifyRequest,
    background_tasks: BackgroundTasks,
):
    """
    Trigger an async backup verification cycle.
    Returns immediately; result is stored in the DB and sent via Slack.
    """
    from backend.api.main import get_state

    async def _run():
        result = await get_state().backup_service.run_verification(
            database_id=req.database_id,
            source_url=req.source_url,
            db_type=req.db_type,
            tables_to_check=req.tables_to_check,
        )
        # Notify on failure
        if result["status"] == "failed":
            from backend.agent.anomaly import AnomalyEvent
            from datetime import datetime, timezone
            event = AnomalyEvent(
                database_id=req.database_id,
                severity="p1",
                anomaly_type="backup_verification_failed",
                title="Backup verification FAILED",
                description=result.get("error_message", "Unknown error"),
                detected_at=datetime.now(timezone.utc),
            )
            await get_state().notifier.send_alert(event)

    background_tasks.add_task(_run)
    return {"status": "scheduled", "database_id": req.database_id}


@router.get("/history/{database_id}")
async def get_verification_history(database_id: str, limit: int = 10):
    """Fetch recent backup verification results."""
    # Query backup_verifications table
    return {"database_id": database_id, "verifications": [], "limit": limit}