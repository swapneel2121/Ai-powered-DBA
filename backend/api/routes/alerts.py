"""Alerts API routes — recent anomaly/security alerts raised by the agent."""
from typing import Optional

from fastapi import APIRouter, Query

router = APIRouter()


@router.get("/")
async def list_alerts(
    limit: int = Query(50, ge=1, le=200),
    severity: Optional[str] = Query(None, description="critical|high|medium|low|p1|p2|p3"),
):
    """Return recent alerts (anomalies + security events), newest first."""
    from backend.api.main import get_state

    alerts = get_state().notifier.recent_alerts(limit)
    if severity:
        alerts = [a for a in alerts if a.get("severity") == severity]
    return {"alerts": alerts, "total": len(alerts)}


@router.get("/security")
async def list_security_alerts(limit: int = Query(50, ge=1, le=200)):
    """Return only security-related alerts (SQL injection, privilege, exfiltration)."""
    from backend.api.main import get_state

    alerts = get_state().notifier.recent_alerts(200)
    security = [
        a
        for a in alerts
        if str(a.get("anomaly_type", "")).startswith(
            ("sql_injection", "privilege_escalation", "sensitive_table")
        )
    ]
    return {"alerts": security[:limit], "total": len(security[:limit])}
