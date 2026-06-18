"""All API route modules in one file for brevity."""
# backend/api/routes/metrics.py
from fastapi import APIRouter, Query

router = APIRouter()

@router.get("/databases")
async def list_databases():
    """
    List the databases the agent is configured to monitor, with the stable
    hashed id the rest of the API uses. The frontend calls this on load to
    discover which database_id to query (instead of hard-coding one).
    """
    from backend.agent.monitor import DatabaseTarget
    from backend.utils.config import settings

    out = []
    for url in settings.monitored_dbs:
        try:
            t = DatabaseTarget.from_url(url)
            out.append({
                "id": t.id,
                "name": t.database or t.host,
                "db_type": t.db_type,
                "host": t.host,
                "port": t.port,
                "database": t.database,
            })
        except Exception:  # noqa: BLE001 - skip malformed URLs
            continue
    return out

@router.get("/health/{database_id}")
async def get_health_snapshot(database_id: str):
    from backend.api.main import get_state
    try:
        snap = await get_state().metric_store.get_latest_snapshot(database_id)
    except Exception:  # noqa: BLE001 - store unavailable -> empty snapshot
        snap = None
    return snap or {}

@router.get("/timeseries/{database_id}")
async def get_timeseries(
    database_id: str,
    metric: str = Query(..., description="e.g. active_connections, p99_latency_ms"),
    hours: int = Query(24, ge=1, le=720),
    bucket: str = Query("5 minutes"),
):
    from backend.api.main import get_state
    try:
        return await get_state().metric_store.get_health_timeseries(
            database_id, metric, hours, bucket
        )
    except Exception:  # noqa: BLE001 - store unavailable -> empty series
        return []

@router.get("/overview")
async def get_all_databases_overview():
    """Summary health for all monitored databases."""
    from backend.agent.monitor import DatabaseTarget
    from backend.api.main import get_state
    from backend.utils.config import settings

    results = []
    for url in settings.monitored_dbs:
        try:
            target = DatabaseTarget.from_url(url)
            snap = await get_state().metric_store.get_latest_snapshot(target.id)
            results.append({"database_id": target.id, "host": target.host, **( snap or {})})
        except Exception:
            pass
    return results