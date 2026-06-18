"""Capacity planning routes."""
from fastapi import APIRouter, Query

router = APIRouter()

@router.get("/forecast/{database_id}")
async def get_capacity_forecast(
    database_id: str,
    metric: str = Query("active_connections"),
    lookahead_days: int = Query(28, ge=7, le=90),
):
    from backend.api.main import get_state
    try:
        return await get_state().forecaster.forecast_metric(database_id, metric, lookahead_days)
    except Exception as e:  # noqa: BLE001 - insufficient data / store unavailable
        return {"database_id": database_id, "metric": metric, "forecast": [], "error": str(e)[:200]}

@router.get("/forecast/{database_id}/all")
async def get_all_forecasts(database_id: str):
    from backend.api.main import get_state
    try:
        return await get_state().forecaster.forecast_all(database_id)
    except Exception as e:  # noqa: BLE001
        return {"database_id": database_id, "forecasts": [], "error": str(e)[:200]}