"""
Capacity planning and forecasting service using Prophet.

Produces 4-week lookahead forecasts for:
  - Storage growth
  - Query volume (QPS)
  - Connection pool utilization
  - p99 latency trends

Detects when a metric will breach a threshold and recommends actions.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import pandas as pd

from backend.utils.config import settings
from backend.utils.logging import get_logger

log = get_logger(__name__)


METRIC_THRESHOLDS = {
    "active_connections": {
        "threshold_pct": 0.85,    # 85% of max_connections
        "unit": "connections",
        "recommendation": "Increase max_connections or add PgBouncer connection pooling",
    },
    "disk_usage_bytes": {
        "threshold_pct": 0.80,    # 80% of available disk
        "unit": "bytes",
        "recommendation": "Provision additional storage or archive cold data to S3",
    },
    "p99_latency_ms": {
        "absolute_threshold": 2000,   # 2 seconds
        "unit": "ms",
        "recommendation": "Review top slow queries and add missing indexes",
    },
    "qps": {
        "growth_rate_threshold": 2.0,  # 2x growth in 4 weeks
        "unit": "queries/sec",
        "recommendation": "Consider read replicas or query caching layer",
    },
}


class CapacityForecaster:
    """
    Uses Facebook Prophet for time-series forecasting of DB metrics.
    Falls back to simple linear regression if Prophet is unavailable.
    """

    def __init__(self, metric_store):
        self._metric_store = metric_store
        self._prophet_available = self._check_prophet()

    def _check_prophet(self) -> bool:
        try:
            from prophet import Prophet  # noqa: F401
            return True
        except ImportError:
            log.warning("prophet_not_installed_using_linear_fallback")
            return False

    async def forecast_metric(
        self,
        database_id: str,
        metric: str,
        lookahead_days: int = 28,
        historical_days: int = 90,
    ) -> Dict:
        """
        Forecast a single metric for `lookahead_days` into the future.
        Returns a structured forecast result with breach detection.
        """
        # Load historical data
        raw = await self._metric_store.get_raw_metrics_for_forecast(
            database_id, metric, historical_days
        )

        if len(raw) < 3:  # Need a few points to fit a trend line
            return {
                "database_id": database_id,
                "metric": metric,
                "unit": METRIC_THRESHOLDS.get(metric, {}).get("unit", ""),
                "current_value": float(raw[-1]["y"]) if raw else 0.0,
                "error": "Collecting data — a forecast will appear within a few minutes.",
                "forecast": [],
            }

        df = pd.DataFrame(raw).rename(columns={"ds": "ds", "y": "y"})
        df["ds"] = pd.to_datetime(df["ds"])
        df = df.dropna(subset=["y"])

        if self._prophet_available:
            forecast_df = self._prophet_forecast(df, lookahead_days)
        else:
            forecast_df = self._linear_forecast(df, lookahead_days)

        # Current value
        current_value = float(df["y"].iloc[-1]) if len(df) else 0.0

        # Breach detection
        breach_date, days_until_breach = self._detect_breach(
            metric, forecast_df, current_value
        )

        # Recommendation
        threshold_info = METRIC_THRESHOLDS.get(metric, {})
        recommendation = None
        if breach_date:
            recommendation = threshold_info.get(
                "recommendation",
                f"Take action before {breach_date.strftime('%Y-%m-%d')} to avoid {metric} breach",
            )

        result = {
            "database_id": database_id,
            "metric": metric,
            "unit": threshold_info.get("unit", ""),
            "current_value": current_value,
            "forecast": forecast_df[
                ["ds", "yhat", "yhat_lower", "yhat_upper"]
            ].to_dict(orient="records"),
            "breach_date": breach_date.isoformat() if breach_date else None,
            "days_until_breach": days_until_breach,
            "recommendation": recommendation,
        }

        if breach_date and days_until_breach <= settings.capacity_warning_days:
            log.warning(
                "capacity_breach_approaching",
                db_id=database_id,
                metric=metric,
                days=days_until_breach,
            )

        return result

    async def forecast_all(self, database_id: str) -> List[Dict]:
        """Run forecasts for all key metrics."""
        metrics = [
            "active_connections",
            "p99_latency_ms",
            "qps",
            "cache_hit_ratio",
        ]
        results = []
        for metric in metrics:
            try:
                r = await self.forecast_metric(database_id, metric)
                results.append(r)
            except Exception as e:
                log.error("forecast_failed", metric=metric, error=str(e))
        return results

    # ── Forecasting methods ───────────────────

    def _prophet_forecast(self, df: pd.DataFrame, lookahead_days: int) -> pd.DataFrame:
        from prophet import Prophet

        m = Prophet(
            interval_width=0.95,
            daily_seasonality=True,
            weekly_seasonality=True,
            changepoint_prior_scale=0.05,
        )
        m.fit(df)

        future = m.make_future_dataframe(
            periods=lookahead_days * 24,  # hourly
            freq="H",
        )
        forecast = m.predict(future)

        # Return only the future portion
        last_known = df["ds"].max()
        return forecast[forecast["ds"] > last_known][
            ["ds", "yhat", "yhat_lower", "yhat_upper"]
        ]

    def _linear_forecast(self, df: pd.DataFrame, lookahead_days: int) -> pd.DataFrame:
        """Simple linear regression fallback when Prophet is unavailable."""
        import numpy as np

        df = df.copy()
        df["t"] = (df["ds"] - df["ds"].min()).dt.total_seconds()

        x = df["t"].values
        y = df["y"].values

        # Least squares
        A = np.vstack([x, np.ones(len(x))]).T
        slope, intercept = np.linalg.lstsq(A, y, rcond=None)[0]

        last_t = df["t"].max()
        last_ds = df["ds"].max()

        future_times = [
            (last_ds + timedelta(hours=i), last_t + i * 3600)
            for i in range(1, lookahead_days * 24 + 1)
        ]

        rows = []
        for ds, t in future_times:
            yhat = slope * t + intercept
            std = float(df["y"].std()) * 1.5  # Simple uncertainty band
            rows.append({
                "ds": ds,
                "yhat": max(0, yhat),
                "yhat_lower": max(0, yhat - std),
                "yhat_upper": yhat + std,
            })

        return pd.DataFrame(rows)

    # ── Breach detection ──────────────────────

    def _detect_breach(
        self,
        metric: str,
        forecast_df: pd.DataFrame,
        current_value: float,
    ) -> tuple[Optional[datetime], Optional[int]]:

        threshold_info = METRIC_THRESHOLDS.get(metric, {})

        if not threshold_info:
            return None, None

        # Absolute threshold (e.g., p99 > 2000ms)
        absolute = threshold_info.get("absolute_threshold")
        if absolute:
            breach_rows = forecast_df[forecast_df["yhat"] >= absolute]
            if not breach_rows.empty:
                breach_dt = pd.Timestamp(breach_rows.iloc[0]["ds"]).to_pydatetime()
                days = (breach_dt - datetime.now(timezone.utc)).days
                return breach_dt, max(0, days)

        # Percentage threshold relative to current
        growth_threshold = threshold_info.get("growth_rate_threshold")
        if growth_threshold and current_value > 0:
            target = current_value * growth_threshold
            breach_rows = forecast_df[forecast_df["yhat"] >= target]
            if not breach_rows.empty:
                breach_dt = pd.Timestamp(breach_rows.iloc[0]["ds"]).to_pydatetime()
                days = (breach_dt - datetime.now(timezone.utc)).days
                return breach_dt, max(0, days)

        return None, None