"""
FastAPI application entry point.

Mounts all routers, sets up lifespan (agent start/stop),
configures CORS, Prometheus middleware, and SSE endpoint.
"""
from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import structlog
from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from prometheus_client import make_asgi_app

from backend.agent.anomaly import AnomalyDetector
from backend.agent.fingerprint import QueryFingerprinter
from backend.agent.monitor import MonitoringAgent
from backend.agent.optimizer import LLMService, SQLOptimizer
from backend.api.routes import backups, capacity, chat, metrics, optimizations, queries
from backend.services.approval import ApprovalService
from backend.services.backup import BackupVerificationService
from backend.services.forecasting import CapacityForecaster
from backend.services.notifications import NotificationService
from backend.services.timescale import MetricStore
from backend.utils.config import settings
from backend.utils.logging import configure_logging, get_logger

configure_logging()
log = get_logger(__name__)

# ─────────────────────────────────────────────
# Application state container
# ─────────────────────────────────────────────

class AppState:
    metric_store: MetricStore
    agent: MonitoringAgent
    llm_service: LLMService
    optimizer: SQLOptimizer
    approval_service: ApprovalService
    forecaster: CapacityForecaster
    backup_service: BackupVerificationService
    notifier: NotificationService
    sse_subscribers: dict  # database_id → list of queues


state = AppState()
state.sse_subscribers = {}


# ─────────────────────────────────────────────
# Lifespan
# ─────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("dba_agent_starting", environment=settings.environment)

    # Initialize services
    state.metric_store = MetricStore()
    try:
        await state.metric_store.connect()
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "timescaledb_unavailable_at_startup",
            error=str(exc)[:200],
            hint="API will start in degraded mode and retry on first use.",
        )

    state.notifier = NotificationService()
    state.llm_service = LLMService()
    state.optimizer = SQLOptimizer(state.llm_service)
    state.forecaster = CapacityForecaster(state.metric_store)
    state.backup_service = BackupVerificationService(state.metric_store)

    fingerprinter = QueryFingerprinter()
    anomaly_detector = AnomalyDetector()

    def publish_snapshot(db_id: str, snapshot: dict) -> None:
        """Fan a fresh health snapshot out to all SSE subscribers for this DB."""
        for queue in list(state.sse_subscribers.get(db_id, [])):
            try:
                queue.put_nowait(snapshot)
            except asyncio.QueueFull:
                # Drop the oldest item to make room, then enqueue the newest.
                try:
                    queue.get_nowait()
                    queue.put_nowait(snapshot)
                except Exception:  # noqa: BLE001
                    pass

    state.agent = MonitoringAgent(
        metric_store=state.metric_store,
        fingerprinter=fingerprinter,
        anomaly_detector=anomaly_detector,
        notifier=state.notifier,
        on_snapshot=publish_snapshot,
    )

    state.approval_service = ApprovalService(
        db_session_factory=None,  # Inject real factory in production
        notifier=state.notifier,
    )

    # Start monitoring agent as background task
    agent_task = asyncio.create_task(state.agent.start())
    log.info("monitoring_agent_started")

    yield  # Application is running

    # Shutdown
    log.info("dba_agent_shutting_down")
    for name, coro in (
        ("agent.stop", state.agent.stop()),
        ("metric_store.disconnect", state.metric_store.disconnect()),
        ("notifier.close", state.notifier.close()),
        ("llm_service.close", state.llm_service.close()),
    ):
        try:
            await coro
        except Exception as exc:  # noqa: BLE001
            log.warning("shutdown_step_failed", step=name, error=str(exc)[:200])
    agent_task.cancel()
    log.info("dba_agent_stopped")


# ─────────────────────────────────────────────
# App factory
# ─────────────────────────────────────────────

def create_app() -> FastAPI:
    app = FastAPI(
        title="Autonomous DBA Agent API",
        version="1.0.0",
        description="AI-powered autonomous PostgreSQL & MySQL administrator",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:3000", "http://localhost:5173"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Prometheus metrics endpoint
    metrics_app = make_asgi_app()
    app.mount("/metrics", metrics_app)

    # Routers
    app.include_router(metrics.router, prefix="/api/v1/metrics", tags=["Metrics"])
    app.include_router(queries.router, prefix="/api/v1/queries", tags=["Queries"])
    app.include_router(optimizations.router, prefix="/api/v1/optimizations", tags=["Optimizations"])
    app.include_router(capacity.router, prefix="/api/v1/capacity", tags=["Capacity"])
    app.include_router(chat.router, prefix="/api/v1/chat", tags=["Chat"])
    app.include_router(backups.router, prefix="/api/v1/backups", tags=["Backups"])

    # SSE endpoint for real-time dashboard streaming
    @app.get("/api/v1/stream/{database_id}", tags=["SSE"])
    async def stream_metrics(database_id: str, request: Request):
        return StreamingResponse(
            _sse_generator(database_id, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    @app.get("/health")
    async def health():
        return {"status": "ok", "agent": "running"}

    return app


async def _sse_generator(
    database_id: str, request: Request
) -> AsyncGenerator[str, None]:
    """
    Server-Sent Events generator.

    Pushes real-time metric snapshots to the React dashboard
    every CRITICAL_POLL_INTERVAL seconds.
    """
    queue: asyncio.Queue = asyncio.Queue(maxsize=100)

    # Register subscriber
    if database_id not in state.sse_subscribers:
        state.sse_subscribers[database_id] = []
    state.sse_subscribers[database_id].append(queue)

    log.info("sse_client_connected", db_id=database_id)

    try:
        while True:
            if await request.is_disconnected():
                break

            # Try to get a metric update
            try:
                snapshot = await asyncio.wait_for(queue.get(), timeout=10.0)
                data = json.dumps(snapshot, default=str)
                yield f"data: {data}\n\n"
            except asyncio.TimeoutError:
                # Send keepalive comment
                yield ": keepalive\n\n"

    finally:
        # Deregister
        subs = state.sse_subscribers.get(database_id, [])
        if queue in subs:
            subs.remove(queue)
        log.info("sse_client_disconnected", db_id=database_id)


def get_state() -> AppState:
    return state


app = create_app()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "backend.api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.environment == "development",
        log_config=None,  # Use our structlog config
    )