# AI-Powered Autonomous Database Administrator

Autonomous DBA agent that monitors PostgreSQL & MySQL clusters, detects performance
bottlenecks, proposes LLM- and rule-based optimizations, validates them via workload
replay, forecasts capacity, and exposes everything through a FastAPI backend and a
React 18 + TypeScript dashboard.

## Architecture

```
backend/
  agent/      monitor, fingerprint, anomaly, explain_parser, optimizer, replay
  api/        FastAPI app (main.py), SSE stream, REST routes
  services/   timescale (metric store), approval (HITL state machine),
              forecasting (Prophet + linear fallback), backup, notifications
  models/     SQLAlchemy ORM + Pydantic schemas
  utils/      config (pydantic-settings), structured logging
frontend/     Vite + React 18 + TS dashboard (Recharts, react-query, Tailwind)
infra/docker/ docker-compose, Dockerfiles, nginx, prometheus, sample SQL
```

## Quick start

### 1. Configure
```bash
cp .env.example .env       # adjust MONITORED_DBS, TIMESCALE_URL, LLM, Slack, etc.
```

### 2. Backend
```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn backend.api.main:app --reload --port 8000
```
- API docs: http://localhost:8000/docs
- Health:   http://localhost:8000/health
- Metrics (Prometheus): http://localhost:8000/metrics

The backend boots in **degraded mode** if TimescaleDB / Redis / Ollama / Docker are
not reachable: it logs a warning, serves all endpoints, and retries backing services
on first use. Bring up the full stack with `infra/docker/docker-compose.yml` for live
data.

### 3. Frontend
```bash
cd frontend
npm install
npm run dev        # http://localhost:3000 (proxies /api -> :8000)
npm run build      # type-checks (tsc) + production build to dist/
```

### 4. Full stack (Docker)
```bash
cd infra/docker
docker compose up --build
```

## Graceful degradation

| Dependency        | Missing behaviour                                           |
|-------------------|-------------------------------------------------------------|
| TimescaleDB       | API starts; read endpoints return empty data, retry on use  |
| Ollama / Groq     | Optimizer falls back to rule-based heuristics                |
| Prophet           | Forecaster falls back to linear regression                   |
| Docker            | Backup verification & workload replay are disabled           |

## Notes
- Python 3.11+ recommended (3.10 also works).
- `MONITORED_DBS` is a comma-separated list of connection URLs.
- Approval workflow persistence is stubbed (in-memory state machine); wire a real
  SQLAlchemy session factory in `ApprovalService` for restart-safe proposals.
