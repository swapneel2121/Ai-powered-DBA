// ─────────────────────────────────────────────
// Shared types
// ─────────────────────────────────────────────

export interface MonitoredDatabase {
  id: string;
  name: string;
  db_type: string;
  host: string;
  port: number;
  database: string;
}

export interface HealthSnapshot {
  timestamp: string;
  database_id: string;
  active_connections: number;
  max_connections: number;
  p99_latency_ms: number;
  p95_latency_ms?: number;
  p50_latency_ms?: number;
  qps: number;
  cache_hit_ratio: number; // 0–100
  lock_waits: number;
  deadlocks?: number;
  replication_lag_seconds: number | null;
}

export interface SlowQuery {
  fingerprint: string;
  normalized_sql: string;
  sample_sql?: string;
  call_count: number;
  mean_time_ms: number;
  total_time_ms: number;
  access_pattern: string;
  last_seen_at?: string;
}

export interface Proposal {
  id: string;
  database_id: string;
  proposal_type: string;
  state: string;
  title: string;
  llm_rationale?: string;
  ddl_statements?: string[];
  estimated_impact_score?: number;
  replay_summary?: unknown;
  created_at: string;
  updated_at?: string;
}

export interface ForecastPoint {
  ds: string;
  yhat: number;
  yhat_lower: number;
  yhat_upper: number;
}

export interface ForecastResponse {
  database_id: string;
  metric: string;
  unit?: string;
  current_value?: number;
  forecast: ForecastPoint[];
  breach_date?: string;
  days_until_breach?: number;
  recommendation?: string;
  error?: string;
}

export interface ChatResponse {
  answer: string;
  sql_executed?: string | null;
  chart_data?: Record<string, unknown> | null;
  suggested_questions?: string[];
}

// ─────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────

const BASE = "/api/v1";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...init?.headers },
    ...init,
  });

  if (!res.ok) {
    let message = `API error ${res.status}`;
    try {
      const body = await res.json();
      message = body?.detail ?? body?.message ?? message;
    } catch {
      /* ignore parse errors */
    }
    throw new Error(message);
  }

  if (res.status === 204) return undefined as T;
  return res.json() as Promise<T>;
}

// ─────────────────────────────────────────────
// API surface — paths match backend/api/routes/*
// ─────────────────────────────────────────────

export const api = {
  // ── Databases ────────────────────────────
  getDatabases(): Promise<MonitoredDatabase[]> {
    return request(`/metrics/databases`);
  },

  // ── Health ───────────────────────────────
  getHealth(dbId: string): Promise<HealthSnapshot | Record<string, never>> {
    return request(`/metrics/health/${dbId}`);
  },

  getTimeseries(
    dbId: string,
    metric: string,
    hours = 24,
  ): Promise<Array<{ time: string; value: number }>> {
    return request(`/metrics/timeseries/${dbId}?metric=${metric}&hours=${hours}`);
  },

  // ── Slow Queries ─────────────────────────
  getSlowQueries(dbId: string, limit = 30): Promise<SlowQuery[]> {
    return request(`/queries/slow/${dbId}?limit=${limit}`);
  },

  analyzeQuery(dbId: string, sql: string): Promise<Record<string, unknown>> {
    return request(`/queries/analyze`, {
      method: "POST",
      body: JSON.stringify({ database_id: dbId, sql, run_explain: false }),
    });
  },

  // ── Optimization proposals ───────────────
  listProposals(dbId?: string): Promise<{ proposals: Proposal[]; total: number }> {
    const qs = dbId ? `?database_id=${dbId}` : "";
    return request(`/optimizations/${qs}`);
  },

  createProposal(
    dbId: string,
    sql: string,
    runReplay = false,
  ): Promise<Proposal> {
    return request(`/optimizations/`, {
      method: "POST",
      body: JSON.stringify({
        database_id: dbId,
        sql,
        run_replay: runReplay,
      }),
    });
  },

  reviewProposal(
    proposalId: string,
    approve: boolean,
    comment: string,
  ): Promise<Proposal> {
    return request(`/optimizations/${proposalId}/review`, {
      method: "POST",
      body: JSON.stringify({ approve, comment }),
    });
  },

  rollbackProposal(proposalId: string, reason: string): Promise<Proposal> {
    return request(
      `/optimizations/${proposalId}/rollback?reason=${encodeURIComponent(reason)}`,
      { method: "POST" },
    );
  },

  // ── Capacity / Forecast ──────────────────
  getForecast(
    dbId: string,
    metric: string,
    days = 28,
  ): Promise<ForecastResponse> {
    return request(
      `/capacity/forecast/${dbId}?metric=${metric}&lookahead_days=${days}`,
    );
  },

  // ── AI Chat ──────────────────────────────
  chat(
    dbId: string,
    messages: Array<{ role: string; content: string }>,
  ): Promise<ChatResponse> {
    return request(`/chat/`, {
      method: "POST",
      body: JSON.stringify({ database_id: dbId, messages }),
    });
  },
};
