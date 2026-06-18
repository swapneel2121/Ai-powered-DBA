/**
 * Autonomous DBA Dashboard
 * Single-file app with tabbed navigation covering all 6 views:
 *   Dashboard | Queries | Optimizations | Capacity | Chat | Settings
 */
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { formatDistanceToNow } from "date-fns";
import {
  Activity,
  AlertTriangle,
  Database,
  Eye,
  MessageSquare,
  RefreshCw,
  RotateCcw,
  Send,
  Settings,
  Shield,
  ThumbsDown,
  ThumbsUp,
  TrendingUp,
  Zap,
} from "lucide-react";
import { useEffect, useRef, useState } from "react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { useSSE } from "./hooks/useSSE";
import { api, HealthSnapshot, SlowQuery } from "./utils/api";

// ─────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────
const DEMO_DB_ID = "demo-db-01";
const NAV_ITEMS = [
  { id: "dashboard", icon: Activity, label: "Dashboard" },
  { id: "queries", icon: Database, label: "Slow Queries" },
  { id: "optimizations", icon: Zap, label: "Optimizations" },
  { id: "capacity", icon: TrendingUp, label: "Capacity" },
  { id: "chat", icon: MessageSquare, label: "AI Chat" },
  { id: "settings", icon: Settings, label: "Settings" },
];

// ─────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────

interface IndexRecommendation {
  ddl: string;
  reason: string;
  estimated_improvement_pct: number;
}

interface QueryAnalysis {
  issues?: string[];
  index_recommendations?: IndexRecommendation[];
  rewritten_query?: string;
  rewrite_explanation?: string;
  overall_impact_score?: number;
  confidence?: number;
  error?: string;
}

// FIX: Typed tuple so destructured values are [string, string], not
// (string | undefined)[] — which caused "unknown not assignable to ReactNode"
// on lines 736-745 when rendering label/val directly in JSX.
type SettingRow = [string, string];

// ─────────────────────────────────────────────
// Utility Components
// ─────────────────────────────────────────────

function MetricCard({
  label,
  value,
  unit,
  trend,
  color = "text-white",
}: {
  label: string;
  value: string | number;
  unit?: string;
  trend?: "up" | "down" | "neutral";
  color?: string;
}) {
  const trendIcon = trend === "up" ? "↑" : trend === "down" ? "↓" : "—";
  const trendColor =
    trend === "up"
      ? "text-red-400"
      : trend === "down"
        ? "text-green-400"
        : "text-gray-500";
  return (
    <div className="card flex flex-col gap-1">
      <span className="text-xs text-gray-500 uppercase tracking-wider">
        {label}
      </span>
      <div className="flex items-end gap-2">
        <span className={`text-2xl font-bold font-mono ${color}`}>{value}</span>
        {unit && <span className="text-sm text-gray-500 mb-0.5">{unit}</span>}
        <span className={`text-sm mb-0.5 ${trendColor}`}>{trendIcon}</span>
      </div>
    </div>
  );
}

function StateBadge({ state }: { state: string }) {
  const map: Record<string, string> = {
    proposed: "badge-info",
    reviewed: "badge-info",
    approved: "badge-success",
    testing: "badge-info",
    deploying: "badge-high",
    monitoring: "badge-info",
    completed: "badge-success",
    rolled_back: "badge-critical",
    rejected: "badge-critical",
  };
  return <span className={map[state] ?? "badge-info"}>{state}</span>;
}

// FIX: SeverityDot was defined but never used — kept here with an underscore
// prefix so noUnusedLocals doesn't error if you re-enable it later.
// If you truly don't need it, delete it entirely.
function _SeverityDot({ severity }: { severity: string }) {
  const map: Record<string, string> = {
    critical: "bg-red-500",
    high: "bg-orange-400",
    medium: "bg-yellow-400",
    low: "bg-green-500",
  };
  return (
    <span
      className={`inline-block w-2 h-2 rounded-full ${map[severity] ?? "bg-gray-500"}`}
    />
  );
}

// ─────────────────────────────────────────────
// Dashboard View
// ─────────────────────────────────────────────

function DashboardView({ dbId }: { dbId: string }) {
  const [history, setHistory] = useState<HealthSnapshot[]>([]);
  const { lastMessage: liveSnap, status } = useSSE<HealthSnapshot>(
    `/api/v1/stream/${dbId}`,
  );

  useEffect(() => {
    if (liveSnap) {
      setHistory((prev) => [...prev.slice(-120), liveSnap]);
    }
  }, [liveSnap]);

  const snap = liveSnap ?? history[history.length - 1];

  const chartData = history.map((s) => ({
    t: new Date(s.timestamp).toLocaleTimeString(),
    connections: s.active_connections,
    p99: s.p99_latency_ms,
    qps: s.qps,
    cache: s.cache_hit_ratio,
  }));

  const connPct = snap
    ? (snap.active_connections / snap.max_connections) * 100
    : 0;
  const connColor =
    connPct > 85
      ? "text-red-400"
      : connPct > 60
        ? "text-yellow-400"
        : "text-green-400";
  const cacheColor =
    (snap?.cache_hit_ratio ?? 100) < 90 ? "text-orange-400" : "text-green-400";

  return (
    <div className="space-y-6">
      {/* Status bar */}
      <div className="flex items-center gap-2 text-xs text-gray-500">
        <span
          className={`w-2 h-2 rounded-full ${status === "open" ? "bg-green-500 animate-pulse" : "bg-red-500"}`}
        />
        {status === "open" ? "Live · updating every 10s" : "Reconnecting…"}
        <span className="ml-auto">DB: {dbId}</span>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <MetricCard
          label="Connections"
          value={snap?.active_connections ?? "—"}
          unit={`/ ${snap?.max_connections ?? "?"}`}
          color={connColor}
          trend={connPct > 80 ? "up" : "neutral"}
        />
        <MetricCard
          label="p99 Latency"
          value={snap ? (snap.p99_latency_ms ?? 0).toFixed(1) : "—"}
          unit="ms"
          color={
            (snap?.p99_latency_ms ?? 0) > 1000 ? "text-red-400" : "text-white"
          }
          trend={(snap?.p99_latency_ms ?? 0) > 1000 ? "up" : "neutral"}
        />
        <MetricCard
          label="Cache Hit Ratio"
          value={snap ? (snap.cache_hit_ratio ?? 0).toFixed(1) : "—"}
          unit="%"
          color={cacheColor}
          trend={(snap?.cache_hit_ratio ?? 100) < 90 ? "down" : "neutral"}
        />
        <MetricCard
          label="Lock Waits"
          value={snap?.lock_waits ?? "—"}
          color={
            (snap?.lock_waits ?? 0) > 5 ? "text-orange-400" : "text-white"
          }
          trend={(snap?.lock_waits ?? 0) > 0 ? "up" : "neutral"}
        />
      </div>

      {/* Charts */}
      <div className="grid lg:grid-cols-2 gap-4">
        <div className="card">
          <h3 className="text-sm font-semibold text-gray-300 mb-4">
            p99 Latency (ms)
          </h3>
          <ResponsiveContainer width="100%" height={200}>
            <AreaChart data={chartData}>
              <defs>
                <linearGradient id="p99g" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="t"
                tick={{ fontSize: 10, fill: "#9ca3af" }}
                interval="preserveStartEnd"
              />
              <YAxis tick={{ fontSize: 10, fill: "#9ca3af" }} />
              <Tooltip
                contentStyle={{
                  background: "#111827",
                  border: "1px solid #374151",
                  borderRadius: 8,
                }}
                labelStyle={{ color: "#9ca3af" }}
              />
              <ReferenceLine
                y={1000}
                stroke="#ef4444"
                strokeDasharray="4 2"
                label={{ value: "SLO", fill: "#ef4444", fontSize: 10 }}
              />
              <Area
                type="monotone"
                dataKey="p99"
                stroke="#3b82f6"
                fill="url(#p99g)"
                strokeWidth={2}
                dot={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        <div className="card">
          <h3 className="text-sm font-semibold text-gray-300 mb-4">
            Active Connections
          </h3>
          <ResponsiveContainer width="100%" height={200}>
            <AreaChart data={chartData}>
              <defs>
                <linearGradient id="conng" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#10b981" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#10b981" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="t"
                tick={{ fontSize: 10, fill: "#9ca3af" }}
                interval="preserveStartEnd"
              />
              <YAxis tick={{ fontSize: 10, fill: "#9ca3af" }} />
              <Tooltip
                contentStyle={{
                  background: "#111827",
                  border: "1px solid #374151",
                  borderRadius: 8,
                }}
              />
              {snap && (
                <ReferenceLine
                  y={snap.max_connections * 0.85}
                  stroke="#f59e0b"
                  strokeDasharray="4 2"
                />
              )}
              <Area
                type="monotone"
                dataKey="connections"
                stroke="#10b981"
                fill="url(#conng)"
                strokeWidth={2}
                dot={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        <div className="card">
          <h3 className="text-sm font-semibold text-gray-300 mb-4">QPS</h3>
          <ResponsiveContainer width="100%" height={200}>
            <AreaChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="t"
                tick={{ fontSize: 10, fill: "#9ca3af" }}
                interval="preserveStartEnd"
              />
              <YAxis tick={{ fontSize: 10, fill: "#9ca3af" }} />
              <Tooltip
                contentStyle={{
                  background: "#111827",
                  border: "1px solid #374151",
                  borderRadius: 8,
                }}
              />
              <Area
                type="monotone"
                dataKey="qps"
                stroke="#a855f7"
                fill="url(#p99g)"
                strokeWidth={2}
                dot={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        <div className="card">
          <h3 className="text-sm font-semibold text-gray-300 mb-4">
            Cache Hit Ratio (%)
          </h3>
          <ResponsiveContainer width="100%" height={200}>
            <AreaChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="t"
                tick={{ fontSize: 10, fill: "#9ca3af" }}
                interval="preserveStartEnd"
              />
              <YAxis
                domain={[80, 100]}
                tick={{ fontSize: 10, fill: "#9ca3af" }}
              />
              <Tooltip
                contentStyle={{
                  background: "#111827",
                  border: "1px solid #374151",
                  borderRadius: 8,
                }}
              />
              <ReferenceLine
                y={90}
                stroke="#f59e0b"
                strokeDasharray="4 2"
                label={{ value: "Min", fill: "#f59e0b", fontSize: 10 }}
              />
              <Area
                type="monotone"
                dataKey="cache"
                stroke="#f59e0b"
                fill="url(#p99g)"
                strokeWidth={2}
                dot={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────
// Slow Queries View
// ─────────────────────────────────────────────

function QueriesView({ dbId }: { dbId: string }) {
  const [selected, setSelected] = useState<SlowQuery | null>(null);
  const [analyzing, setAnalyzing] = useState(false);
  const [analysis, setAnalysis] = useState<QueryAnalysis | null>(null);
  const qc = useQueryClient();

  const { data: queries = [], isLoading } = useQuery({
    queryKey: ["slow-queries", dbId],
    queryFn: () => api.getSlowQueries(dbId, 30),
    refetchInterval: 60_000,
  });

  async function handleAnalyze(q: SlowQuery) {
    setSelected(q);
    setAnalyzing(true);
    setAnalysis(null);
    try {
      const r = await api.analyzeQuery(dbId, q.sample_sql ?? q.normalized_sql);
      setAnalysis(r as QueryAnalysis);
    } catch (e) {
      setAnalysis({ error: String(e) });
    } finally {
      setAnalyzing(false);
    }
  }

  async function handleCreateProposal(q: SlowQuery) {
    await api.createProposal(dbId, q.sample_sql ?? q.normalized_sql, true);
    qc.invalidateQueries({ queryKey: ["proposals"] });
    alert("Proposal created and replay scheduled!");
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold">Top Slow Queries</h2>
        <span className="text-xs text-gray-500">
          {queries.length} queries · threshold: 1000ms
        </span>
      </div>

      {isLoading && <div className="text-gray-500 text-sm">Loading…</div>}

      <div className="card overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-xs text-gray-500 border-b border-gray-700">
              <th className="text-left py-2 pr-4">Query</th>
              <th className="text-right pr-4">Calls</th>
              <th className="text-right pr-4">Avg (ms)</th>
              <th className="text-right pr-4">Total (ms)</th>
              <th className="text-left pr-4">Pattern</th>
              <th className="text-right">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-800">
            {queries.map((q) => (
              <tr
                key={q.fingerprint}
                className={`hover:bg-gray-800 cursor-pointer transition-colors ${
                  selected?.fingerprint === q.fingerprint ? "bg-gray-800" : ""
                }`}
                onClick={() => setSelected(q)}
              >
                <td className="py-2 pr-4 max-w-xs">
                  <span className="font-mono text-xs text-green-400 truncate block">
                    {q.normalized_sql.substring(0, 80)}…
                  </span>
                </td>
                <td className="text-right pr-4 font-mono text-xs">
                  {q.call_count.toLocaleString()}
                </td>
                <td
                  className={`text-right pr-4 font-mono text-xs font-bold ${
                    q.mean_time_ms > 2000
                      ? "text-red-400"
                      : q.mean_time_ms > 500
                        ? "text-orange-400"
                        : "text-green-400"
                  }`}
                >
                  {q.mean_time_ms.toFixed(0)}
                </td>
                <td className="text-right pr-4 font-mono text-xs text-gray-400">
                  {(q.total_time_ms / 1000).toFixed(1)}s
                </td>
                <td className="pr-4">
                  <span className="badge-info text-xs">{q.access_pattern}</span>
                </td>
                <td className="text-right">
                  <div className="flex gap-1 justify-end">
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        handleAnalyze(q);
                      }}
                      className="text-xs bg-blue-900/40 hover:bg-blue-800/60 text-blue-300 border border-blue-700 px-2 py-0.5 rounded transition-colors"
                    >
                      Analyze
                    </button>
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        handleCreateProposal(q);
                      }}
                      className="text-xs bg-purple-900/40 hover:bg-purple-800/60 text-purple-300 border border-purple-700 px-2 py-0.5 rounded transition-colors"
                    >
                      Optimize
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Detail panel */}
      {selected && (
        <div className="card space-y-4">
          <h3 className="text-sm font-semibold text-gray-300 flex items-center gap-2">
            <Eye size={14} /> Query Detail
          </h3>
          <pre className="sql-block">
            {selected.sample_sql ?? selected.normalized_sql}
          </pre>

          {analyzing && (
            <div className="flex items-center gap-2 text-blue-400 text-sm">
              <RefreshCw size={14} className="animate-spin" /> Running LLM
              analysis…
            </div>
          )}

          {analysis && !analyzing && (
            <div className="space-y-3">
              {/* Error state */}
              {analysis.error && (
                <div className="text-sm text-red-400 bg-red-900/10 border border-red-800/30 rounded p-2">
                  {analysis.error}
                </div>
              )}

              {/* Issues */}
              {analysis.issues && analysis.issues.length > 0 && (
                <div>
                  <p className="text-xs font-semibold text-gray-400 mb-1">
                    Issues Found
                  </p>
                  <ul className="space-y-1">
                    {analysis.issues.map((issue: string, i: number) => (
                      <li
                        key={i}
                        className="flex items-start gap-2 text-sm text-orange-300"
                      >
                        <AlertTriangle size={12} className="mt-0.5 shrink-0" />
                        {issue}
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              {/* Index recommendations */}
              {analysis.index_recommendations &&
                analysis.index_recommendations.length > 0 &&
                analysis.index_recommendations.map(
                  (idx: IndexRecommendation, i: number) => (
                    <div
                      key={i}
                      className="rounded-lg border border-green-800/50 bg-green-900/10 p-3"
                    >
                      <p className="text-xs text-green-400 font-semibold mb-1">
                        Index Recommendation · est.{" "}
                        {idx.estimated_improvement_pct}% faster
                      </p>
                      <pre className="sql-block text-green-300">{idx.ddl}</pre>
                      <p className="text-xs text-gray-400 mt-1">{idx.reason}</p>
                    </div>
                  ),
                )}

              {/* Rewritten query */}
              {analysis.rewritten_query && (
                <div className="rounded-lg border border-blue-800/50 bg-blue-900/10 p-3">
                  <p className="text-xs text-blue-400 font-semibold mb-1">
                    Rewritten Query
                  </p>
                  <pre className="sql-block text-blue-300">
                    {analysis.rewritten_query}
                  </pre>
                  {analysis.rewrite_explanation && (
                    <p className="text-xs text-gray-400 mt-1">
                      {analysis.rewrite_explanation}
                    </p>
                  )}
                </div>
              )}

              {/* Scores */}
              {(analysis.overall_impact_score != null ||
                analysis.confidence != null) && (
                <div className="flex items-center gap-3 text-xs text-gray-500">
                  {analysis.overall_impact_score != null && (
                    <span>
                      Impact score:{" "}
                      <span className="text-white font-bold">
                        {Number(analysis.overall_impact_score)}/100
                      </span>
                    </span>
                  )}
                  {analysis.confidence != null && (
                    <span>
                      Confidence:{" "}
                      <span className="text-white font-bold">
                        {(Number(analysis.confidence) * 100).toFixed(0)}%
                      </span>
                    </span>
                  )}
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────
// Optimizations View
// ─────────────────────────────────────────────

function OptimizationsView() {
  const qc = useQueryClient();
  const [reviewing, setReviewing] = useState<string | null>(null);
  const [comment, setComment] = useState("");

  const { data, isLoading } = useQuery({
    queryKey: ["proposals"],
    queryFn: () => api.listProposals(),
    refetchInterval: 30_000,
  });

  const proposals = data?.proposals ?? [];

  const approveMutation = useMutation({
    mutationFn: ({ id, approve }: { id: string; approve: boolean }) =>
      api.reviewProposal(id, approve, comment || "No comment"),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["proposals"] });
      setReviewing(null);
      setComment("");
    },
  });

  const rollbackMutation = useMutation({
    mutationFn: (id: string) =>
      api.rollbackProposal(id, "Manual rollback by DBA"),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["proposals"] }),
  });

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold">Optimization Proposals</h2>
        <span className="text-xs text-gray-500">
          {proposals.length} proposals
        </span>
      </div>

      {isLoading && <div className="text-gray-500 text-sm">Loading…</div>}

      {proposals.length === 0 && !isLoading && (
        <div className="card text-center py-12 text-gray-600">
          <Zap size={32} className="mx-auto mb-2 opacity-30" />
          No proposals yet. Analyze a slow query to generate one.
        </div>
      )}

      <div className="space-y-3">
        {proposals.map((p) => (
          <div key={p.id} className="card space-y-3">
            <div className="flex items-start justify-between gap-3">
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 mb-1">
                  <StateBadge state={p.state} />
                  <span className="text-xs text-gray-500 capitalize">
                    {p.proposal_type}
                  </span>
                </div>
                <p className="text-sm font-medium">{p.title}</p>
                <p className="text-xs text-gray-500 mt-0.5">
                  {formatDistanceToNow(new Date(p.created_at), {
                    addSuffix: true,
                  })}
                  {p.estimated_impact_score != null && (
                    <span className="ml-2">
                      Impact:{" "}
                      <span className="text-white">
                        {p.estimated_impact_score.toFixed(0)}/100
                      </span>
                    </span>
                  )}
                </p>
              </div>

              <div className="flex gap-1 shrink-0">
                {["proposed", "reviewed"].includes(p.state) && (
                  <button
                    onClick={() =>
                      setReviewing(reviewing === p.id ? null : p.id)
                    }
                    className="text-xs bg-gray-800 hover:bg-gray-700 text-gray-300 border border-gray-600 px-2 py-1 rounded"
                  >
                    Review
                  </button>
                )}
                {["deploying", "monitoring"].includes(p.state) && (
                  <button
                    onClick={() => rollbackMutation.mutate(p.id)}
                    className="text-xs bg-red-900/40 hover:bg-red-800 text-red-300 border border-red-700 px-2 py-1 rounded flex items-center gap-1"
                  >
                    <RotateCcw size={11} /> Rollback
                  </button>
                )}
              </div>
            </div>

            {p.llm_rationale && (
              <p className="text-xs text-gray-400 bg-gray-800/50 rounded p-2">
                {p.llm_rationale}
              </p>
            )}

            {p.ddl_statements?.length ? (
              <div>
                <p className="text-xs text-gray-500 mb-1">DDL to Execute</p>
                <pre className="sql-block">{p.ddl_statements.join(";\n")}</pre>
              </div>
            ) : null}

            {p.replay_summary != null && (
              <div className="text-xs text-gray-400 bg-green-900/10 border border-green-800/30 rounded p-2">
                ✅ Replay:{" "}
                {typeof p.replay_summary === "object" &&
                p.replay_summary !== null
                  ? (
                      (p.replay_summary as Record<string, unknown>)
                        .summary as string | undefined
                    ) ?? JSON.stringify(p.replay_summary)
                  : String(p.replay_summary)}
              </div>
            )}

            {/* Inline review form */}
            {reviewing === p.id && (
              <div className="border-t border-gray-700 pt-3 space-y-2">
                <textarea
                  className="w-full bg-gray-800 border border-gray-600 rounded p-2 text-sm resize-none focus:outline-none focus:border-blue-500"
                  rows={2}
                  placeholder="Required: review comment (min 10 chars)…"
                  value={comment}
                  onChange={(e) => setComment(e.target.value)}
                />
                <div className="flex gap-2">
                  <button
                    disabled={comment.length < 10 || approveMutation.isPending}
                    onClick={() =>
                      approveMutation.mutate({ id: p.id, approve: true })
                    }
                    className="flex items-center gap-1 text-xs bg-green-900/50 hover:bg-green-800 disabled:opacity-40 text-green-300 border border-green-700 px-3 py-1.5 rounded transition-colors"
                  >
                    <ThumbsUp size={11} /> Approve
                  </button>
                  <button
                    disabled={comment.length < 10 || approveMutation.isPending}
                    onClick={() =>
                      approveMutation.mutate({ id: p.id, approve: false })
                    }
                    className="flex items-center gap-1 text-xs bg-red-900/50 hover:bg-red-800 disabled:opacity-40 text-red-300 border border-red-700 px-3 py-1.5 rounded transition-colors"
                  >
                    <ThumbsDown size={11} /> Reject
                  </button>
                  <button
                    onClick={() => {
                      setReviewing(null);
                      setComment("");
                    }}
                    className="text-xs text-gray-500 hover:text-gray-300 px-2"
                  >
                    Cancel
                  </button>
                </div>
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────
// Capacity Planning View
// ─────────────────────────────────────────────

function CapacityView({ dbId }: { dbId: string }) {
  const [metric, setMetric] = useState("active_connections");

  const { data: forecast, isLoading } = useQuery({
    queryKey: ["forecast", dbId, metric],
    queryFn: () => api.getForecast(dbId, metric, 28),
    staleTime: 300_000,
  });

  const chartData =
    forecast?.forecast.map((p) => ({
      date: new Date(p.ds).toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
      }),
      predicted: p.yhat,
      lower: p.yhat_lower,
      upper: p.yhat_upper,
    })) ?? [];

  const metrics = [
    { id: "active_connections", label: "Connections" },
    { id: "p99_latency_ms", label: "p99 Latency" },
    { id: "qps", label: "QPS" },
    { id: "cache_hit_ratio", label: "Cache Hit Ratio" },
  ];

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-4">
        <h2 className="text-lg font-semibold">
          Capacity Planning · 28-day Forecast
        </h2>
        <div className="flex gap-1 ml-auto">
          {metrics.map((m) => (
            <button
              key={m.id}
              onClick={() => setMetric(m.id)}
              className={`text-xs px-2 py-1 rounded border transition-colors ${
                metric === m.id
                  ? "bg-blue-600 border-blue-500 text-white"
                  : "bg-gray-800 border-gray-600 text-gray-400 hover:text-white"
              }`}
            >
              {m.label}
            </button>
          ))}
        </div>
      </div>

      {/* Breach alert */}
      {forecast?.breach_date && (
        <div className="card border-orange-700/60 bg-orange-900/10 flex items-start gap-3">
          <AlertTriangle
            className="text-orange-400 shrink-0 mt-0.5"
            size={18}
          />
          <div>
            <p className="text-sm font-semibold text-orange-300">
              Capacity breach projected in {forecast.days_until_breach} days (
              {new Date(forecast.breach_date).toLocaleDateString()})
            </p>
            {forecast.recommendation && (
              <p className="text-xs text-gray-400 mt-1">
                {forecast.recommendation}
              </p>
            )}
          </div>
        </div>
      )}

      <div className="card">
        {isLoading ? (
          <div className="text-gray-500 text-sm py-8 text-center">
            Generating forecast…
          </div>
        ) : (
          <>
            <div className="flex items-center gap-4 mb-4 text-xs text-gray-400">
              <span>
                Current:{" "}
                <span className="text-white font-mono">
                  {(forecast?.current_value ?? 0).toFixed(1)} {forecast?.unit ?? ""}
                </span>
              </span>
            </div>
            <ResponsiveContainer width="100%" height={300}>
              <ComposedChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                <XAxis
                  dataKey="date"
                  tick={{ fontSize: 10, fill: "#9ca3af" }}
                  interval={3}
                />
                <YAxis tick={{ fontSize: 10, fill: "#9ca3af" }} />
                <Tooltip
                  contentStyle={{
                    background: "#111827",
                    border: "1px solid #374151",
                    borderRadius: 8,
                  }}
                />
                <Legend wrapperStyle={{ fontSize: 11 }} />
                <Area
                  dataKey="upper"
                  fill="#3b82f6"
                  fillOpacity={0.1}
                  stroke="none"
                  name="Confidence band"
                />
                <Area
                  dataKey="lower"
                  fill="#111827"
                  fillOpacity={1}
                  stroke="none"
                />
                <Line
                  type="monotone"
                  dataKey="predicted"
                  stroke="#3b82f6"
                  strokeWidth={2}
                  dot={false}
                  name="Forecast"
                />
              </ComposedChart>
            </ResponsiveContainer>
          </>
        )}
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────
// AI Chat View
// ─────────────────────────────────────────────

function ChatView({ dbId }: { dbId: string }) {
  const [messages, setMessages] = useState<
    Array<{ role: string; content: string }>
  >([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const bottomRef = useRef<HTMLDivElement>(null);

  const SUGGESTIONS = [
    "What are the top 5 slowest queries?",
    "How many active connections are there?",
    "What is the cache hit ratio?",
    "Are there any lock waits or deadlocks?",
    "Give me a health summary",
  ];

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  async function send(text?: string) {
    const content = text ?? input.trim();
    if (!content) return;
    setInput("");
    const userMsg = { role: "user", content };
    const newMessages = [...messages, userMsg];
    setMessages(newMessages);
    setLoading(true);

    try {
      const resp = await api.chat(dbId, newMessages);
      setMessages((prev) => [
        ...prev,
        { role: "assistant", content: resp.answer },
      ]);
    } catch (e) {
      setMessages((prev) => [
        ...prev,
        { role: "assistant", content: `Error: ${String(e)}` },
      ]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="flex flex-col h-[calc(100vh-12rem)]">
      <h2 className="text-lg font-semibold mb-4">AI DBA Assistant</h2>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto space-y-3 pr-1">
        {messages.length === 0 && (
          <div className="text-center pt-8 space-y-4">
            <MessageSquare size={40} className="mx-auto text-gray-700" />
            <p className="text-gray-500 text-sm">
              Ask me anything about your database performance
            </p>
            <div className="flex flex-wrap gap-2 justify-center mt-4">
              {SUGGESTIONS.map((s) => (
                <button
                  key={s}
                  onClick={() => send(s)}
                  className="text-xs bg-gray-800 hover:bg-gray-700 text-gray-300 border border-gray-600 px-3 py-1.5 rounded-full transition-colors"
                >
                  {s}
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((m, i) => (
          <div
            key={i}
            className={`flex ${m.role === "user" ? "justify-end" : "justify-start"}`}
          >
            <div
              className={`max-w-2xl rounded-xl px-4 py-2.5 text-sm leading-relaxed ${
                m.role === "user"
                  ? "bg-blue-600 text-white rounded-br-sm"
                  : "bg-gray-800 text-gray-200 rounded-bl-sm border border-gray-700"
              }`}
            >
              {m.content}
            </div>
          </div>
        ))}

        {loading && (
          <div className="flex justify-start">
            <div className="bg-gray-800 border border-gray-700 rounded-xl rounded-bl-sm px-4 py-2.5">
              <div className="flex gap-1">
                {[0, 1, 2].map((i) => (
                  <span
                    key={i}
                    className="w-1.5 h-1.5 bg-gray-500 rounded-full animate-bounce"
                    style={{ animationDelay: `${i * 0.15}s` }}
                  />
                ))}
              </div>
            </div>
          </div>
        )}

        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <div className="mt-3 flex gap-2">
        <input
          type="text"
          className="flex-1 bg-gray-800 border border-gray-600 rounded-xl px-4 py-2.5 text-sm focus:outline-none focus:border-blue-500 placeholder:text-gray-600"
          placeholder="Ask about query performance, capacity, alerts…"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && !e.shiftKey && send()}
          disabled={loading}
        />
        <button
          onClick={() => send()}
          disabled={!input.trim() || loading}
          className="bg-blue-600 hover:bg-blue-500 disabled:opacity-40 text-white p-2.5 rounded-xl transition-colors"
        >
          <Send size={16} />
        </button>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────
// Settings View
// ─────────────────────────────────────────────

function SettingsView() {
  // FIX: Typed as SettingRow[] (a [string, string] tuple array) so that
  // destructured `label` and `val` are string — not string | undefined —
  // eliminating "Type 'unknown' is not assignable to type 'ReactNode'".
  const thresholds: SettingRow[] = [
    ["Slow query threshold", "1000ms"],
    ["p99 regression rollback", "10%"],
    ["Max monitoring overhead", "2%"],
    ["Capacity warning window", "28 days"],
  ];

  return (
    <div className="space-y-6 max-w-2xl">
      <h2 className="text-lg font-semibold">Settings</h2>

      <div className="card space-y-4">
        <h3 className="text-sm font-semibold text-gray-300 flex items-center gap-2">
          <Database size={14} /> Monitored Databases
        </h3>
        <p className="text-xs text-gray-500">
          Configure in <code className="bg-gray-800 px-1 rounded">.env</code>{" "}
          via <code className="bg-gray-800 px-1 rounded">MONITORED_DBS</code>
        </p>
        <div className="bg-gray-950 rounded p-3 font-mono text-xs text-green-400">
          MONITORED_DBS=postgresql://user:pass@host:5432/db,mysql://user:pass@host:3306/db
        </div>
      </div>

      <div className="card space-y-4">
        <h3 className="text-sm font-semibold text-gray-300 flex items-center gap-2">
          <Zap size={14} /> LLM Provider
        </h3>
        <div className="space-y-2 text-sm text-gray-400">
          <div className="flex items-center gap-3">
            <span className="badge-success">Ollama</span>
            <span>Local model (DeepSeek-Coder 33B) — zero API cost</span>
          </div>
          <div className="flex items-center gap-3">
            <span className="badge-info">Groq</span>
            <span>
              Cloud burst fallback — set{" "}
              <code className="bg-gray-800 px-1 rounded">GROQ_API_KEY</code>
            </span>
          </div>
        </div>
      </div>

      <div className="card space-y-4">
        <h3 className="text-sm font-semibold text-gray-300 flex items-center gap-2">
          <Shield size={14} /> Alert Thresholds
        </h3>
        <div className="grid grid-cols-2 gap-3 text-xs text-gray-400">
          {thresholds.map(([label, val]) => (
            <div
              key={label}
              className="flex justify-between border-b border-gray-800 pb-1"
            >
              <span>{label}</span>
              <span className="text-white font-mono">{val}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────
// Root App
// ─────────────────────────────────────────────

export default function App() {
  const [tab, setTab] = useState("dashboard");
  const { data: databases = [], isLoading: dbsLoading } = useQuery({
    queryKey: ["databases"],
    queryFn: () => api.getDatabases(),
    refetchInterval: 60_000,
  });
  // Use the real, backend-assigned database id (a hash of the connection URL),
  // falling back to the demo id only if nothing is configured yet.
  const dbId = databases[0]?.id ?? "";

  return (
    <div className="flex h-screen overflow-hidden bg-gray-950">
      {/* Sidebar */}
      <aside className="w-56 shrink-0 border-r border-gray-800 flex flex-col">
        <div className="p-4 border-b border-gray-800">
          <div className="flex items-center gap-2">
            <div className="w-7 h-7 bg-blue-600 rounded-lg flex items-center justify-center">
              <Database size={14} />
            </div>
            <div>
              <p className="text-xs font-bold leading-none">Autonomous DBA</p>
              <p className="text-[10px] text-gray-500 mt-0.5">
                AI-Powered Admin
              </p>
            </div>
          </div>
        </div>

        <nav className="flex-1 p-2 space-y-0.5">
          {NAV_ITEMS.map(({ id, icon: Icon, label }) => (
            <button
              key={id}
              onClick={() => setTab(id)}
              className={`w-full flex items-center gap-2.5 px-3 py-2 rounded-lg text-sm transition-colors ${
                tab === id
                  ? "bg-blue-600/20 text-blue-400 border border-blue-600/30"
                  : "text-gray-400 hover:text-white hover:bg-gray-800"
              }`}
            >
              <Icon size={15} />
              {label}
            </button>
          ))}
        </nav>

        <div className="p-3 border-t border-gray-800">
          <div className="text-xs text-gray-600 leading-relaxed">
            <p>
              DB:{" "}
              <span className="text-gray-400 font-mono">
                {databases[0]?.name ?? (dbsLoading ? "connecting…" : "none")}
              </span>
            </p>
            <p className="mt-0.5">v1.0.0</p>
          </div>
        </div>
      </aside>

      {/* Main content */}
      <main className="flex-1 overflow-y-auto p-6">
        {tab === "settings" && <SettingsView />}
        {tab === "optimizations" && <OptimizationsView />}

        {tab !== "settings" && tab !== "optimizations" && !dbId && (
          <div className="text-sm text-gray-500 pt-8 text-center">
            {dbsLoading
              ? "Discovering monitored databases…"
              : "No monitored database is configured. Set MONITORED_DBS in your .env and restart the backend."}
          </div>
        )}

        {dbId && tab === "dashboard" && <DashboardView dbId={dbId} />}
        {dbId && tab === "queries" && <QueriesView dbId={dbId} />}
        {dbId && tab === "capacity" && <CapacityView dbId={dbId} />}
        {dbId && tab === "chat" && <ChatView dbId={dbId} />}
      </main>
    </div>
  );
}
