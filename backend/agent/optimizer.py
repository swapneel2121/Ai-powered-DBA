"""
LLM-powered SQL optimization service.

Uses Ollama (local) or Groq (cloud burst) to:
  1. Analyze EXPLAIN ANALYZE output
  2. Rewrite inefficient queries
  3. Generate index recommendations
  4. Tune configuration parameters

Includes a validation harness to ensure rewrites produce identical results.
"""
from __future__ import annotations

import json
import re
from typing import Dict, List, Optional, Tuple

import httpx

from backend.agent.explain_parser import ExplainResult
from backend.utils.config import settings
from backend.utils.logging import get_logger

log = get_logger(__name__)

# ─────────────────────────────────────────────
# Prompts
# ─────────────────────────────────────────────

SYSTEM_PROMPT = """You are an expert PostgreSQL and MySQL DBA with 20 years of experience.
Your job is to analyze slow SQL queries and EXPLAIN plans, then produce concrete optimizations.

RULES:
1. Only suggest changes that are safe and reversible
2. Always explain WHY each change improves performance
3. For index recommendations, include the exact CREATE INDEX DDL
4. For query rewrites, preserve exact semantics (same result set)
5. Respond ONLY with valid JSON matching the requested schema
6. Never suggest DROP operations without explicit confirmation
"""

OPTIMIZE_QUERY_PROMPT = """Analyze this slow SQL query and its EXPLAIN plan.

DATABASE TYPE: {db_type}
QUERY:
```sql
{sql}
```

EXPLAIN ANALYSIS:
{explain_summary}

SCHEMA CONTEXT:
{schema_context}

Respond with JSON:
{{
  "issues": ["list of identified performance issues"],
  "index_recommendations": [
    {{
      "ddl": "CREATE INDEX CONCURRENTLY idx_name ON table(col1, col2)",
      "reason": "why this index helps",
      "estimated_improvement_pct": 60
    }}
  ],
  "rewritten_query": "optimized SQL or null if no rewrite needed",
  "rewrite_explanation": "what changed and why",
  "config_recommendations": [
    {{
      "parameter": "work_mem",
      "current_value": "4MB",
      "recommended_value": "64MB",
      "reason": "hash join spilling to disk"
    }}
  ],
  "overall_impact_score": 75,
  "confidence": 0.85
}}"""

NL_TO_SQL_PROMPT = """You are a DBA assistant. Convert this natural language question
about database performance into SQL that queries the monitoring data store.

Available tables:
- query_snapshots(database_id, fingerprint, normalized_sql, call_count, mean_time_ms, p99_time_ms, captured_at)
- alert_log(database_id, severity, title, fired_at, resolved_at)
- optimization_proposals(database_id, title, state, estimated_improvement_pct, created_at)

DATABASE_ID to query: {database_id}

QUESTION: {question}

Respond with JSON:
{{
  "sql": "SELECT ... FROM ...",
  "chart_type": "line|bar|table|number",
  "explanation": "what this query answers"
}}"""

WHAT_IF_PROMPT = """A database currently has these statistics:
{db_stats}

The user asks: "{question}"

Provide a data-backed analysis. Respond with JSON:
{{
  "analysis": "detailed explanation",
  "predicted_impact": {{
    "metric": "value with unit"
  }},
  "recommendations": ["action 1", "action 2"],
  "confidence": 0.75
}}"""


# ─────────────────────────────────────────────
# LLM Client
# ─────────────────────────────────────────────

class LLMService:
    """
    Unified LLM client supporting Ollama (local) and Groq (cloud).
    Automatically falls back to Groq when Ollama is unavailable.
    """

    def __init__(self):
        # Fail fast when the LLM is unreachable (3s connect) so the UI degrades
        # to the rule-based path quickly instead of hanging; allow up to 30s for
        # a model that is responding but slow.
        self._http = httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=3.0))
        self._cache: Dict[str, str] = {}  # Simple in-memory; Redis in production

    async def complete(self, prompt: str, system: str = SYSTEM_PROMPT) -> str:
        cache_key = self._cache_key(system, prompt)
        if cache_key in self._cache:
            log.debug("llm_cache_hit")
            return self._cache[cache_key]

        if settings.llm_provider == "ollama":
            try:
                result = await self._ollama_complete(prompt, system)
                self._cache[cache_key] = result
                return result
            except Exception as e:
                log.warning("ollama_failed_falling_back_to_groq", error=str(e))

        result = await self._groq_complete(prompt, system)
        self._cache[cache_key] = result
        return result

    async def _ollama_complete(self, prompt: str, system: str) -> str:
        payload = {
            "model": settings.ollama_model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            "stream": False,
            "options": {"temperature": 0.1},
        }
        resp = await self._http.post(
            f"{settings.ollama_base_url}/api/chat",
            json=payload,
        )
        resp.raise_for_status()
        return resp.json()["message"]["content"]

    async def _groq_complete(self, prompt: str, system: str) -> str:
        payload = {
            "model": settings.groq_model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.1,
            "max_tokens": 2048,
        }
        resp = await self._http.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {settings.groq_api_key}"},
            json=payload,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]

    def _cache_key(self, system: str, prompt: str) -> str:
        import hashlib
        return hashlib.md5(f"{system}:{prompt}".encode()).hexdigest()

    async def close(self):
        await self._http.aclose()


# ─────────────────────────────────────────────
# Optimizer
# ─────────────────────────────────────────────

class SQLOptimizer:
    """High-level optimization orchestrator."""

    def __init__(self, llm: LLMService):
        self.llm = llm

    async def analyze_query(
        self,
        sql: str,
        db_type: str,
        explain_result: Optional[ExplainResult] = None,
        schema_context: str = "",
    ) -> Dict:
        """
        Full optimization analysis for a single query.
        Returns structured recommendations ready for proposal creation.
        """
        explain_summary = (
            explain_result.summary_for_llm()
            if explain_result
            else "EXPLAIN not available"
        )

        prompt = OPTIMIZE_QUERY_PROMPT.format(
            db_type=db_type,
            sql=sql,
            explain_summary=explain_summary,
            schema_context=schema_context or "No schema context provided",
        )

        try:
            raw = await self.llm.complete(prompt)
            result = self._parse_json_response(raw)
            # If the LLM produced no usable structured output, fall back.
            if not isinstance(result, dict) or "raw_response" in result:
                raise ValueError("LLM returned no structured optimization")
        except Exception as e:  # noqa: BLE001 - any LLM/network failure degrades gracefully
            log.warning("llm_analysis_failed_using_rule_based", error=str(e)[:200])
            result = self._rule_based_analysis(sql, db_type, explain_result)

        log.info(
            "query_analyzed",
            impact_score=result.get("overall_impact_score"),
            index_count=len(result.get("index_recommendations", [])),
            has_rewrite=bool(result.get("rewritten_query")),
            source=result.get("analysis_source", "llm"),
        )
        return result

    async def batch_analyze(
        self, queries: List[Dict], db_type: str, batch_size: int = 20
    ) -> List[Dict]:
        """
        Analyze multiple queries grouped into batches of `batch_size`
        to reduce LLM API calls.
        """
        results = []
        for i in range(0, len(queries), batch_size):
            batch = queries[i : i + batch_size]
            batch_prompt = self._build_batch_prompt(batch, db_type)
            raw = await self.llm.complete(batch_prompt)
            parsed = self._parse_json_response(raw)
            if isinstance(parsed, list):
                results.extend(parsed)
            else:
                results.append(parsed)
        return results

    async def answer_nl_question(
        self, question: str, database_id: str, db_stats: Dict
    ) -> Tuple[str, Optional[str]]:
        """
        Translate a natural language question into SQL (for monitoring data)
        or a what-if analysis.
        """
        # What-if questions get a different prompt
        what_if_triggers = ["what would happen", "what if", "simulate", "predict"]
        if any(t in question.lower() for t in what_if_triggers):
            prompt = WHAT_IF_PROMPT.format(
                db_stats=json.dumps(db_stats, indent=2, default=str),
                question=question,
            )
            try:
                raw = await self.llm.complete(prompt)
                parsed = self._parse_json_response(raw)
                return parsed.get("analysis", raw), None
            except Exception as e:  # noqa: BLE001
                log.warning("llm_whatif_failed", error=str(e)[:200])
                return (
                    "The language model is currently unavailable, so I can't run a "
                    "what-if simulation right now. Current snapshot: "
                    + json.dumps(db_stats, default=str)[:500],
                    None,
                )

        # Standard NL→SQL
        prompt = NL_TO_SQL_PROMPT.format(
            database_id=database_id,
            question=question,
        )
        try:
            raw = await self.llm.complete(prompt)
            parsed = self._parse_json_response(raw)
        except Exception as e:  # noqa: BLE001
            log.warning("llm_nl2sql_failed", error=str(e)[:200])
            return (
                "The language model is currently unavailable, so I can't translate "
                "that question into SQL right now. Please try again once Ollama/Groq "
                "is reachable.",
                None,
            )
        sql = parsed.get("sql")
        explanation = parsed.get("explanation", "")
        return explanation, sql

    # ── Validation ────────────────────────────

    async def validate_rewrite(
        self,
        original_sql: str,
        rewritten_sql: str,
        test_conn,
        sample_size: int = 1000,
    ) -> Tuple[bool, str]:
        """
        Verify rewritten query produces identical results to original
        on a sample of test data.

        Returns (is_valid, reason).
        """
        try:
            orig_rows = await test_conn.fetch(
                f"SELECT * FROM ({original_sql}) t LIMIT {sample_size}"
            )
            new_rows = await test_conn.fetch(
                f"SELECT * FROM ({rewritten_sql}) t LIMIT {sample_size}"
            )

            orig_set = {json.dumps(dict(r), sort_keys=True, default=str) for r in orig_rows}
            new_set = {json.dumps(dict(r), sort_keys=True, default=str) for r in new_rows}

            if orig_set == new_set:
                return True, "Result sets identical"

            diff_count = len(orig_set.symmetric_difference(new_set))
            return False, f"Result sets differ: {diff_count} rows different"

        except Exception as e:
            return False, f"Validation error: {e}"

    # ── Rule-based fallback ───────────────────

    def _rule_based_analysis(
        self, sql: str, db_type: str, explain_result: Optional[ExplainResult] = None
    ) -> Dict:
        """
        Heuristic optimizer used when the LLM is unavailable.

        Produces a best-effort set of issues and index recommendations from the
        raw SQL (and EXPLAIN plan when present) so the pipeline keeps working
        without a model. Never raises.
        """
        issues: List[str] = []
        index_recs: List[Dict] = []
        config_recs: List[Dict] = []
        impact = 0

        sql_l = sql.lower()
        first_table = None
        m = re.search(r"\bfrom\s+([a-zA-Z_][\w\.]*)", sql_l)
        if m:
            first_table = m.group(1)

        # SELECT *  →  projection pruning
        if re.search(r"select\s+\*", sql_l):
            issues.append("Query uses SELECT * which reads unnecessary columns and bloats I/O.")
            impact += 15

        # WHERE equality / range predicates → candidate index columns
        where_cols = re.findall(r"\bwhere\b(.*?)(?:group by|order by|limit|$)", sql_l, re.S)
        cols: List[str] = []
        if where_cols:
            cols = re.findall(r"([a-zA-Z_][\w]*)\s*(?:=|>|<|>=|<=|like|in)\b", where_cols[0])
            cols = [c for c in cols if c not in ("and", "or", "not", "null")]
        if first_table and cols:
            uniq = list(dict.fromkeys(cols))[:3]
            idx_name = f"idx_{first_table.replace('.', '_')}_{'_'.join(uniq)}"
            cols_sql = ", ".join(uniq)
            concurrently = "CONCURRENTLY " if db_type == "postgresql" else ""
            index_recs.append(
                {
                    "ddl": f"CREATE INDEX {concurrently}{idx_name} ON {first_table} ({cols_sql})",
                    "reason": f"Predicate columns ({cols_sql}) are filtered but may lack a supporting index.",
                    "estimated_improvement_pct": 50,
                }
            )
            impact += 40

        # ORDER BY without LIMIT on a wide scan
        if "order by" in sql_l and "limit" not in sql_l:
            issues.append("ORDER BY without LIMIT may sort the entire result set; consider pagination.")
            impact += 10

        # LIKE '%foo' leading wildcard defeats b-tree indexes
        if re.search(r"like\s+'%", sql_l):
            issues.append("Leading-wildcard LIKE ('%...') cannot use a b-tree index; consider trigram/GIN index.")
            impact += 10

        # EXPLAIN-derived signals
        if explain_result is not None:
            try:
                summary = explain_result.summary_for_llm().lower()
                if "seq scan" in summary:
                    issues.append("Sequential scan detected in EXPLAIN plan; an index may eliminate it.")
                    impact += 20
            except Exception:  # noqa: BLE001
                pass

        impact = min(impact, 95)
        return {
            "issues": issues or ["No obvious heuristic issues found; full LLM analysis recommended."],
            "index_recommendations": index_recs,
            "rewritten_query": None,
            "rewrite_explanation": "Rule-based fallback does not rewrite queries; enable the LLM for rewrites.",
            "config_recommendations": config_recs,
            "overall_impact_score": impact,
            "confidence": 0.4,
            "analysis_source": "rule_based",
        }

    # ── Helpers ───────────────────────────────

    def _parse_json_response(self, raw: str) -> Dict:
        """Extract JSON from LLM response, handling markdown code blocks."""
        # Strip markdown fences
        raw = re.sub(r"```json\s*", "", raw)
        raw = re.sub(r"```\s*", "", raw)

        # Find the first { or [
        start = min(
            (raw.find("{") if "{" in raw else len(raw)),
            (raw.find("[") if "[" in raw else len(raw)),
        )
        if start == len(raw):
            log.warning("llm_response_no_json", snippet=raw[:200])
            return {"raw_response": raw}

        try:
            return json.loads(raw[start:])
        except json.JSONDecodeError as e:
            log.warning("llm_json_parse_failed", error=str(e), snippet=raw[:300])
            return {"raw_response": raw}

    def _build_batch_prompt(self, queries: List[Dict], db_type: str) -> str:
        lines = [f"Analyze these {len(queries)} slow {db_type} queries as a batch.\n"]
        for i, q in enumerate(queries):
            lines.append(f"QUERY {i+1} (avg {q.get('mean_time_ms', '?')}ms):")
            lines.append(f"```sql\n{q.get('normalized_sql', '')}\n```")
        lines.append(
            "\nReturn a JSON array with one optimization object per query, "
            "same schema as single-query analysis."
        )
        return "\n".join(lines)