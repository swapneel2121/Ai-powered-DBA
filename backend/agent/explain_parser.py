"""
EXPLAIN ANALYZE parser.

Parses PostgreSQL JSON EXPLAIN output into a structured tree
for analysis by rule-based heuristics and LLM agents.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from backend.utils.logging import get_logger

log = get_logger(__name__)


@dataclass
class PlanNode:
    node_type: str
    relation_name: Optional[str] = None
    alias: Optional[str] = None
    index_name: Optional[str] = None
    join_type: Optional[str] = None

    # Cost estimates
    startup_cost: float = 0.0
    total_cost: float = 0.0
    plan_rows: int = 0

    # Actuals (ANALYZE)
    actual_rows: int = 0
    actual_time_ms: float = 0.0
    loops: int = 1

    # Buffer usage
    shared_hit_blocks: int = 0
    shared_read_blocks: int = 0
    temp_read_blocks: int = 0
    temp_written_blocks: int = 0

    # I/O timing
    io_read_ms: float = 0.0
    io_write_ms: float = 0.0

    # Filter
    filter_condition: Optional[str] = None
    rows_removed_by_filter: int = 0

    children: List["PlanNode"] = field(default_factory=list)

    @property
    def cost_score(self) -> float:
        """Weighted cost score for highlighting expensive nodes."""
        return self.total_cost * max(1, self.loops)

    @property
    def row_estimate_error(self) -> float:
        """Ratio of actual/estimated rows — high values indicate bad statistics."""
        if self.plan_rows == 0:
            return 0.0
        return abs(self.actual_rows - self.plan_rows) / self.plan_rows

    @property
    def is_seq_scan(self) -> bool:
        return self.node_type == "Seq Scan"

    @property
    def is_problematic(self) -> bool:
        return (
            self.is_seq_scan
            or self.row_estimate_error > 10
            or self.rows_removed_by_filter > 1000
            or self.shared_read_blocks > 1000
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_type": self.node_type,
            "relation_name": self.relation_name,
            "alias": self.alias,
            "index_name": self.index_name,
            "join_type": self.join_type,
            "startup_cost": self.startup_cost,
            "total_cost": self.total_cost,
            "plan_rows": self.plan_rows,
            "actual_rows": self.actual_rows,
            "actual_time_ms": self.actual_time_ms,
            "loops": self.loops,
            "shared_hit_blocks": self.shared_hit_blocks,
            "shared_read_blocks": self.shared_read_blocks,
            "filter_condition": self.filter_condition,
            "rows_removed_by_filter": self.rows_removed_by_filter,
            "row_estimate_error": round(self.row_estimate_error, 2),
            "is_problematic": self.is_problematic,
            "children": [c.to_dict() for c in self.children],
        }


@dataclass
class ExplainResult:
    root: PlanNode
    total_cost: float
    total_time_ms: float
    planning_time_ms: float
    execution_time_ms: float
    shared_hit_blocks: int
    shared_read_blocks: int

    # Derived findings
    seq_scans: List[PlanNode] = field(default_factory=list)
    missing_index_candidates: List[Dict] = field(default_factory=list)
    bad_estimates: List[PlanNode] = field(default_factory=list)
    high_cost_nodes: List[PlanNode] = field(default_factory=list)

    def summary_for_llm(self) -> str:
        """
        Compact text summary suitable for LLM prompt context.
        Keeps token usage low while preserving key information.
        """
        lines = [
            f"Execution time: {self.execution_time_ms:.1f}ms",
            f"Planning time: {self.planning_time_ms:.1f}ms",
            f"Total cost: {self.total_cost:.1f}",
            f"Buffer hits: {self.shared_hit_blocks}, reads: {self.shared_read_blocks}",
            "",
        ]

        if self.seq_scans:
            lines.append(f"SEQ SCANS ({len(self.seq_scans)}):")
            for node in self.seq_scans[:5]:
                lines.append(
                    f"  - {node.relation_name} "
                    f"(actual_rows={node.actual_rows}, "
                    f"cost={node.total_cost:.1f}, "
                    f"filter_removed={node.rows_removed_by_filter})"
                )

        if self.bad_estimates:
            lines.append(f"\nBAD ROW ESTIMATES ({len(self.bad_estimates)}):")
            for node in self.bad_estimates[:3]:
                lines.append(
                    f"  - {node.node_type} on {node.relation_name}: "
                    f"estimated={node.plan_rows} actual={node.actual_rows}"
                )

        if self.missing_index_candidates:
            lines.append(f"\nINDEX CANDIDATES:")
            for c in self.missing_index_candidates[:5]:
                lines.append(f"  - {c['table']}.({', '.join(c['columns'])})")

        return "\n".join(lines)


class ExplainParser:
    """Parse PostgreSQL EXPLAIN (FORMAT JSON) output into PlanNode tree."""

    def parse(self, explain_json: str | list | dict) -> ExplainResult:
        if isinstance(explain_json, str):
            data = json.loads(explain_json)
        else:
            data = explain_json

        # PG returns a list with one object
        if isinstance(data, list):
            data = data[0]

        plan_data = data.get("Plan", {})
        planning_time = data.get("Planning Time", 0.0)
        execution_time = data.get("Execution Time", 0.0)

        root = self._parse_node(plan_data)

        result = ExplainResult(
            root=root,
            total_cost=root.total_cost,
            total_time_ms=root.actual_time_ms * root.loops,
            planning_time_ms=planning_time,
            execution_time_ms=execution_time,
            shared_hit_blocks=root.shared_hit_blocks,
            shared_read_blocks=root.shared_read_blocks,
        )

        self._analyze(result, root)
        return result

    def _parse_node(self, data: Dict) -> PlanNode:
        node = PlanNode(
            node_type=data.get("Node Type", "Unknown"),
            relation_name=data.get("Relation Name"),
            alias=data.get("Alias"),
            index_name=data.get("Index Name"),
            join_type=data.get("Join Type"),
            startup_cost=data.get("Startup Cost", 0.0),
            total_cost=data.get("Total Cost", 0.0),
            plan_rows=data.get("Plan Rows", 0),
            actual_rows=data.get("Actual Rows", 0),
            actual_time_ms=data.get("Actual Total Time", 0.0),
            loops=data.get("Actual Loops", 1),
            shared_hit_blocks=data.get("Shared Hit Blocks", 0),
            shared_read_blocks=data.get("Shared Read Blocks", 0),
            temp_read_blocks=data.get("Temp Read Blocks", 0),
            temp_written_blocks=data.get("Temp Written Blocks", 0),
            io_read_ms=data.get("I/O Read Time", 0.0),
            io_write_ms=data.get("I/O Write Time", 0.0),
            filter_condition=data.get("Filter"),
            rows_removed_by_filter=data.get("Rows Removed by Filter", 0),
        )

        for child_data in data.get("Plans", []):
            node.children.append(self._parse_node(child_data))

        return node

    def _analyze(self, result: ExplainResult, node: PlanNode):
        """Walk tree and populate derived analysis lists."""
        if node.is_seq_scan and node.actual_rows > 500:
            result.seq_scans.append(node)

            # If filter removes many rows, suggest an index on filtered columns
            if node.rows_removed_by_filter > 100 and node.filter_condition:
                cols = self._extract_columns(node.filter_condition)
                if cols:
                    result.missing_index_candidates.append({
                        "table": node.relation_name,
                        "columns": cols,
                        "reason": f"Filter removes {node.rows_removed_by_filter} rows",
                    })

        if node.row_estimate_error > 10 and node.actual_rows > 100:
            result.bad_estimates.append(node)

        if node.total_cost > result.total_cost * 0.5:
            result.high_cost_nodes.append(node)

        for child in node.children:
            self._analyze(result, child)

    def _extract_columns(self, filter_expr: str) -> List[str]:
        """Heuristically extract column names from a filter expression."""
        import re
        # Match patterns like: column_name = ? or column_name > ?
        cols = re.findall(r"\b([a-z_][a-z0-9_]*)\s*(?:=|>|<|>=|<=|LIKE|IN)", filter_expr, re.I)
        # Filter out SQL keywords
        keywords = {"and", "or", "not", "null", "true", "false", "is"}
        return [c for c in cols if c.lower() not in keywords][:4]