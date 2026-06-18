"""
Query fingerprinting, normalization, LSH-based deduplication,
and access-pattern classification using XGBoost.
"""

from __future__ import annotations

import re
import hashlib
import pickle
from pathlib import Path
from typing import List, Optional

import numpy as np

from backend.utils.logging import get_logger

log = get_logger(__name__)

# ─────────────────────────────────────────────
# SQL Normalization Patterns
# ─────────────────────────────────────────────

# Replace string literals
_RE_STRINGS = re.compile(r"'[^']*'|\"[^\"]*\"")
# Replace numeric literals
_RE_NUMBERS = re.compile(r"\b\d+(\.\d+)?\b")
# Replace IN (...) list values
_RE_IN_LIST = re.compile(r"\bIN\s*\([^)]+\)", re.IGNORECASE)
# Collapse whitespace
_RE_WHITESPACE = re.compile(r"\s+")
# Remove comments
_RE_COMMENT_LINE = re.compile(r"--[^\n]*")
_RE_COMMENT_BLOCK = re.compile(r"/\*.*?\*/", re.DOTALL)

ACCESS_PATTERNS = ["oltp_point", "olap_scan", "batch_insert", "ddl", "unknown"]


class QueryFingerprinter:
    """
    Normalizes SQL queries and produces stable fingerprints
    for deduplication across different literal values.

    Uses Locality-Sensitive Hashing (SimHash) so that structurally
    similar queries cluster together even with minor variations.
    """

    def __init__(self, model_path: Optional[str] = None):
        self._classifier = None
        if model_path and Path(model_path).exists():
            with open(model_path, "rb") as f:
                self._classifier = pickle.load(f)
            log.info("classifier_loaded", path=model_path)

    # ── Normalization ─────────────────────────

    def normalize(self, sql: str) -> str:
        """Strip literals and collapse whitespace to produce a canonical form."""
        s = sql.strip()
        s = _RE_COMMENT_LINE.sub("", s)
        s = _RE_COMMENT_BLOCK.sub("", s)
        s = _RE_STRINGS.sub("?", s)
        s = _RE_IN_LIST.sub("IN (?)", s)
        s = _RE_NUMBERS.sub("?", s)
        s = _RE_WHITESPACE.sub(" ", s)
        return s.strip().upper()

    # ── Fingerprinting (SimHash) ──────────────

    def fingerprint(self, sql: str) -> str:
        """
        Produce a 64-bit SimHash fingerprint of the normalized SQL.
        Similar queries → Hamming distance < threshold.
        """
        normalized = self.normalize(sql)
        tokens = normalized.split()
        v = [0] * 64

        for token in tokens:
            h = int(hashlib.md5(token.encode()).hexdigest(), 16)
            for i in range(64):
                bit = (h >> i) & 1
                v[i] += 1 if bit else -1

        simhash = 0
        for i in range(64):
            if v[i] > 0:
                simhash |= 1 << i

        return format(simhash, "016x")

    def hamming_distance(self, fp1: str, fp2: str) -> int:
        """Hamming distance between two hex fingerprints."""
        h1 = int(fp1, 16)
        h2 = int(fp2, 16)
        xor = h1 ^ h2
        return bin(xor).count("1")

    def are_similar(self, fp1: str, fp2: str, threshold: int = 8) -> bool:
        """Two queries are 'similar' if their SimHash distance < threshold."""
        return self.hamming_distance(fp1, fp2) < threshold

    # ── Access Pattern Classification ─────────

    def classify_pattern(self, normalized_sql: str) -> str:
        """
        Classify a normalized SQL query into an access pattern.

        Rules-based heuristics first; XGBoost classifier as fallback.
        """
        sql = normalized_sql.strip().upper()

        # DDL
        if re.match(r"^(CREATE|DROP|ALTER|TRUNCATE|RENAME)\b", sql):
            return "ddl"

        # Batch insert
        if re.match(r"^INSERT\b", sql) and sql.count("(?) ") > 5:
            return "batch_insert"

        # OLTP: single-row lookup by primary key / equality predicate
        if (
            re.match(r"^(SELECT|UPDATE|DELETE)\b", sql)
            and re.search(r"\bWHERE\b", sql)
            and not re.search(r"\b(GROUP BY|HAVING|DISTINCT)\b", sql)
            and not re.search(r"\bJOIN\b.*\bJOIN\b", sql)  # at most 1 join
        ):
            return "oltp_point"

        # OLAP: aggregations, full scans, many joins
        if re.search(r"\b(GROUP BY|HAVING|SUM\(|COUNT\(|AVG\(|MAX\(|MIN\()\b", sql):
            return "olap_scan"

        # Use ML model if loaded
        if self._classifier is not None:
            features = self._extract_features(sql)
            pred = self._classifier.predict([features])[0]
            return ACCESS_PATTERNS[int(pred)]

        return "unknown"

    def _extract_features(self, sql: str) -> List[float]:
        """Extract numeric features for XGBoost classifier."""
        return [
            float(bool(re.match(r"^SELECT", sql))),
            float(bool(re.match(r"^INSERT", sql))),
            float(bool(re.match(r"^UPDATE", sql))),
            float(bool(re.match(r"^DELETE", sql))),
            float(bool(re.match(r"^(CREATE|DROP|ALTER)", sql))),
            sql.count("JOIN"),
            sql.count("WHERE"),
            sql.count("GROUP BY"),
            sql.count("ORDER BY"),
            sql.count("LIMIT"),
            sql.count("HAVING"),
            sql.count("DISTINCT"),
            sql.count("UNION"),
            sql.count("SUBQUERY") + sql.count("SELECT", 1),  # nested selects
            float(len(sql)),
        ]

    # ── Training helper ───────────────────────

    def train_classifier(self, labeled_queries: List[dict], output_path: str):
        """
        Train XGBoost classifier on labeled {sql, pattern} pairs.

        labeled_queries: [{"sql": "SELECT ...", "pattern": "oltp_point"}, ...]
        """
        try:
            import xgboost as xgb
        except ImportError:
            log.error("xgboost_not_installed")
            return

        X, y = [], []
        for item in labeled_queries:
            normalized = self.normalize(item["sql"])
            features = self._extract_features(normalized.upper())
            label = ACCESS_PATTERNS.index(item["pattern"])
            X.append(features)
            y.append(label)

        model = xgb.XGBClassifier(
            n_estimators=100,
            max_depth=4,
            use_label_encoder=False,
            eval_metric="mlogloss",
        )
        model.fit(np.array(X), np.array(y))

        with open(output_path, "wb") as f:
            pickle.dump(model, f)

        self._classifier = model
        log.info("classifier_trained", samples=len(X), path=output_path)
