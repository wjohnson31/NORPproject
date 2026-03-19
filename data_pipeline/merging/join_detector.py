"""
Join Detector
=============

Compares two dataset profiles (and optionally data samples) to identify
compatible join keys for merging.

Detection methods:
    1. **Synonym matching:** Column names are matched against predefined
       synonym groups (e.g., ``tax_year`` and ``year`` both map to
       ``time_year``).
    2. **Exact name matching:** Columns with identical names across datasets
       are flagged as potential keys (lower confidence).
    3. **Value overlap (optional):** If DataFrames are provided, computes
       the Jaccard overlap of unique values to validate candidates.

The detector returns a list of join-key-pair dicts sorted by confidence.
"""

import logging
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Synonym groups — each set maps column names to a semantic key type
# ---------------------------------------------------------------------------

_TIME_YEAR_SYNONYMS: set[str] = {
    "year", "tax_year", "fiscal_year", "fy", "calendar_year",
    "data_year", "report_year", "survey_year",
}

_GEO_STATE_SYNONYMS: set[str] = {
    "state", "state_cd", "state_code", "st", "state_abbr",
    "state_name", "state_abbreviation",
}

_GEO_COUNTY_SYNONYMS: set[str] = {
    "county", "county_name", "county_cd", "county_code", "county_fips",
}

_GEO_CITY_SYNONYMS: set[str] = {"city", "city_name"}

_GEO_ZIP_SYNONYMS: set[str] = {
    "zip", "zipcode", "zip_code", "zip5", "zip_5",
}

# Ordered list of (synonyms, key_type) for classification
_SYNONYM_GROUPS: list[tuple[set[str], str]] = [
    (_TIME_YEAR_SYNONYMS, "time_year"),
    (_GEO_STATE_SYNONYMS, "geo_state"),
    (_GEO_COUNTY_SYNONYMS, "geo_county"),
    (_GEO_CITY_SYNONYMS, "geo_city"),
    (_GEO_ZIP_SYNONYMS, "geo_zip"),
]


def _classify_column(col_name: str) -> Optional[str]:
    """Return the key_type for a column name, or None if unrecognized."""
    normalized = col_name.lower().strip()
    for synonyms, key_type in _SYNONYM_GROUPS:
        if normalized in synonyms:
            return key_type
    return None


def _value_overlap_ratio(
    series_a: pd.Series, series_b: pd.Series
) -> float:
    """Jaccard-like overlap between the unique non-null values of two series."""
    vals_a = set(series_a.dropna().astype(str).str.strip().str.lower().unique())
    vals_b = set(series_b.dropna().astype(str).str.strip().str.lower().unique())
    if not vals_a or not vals_b:
        return 0.0
    intersection = vals_a & vals_b
    union = vals_a | vals_b
    return len(intersection) / len(union) if union else 0.0


class JoinDetector:
    """Detect compatible join keys between two datasets.

    Usage::

        detector = JoinDetector()
        keys = detector.detect_join_keys(profile_a, profile_b, df_a, df_b)
        # [{"left_col": "state", "right_col": "state", "key_type": "geo_state",
        #   "confidence": 0.95, "match_method": "name_exact"}, ...]

    Parameters
    ----------
    min_confidence : float
        Minimum confidence threshold (0–1).  Pairs below this are discarded.
    """

    def __init__(self, min_confidence: float = 0.5) -> None:
        self._min_confidence = min_confidence

    def detect_join_keys(
        self,
        profile_left: dict[str, Any],
        profile_right: dict[str, Any],
        df_left: Optional[pd.DataFrame] = None,
        df_right: Optional[pd.DataFrame] = None,
    ) -> list[dict[str, Any]]:
        """Find compatible join key pairs between two dataset profiles.

        Parameters
        ----------
        profile_left : dict
            Schema profile of the left (primary) dataset.
        profile_right : dict
            Schema profile of the right (context) dataset.
        df_left, df_right : pd.DataFrame, optional
            If provided, value overlap is computed to refine confidence.

        Returns
        -------
        list[dict]
            Sorted by confidence (descending).  Each dict contains:
            ``left_col``, ``right_col``, ``key_type``, ``confidence``,
            ``match_method``, and optionally ``value_overlap``.
        """
        left_cols = profile_left.get("columns", [])
        right_cols = profile_right.get("columns", [])

        join_keys: list[dict[str, Any]] = []
        used_right: set[str] = set()

        # --- Pass 1: synonym-group matching --------------------------------
        left_types = {col: _classify_column(col) for col in left_cols}
        right_types = {col: _classify_column(col) for col in right_cols}

        for left_col, left_type in left_types.items():
            if left_type is None:
                continue
            for right_col, right_type in right_types.items():
                if right_col in used_right or right_type != left_type:
                    continue

                exact = left_col == right_col
                confidence = 0.95 if exact else 0.85
                method = "name_exact" if exact else "name_synonym"

                key_info = self._make_key(
                    left_col, right_col, left_type, confidence, method,
                    df_left, df_right,
                )
                join_keys.append(key_info)
                used_right.add(right_col)
                break  # one match per left column

        # --- Pass 2: exact-name matches for unclassified columns -----------
        matched_left = {jk["left_col"] for jk in join_keys}
        for left_col in left_cols:
            if left_col in matched_left:
                continue
            if left_col in right_cols and left_col not in used_right:
                key_info = self._make_key(
                    left_col, left_col, "generic", 0.7,
                    "name_exact_unclassified", df_left, df_right,
                )
                join_keys.append(key_info)
                used_right.add(left_col)

        # --- Filter and sort -----------------------------------------------
        join_keys = [
            jk for jk in join_keys if jk["confidence"] >= self._min_confidence
        ]
        join_keys.sort(key=lambda x: x["confidence"], reverse=True)

        if join_keys:
            summary = [
                (jk["left_col"], jk["right_col"], jk["key_type"])
                for jk in join_keys
            ]
            logger.info("Detected %d join key pair(s): %s", len(join_keys), summary)
        else:
            logger.warning("No compatible join keys detected between datasets.")

        return join_keys

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _make_key(
        left_col: str,
        right_col: str,
        key_type: str,
        confidence: float,
        method: str,
        df_left: Optional[pd.DataFrame],
        df_right: Optional[pd.DataFrame],
    ) -> dict[str, Any]:
        """Build a join-key dict, optionally enriched with value overlap."""
        key_info: dict[str, Any] = {
            "left_col": left_col,
            "right_col": right_col,
            "key_type": key_type,
            "confidence": confidence,
            "match_method": method,
        }
        if df_left is not None and df_right is not None:
            if left_col in df_left.columns and right_col in df_right.columns:
                overlap = _value_overlap_ratio(
                    df_left[left_col], df_right[right_col]
                )
                key_info["value_overlap"] = round(overlap, 4)
                if overlap > 0.1:
                    key_info["confidence"] = min(1.0, confidence + 0.05)
                elif overlap == 0.0:
                    key_info["confidence"] = max(0.0, confidence - 0.3)
        return key_info
