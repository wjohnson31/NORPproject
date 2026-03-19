"""
Schema Profiler
===============

Extracts structural metadata from a loaded DataFrame.  The profile captures
column types, row/column counts, missingness, and lightweight heuristic
detection of temporal and geographic columns.

Architectural notes:
    - Detection heuristics for time and geography columns are intentionally
      shallow — they match on column *names* only (not values).  This avoids
      false positives from value-sniffing and keeps the profiler fast.
    - The profile is returned as a plain ``dict`` so it can be serialized
      to JSON without custom encoders.  Numpy dtypes are cast to strings
      for the same reason.
    - This module does NOT attempt semantic inference, type coercion, or
      any data modification.  It is strictly read-only.
"""

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Heuristic keyword sets for column-name matching
# ---------------------------------------------------------------------------

# Keywords that suggest a column contains temporal information.
_TIME_KEYWORDS: set[str] = {
    "date", "year", "month", "day", "quarter", "time",
    "timestamp", "fiscal_year", "fy", "tax_year", "period",
    "tax_period", "tax_prd",
}

# Keywords that suggest a column contains geographic information.
_GEO_KEYWORDS: set[str] = {
    "state", "fips", "county", "zip", "zipcode", "zip_code",
    "city", "region", "country", "province", "territory",
    "state_cd", "state_code", "st",
}

# Keywords that suggest a column is an identifier (key, not a metric).
_ID_KEYWORDS: set[str] = {
    "id", "ein", "code", "key", "name", "desc", "description",
    "type", "category", "status", "ntee", "naics", "sic",
}


class SchemaProfiler:
    """Profile the schema and basic statistics of a DataFrame.

    The profiler produces a structured dictionary containing:

    - ``columns``       : list of column names
    - ``dtypes``        : mapping of column name → dtype string
    - ``num_rows``      : total row count
    - ``num_columns``   : total column count
    - ``missingness``   : mapping of column name → percent missing (0–100)
    - ``time_columns``  : columns likely containing temporal data
    - ``geo_columns``   : columns likely containing geographic data

    Usage::

        profiler = SchemaProfiler(df)
        profile = profiler.generate_profile()

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to profile (typically from :class:`DatasetLoader`).
    """

    def __init__(self, df: pd.DataFrame) -> None:
        if df.empty:
            logger.warning("Profiling an empty DataFrame.")
        self._df: pd.DataFrame = df

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_profile(self) -> dict[str, Any]:
        """Generate the full schema profile.

        Returns
        -------
        dict
            A JSON-serializable dictionary containing all profile fields.
        """
        time_cols = self._detect_time_columns()
        geo_cols = self._detect_geo_columns()

        profile: dict[str, Any] = {
            "columns": self._extract_columns(),
            "dtypes": self._extract_dtypes(),
            "num_rows": self._count_rows(),
            "num_columns": self._count_columns(),
            "missingness": self._compute_missingness(),
            "time_columns": time_cols,
            "geo_columns": geo_cols,
            "column_roles": self._detect_column_roles(time_cols, geo_cols),
        }

        roles = profile["column_roles"]
        n_keys = sum(1 for r in roles.values() if r == "key")
        n_metrics = sum(1 for r in roles.values() if r == "metric")
        logger.info(
            "Profile generated — rows=%d, cols=%d, time=%d, geo=%d, "
            "keys=%d, metrics=%d",
            profile["num_rows"],
            profile["num_columns"],
            len(time_cols),
            len(geo_cols),
            n_keys,
            n_metrics,
        )

        return profile

    # ------------------------------------------------------------------
    # Individual extraction methods (kept separate for testability)
    # ------------------------------------------------------------------

    def _extract_columns(self) -> list[str]:
        """Return the list of column names."""
        return list(self._df.columns)

    def _extract_dtypes(self) -> dict[str, str]:
        """Return a mapping of column name to dtype as a string.

        Numpy dtype objects are not JSON-serializable, so we cast to ``str``.
        """
        return {col: str(dtype) for col, dtype in self._df.dtypes.items()}

    def _count_rows(self) -> int:
        """Return the number of rows in the DataFrame."""
        return len(self._df)

    def _count_columns(self) -> int:
        """Return the number of columns in the DataFrame."""
        return len(self._df.columns)

    def _compute_missingness(self) -> dict[str, float]:
        """Compute the percentage of missing values per column.

        Returns
        -------
        dict[str, float]
            Column name → missing percentage rounded to two decimal places.
            A value of ``0.0`` means no missing values; ``100.0`` means
            the entire column is null.
        """
        if len(self._df) == 0:
            return {col: 0.0 for col in self._df.columns}

        missing_pct = (
            self._df.isnull().sum() / len(self._df) * 100
        ).round(2)

        return missing_pct.to_dict()

    def _detect_time_columns(self) -> list[str]:
        """Identify columns whose names suggest temporal data.

        Detection is purely keyword-based against ``_TIME_KEYWORDS``.
        No value inspection is performed.

        Returns
        -------
        list[str]
            Column names that matched at least one time keyword.
        """
        matches: list[str] = []
        for col in self._df.columns:
            # Split on underscores and check each token.
            tokens = set(col.lower().split("_"))
            if tokens & _TIME_KEYWORDS:
                matches.append(col)

        if matches:
            logger.info("Detected potential time columns: %s", matches)
        return matches

    def _detect_geo_columns(self) -> list[str]:
        """Identify columns whose names suggest geographic data.

        Detection is purely keyword-based against ``_GEO_KEYWORDS``.
        No value inspection is performed.

        Returns
        -------
        list[str]
            Column names that matched at least one geography keyword.
        """
        matches: list[str] = []
        for col in self._df.columns:
            tokens = set(col.lower().split("_"))
            if tokens & _GEO_KEYWORDS:
                matches.append(col)

        if matches:
            logger.info("Detected potential geo columns: %s", matches)
        return matches

    def _detect_column_roles(
        self, time_cols: list[str], geo_cols: list[str],
    ) -> dict[str, str]:
        """Classify each column as ``'key'``, ``'metric'``, or ``'dimension'``.

        Rules:
            - Time and geo columns are always ``'key'``.
            - Columns whose name contains an ID keyword are ``'key'``.
            - Remaining numeric columns are ``'metric'``.
            - Everything else is ``'dimension'``.

        Returns
        -------
        dict[str, str]
            Column name → role string.
        """
        key_cols = set(time_cols) | set(geo_cols)
        roles: dict[str, str] = {}

        for col in self._df.columns:
            if col in key_cols:
                roles[col] = "key"
                continue
            tokens = set(col.lower().split("_"))
            if tokens & _ID_KEYWORDS:
                roles[col] = "key"
                continue
            dtype_str = str(self._df[col].dtype)
            if "int" in dtype_str or "float" in dtype_str:
                roles[col] = "metric"
            else:
                roles[col] = "dimension"

        return roles
