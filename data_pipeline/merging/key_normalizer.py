"""
Key Normalizer
==============

Standardizes join key values across datasets so that semantically equivalent
values match reliably during merges.

Supported normalizations:
    - **State (geo):** Full state names → 2-letter abbreviations, case
      normalization.
    - **Year (time):** Extract year from datetime strings, YYYYMM formats,
      integer years, etc.
    - **String (generic):** Lowercase, strip whitespace, collapse multiple
      spaces.

Design decisions:
    - Normalization is applied on copies — original DataFrames are never
      mutated.
    - State normalization handles all 50 US states + DC and territories.
    - Year normalization uses pd.to_datetime with coercion for robustness.
    - Unrecognized values are left as-is after basic cleanup, rather than
      being dropped or set to NaN.
"""

import logging
import re
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# US state name → abbreviation mapping (lowercase keys)
# ---------------------------------------------------------------------------

_STATE_NAME_TO_ABBREV: dict[str, str] = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT",
    "delaware": "DE", "florida": "FL", "georgia": "GA", "hawaii": "HI",
    "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME",
    "maryland": "MD", "massachusetts": "MA", "michigan": "MI",
    "minnesota": "MN", "mississippi": "MS", "missouri": "MO",
    "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
    "new york": "NY", "north carolina": "NC", "north dakota": "ND",
    "ohio": "OH", "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA",
    "rhode island": "RI", "south carolina": "SC", "south dakota": "SD",
    "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY",
    "district of columbia": "DC", "puerto rico": "PR",
    "virgin islands": "VI", "guam": "GU", "american samoa": "AS",
    "northern mariana islands": "MP",
}

# Abbreviation → itself (lowercase key → uppercase value) for validation
_ABBREV_SELF: dict[str, str] = {
    v.lower(): v for v in _STATE_NAME_TO_ABBREV.values()
}

# Combined lookup: lowercase input → uppercase 2-letter abbreviation
_STATE_LOOKUP: dict[str, str] = {**_STATE_NAME_TO_ABBREV, **_ABBREV_SELF}


class KeyNormalizer:
    """Normalize join key values for reliable cross-dataset merging.

    All methods are stateless and can be used as static helpers or
    through the convenience method :meth:`normalize_column`.
    """

    @staticmethod
    def normalize_state(series: pd.Series) -> pd.Series:
        """Normalize state values to 2-letter uppercase abbreviations.

        Handles full names (``"California"`` → ``"CA"``), abbreviations
        (``"ca"`` → ``"CA"``), and mixed formats.  Unrecognized values
        are stripped and uppercased as a best-effort fallback.
        """
        def _norm(val):
            if pd.isna(val):
                return val
            s = str(val).strip().lower()
            return _STATE_LOOKUP.get(s, str(val).strip().upper())

        result = series.map(_norm)
        n_changed = int((result != series).sum())
        if n_changed:
            logger.info(
                "State normalization: %d/%d values changed", n_changed, len(series)
            )
        return result

    @staticmethod
    def normalize_year(series: pd.Series) -> pd.Series:
        """Extract / normalize year values to integers.

        Handles:
            - Integer years (``2020`` → ``2020``)
            - YYYYMM formats (``202012`` → ``2020``)
            - YYYYMMDD formats (``20200115`` → ``2020``)
            - Datetime strings (``"2020-01-15"`` → ``2020``)
            - String years (``"2020"`` → ``2020``)
        """
        def _extract_year(val):
            if pd.isna(val):
                return val
            # Integer-like value
            try:
                n = int(float(val))
                if 1900 <= n <= 2100:
                    return n
                s = str(n)
                if len(s) == 6:   # YYYYMM
                    return int(s[:4])
                if len(s) == 8:   # YYYYMMDD
                    return int(s[:4])
            except (ValueError, TypeError):
                pass
            # Try datetime parsing
            try:
                dt = pd.to_datetime(val, errors="coerce")
                if pd.notna(dt):
                    return dt.year
            except Exception:
                pass
            return val

        result = series.map(_extract_year)
        try:
            result = result.astype("Int64")
        except (ValueError, TypeError):
            pass
        logger.info("Year normalization applied to %d values", len(series))
        return result

    @staticmethod
    def normalize_string_key(series: pd.Series) -> pd.Series:
        """Generic string normalization: strip, lowercase, collapse whitespace."""
        def _norm(val):
            if pd.isna(val):
                return val
            s = str(val).strip().lower()
            s = re.sub(r"\s+", " ", s)
            return s

        return series.map(_norm)

    def normalize_column(
        self, series: pd.Series, key_type: str
    ) -> pd.Series:
        """Apply the appropriate normalization based on key type.

        Parameters
        ----------
        series : pd.Series
            Column values to normalize.
        key_type : str
            One of ``'geo_state'``, ``'time_year'``, or ``'generic'``.

        Returns
        -------
        pd.Series
            Normalized values.
        """
        dispatch = {
            "geo_state": self.normalize_state,
            "time_year": self.normalize_year,
        }
        normalizer = dispatch.get(key_type, self.normalize_string_key)
        return normalizer(series)
