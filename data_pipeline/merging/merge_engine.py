"""
Merge Engine
============

Performs controlled merges between two datasets with key normalization,
validation, and comprehensive reporting.

Design decisions:
    - Uses left join by default (primary dataset keeps all rows).
    - Key normalization is applied before merge to handle format differences.
    - Post-merge validation checks for row count sanity, NaN inflation,
      key coverage, and row multiplication.
    - A merge report is generated and saved as JSON for auditability.
    - On failure the original primary DataFrame is returned unchanged.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from data_pipeline.merging.key_normalizer import KeyNormalizer

logger = logging.getLogger(__name__)


@dataclass
class MergeResult:
    """Container for the output of a merge operation."""

    merged_df: pd.DataFrame
    report: dict[str, Any]
    success: bool
    error: Optional[str] = None


class MergeEngine:
    """Merge two datasets on detected join keys with validation.

    Usage::

        engine = MergeEngine()
        result = engine.merge(
            primary_df, context_df,
            join_keys=[{"left_col": "state", "right_col": "state",
                        "key_type": "geo_state"}],
            primary_name="nonprofit_financials",
            context_name="unemployment_data",
        )
        if result.success:
            result.merged_df.to_csv("merged.csv", index=False)
    """

    def __init__(self) -> None:
        self._normalizer = KeyNormalizer()

    def merge(
        self,
        primary_df: pd.DataFrame,
        context_df: pd.DataFrame,
        join_keys: list[dict[str, Any]],
        *,
        primary_name: str = "primary",
        context_name: str = "context",
        how: str = "left",
    ) -> MergeResult:
        """Merge primary and context datasets on the given join keys.

        Parameters
        ----------
        primary_df : pd.DataFrame
            The primary (left) dataset.
        context_df : pd.DataFrame
            The context (right) dataset used to enrich the primary.
        join_keys : list[dict]
            Join key pairs produced by :class:`JoinDetector`.
        primary_name, context_name : str
            Dataset names for the report.
        how : str
            Pandas merge strategy (``'left'``, ``'inner'``, ``'outer'``).

        Returns
        -------
        MergeResult
        """
        logger.info("=" * 60)
        logger.info(
            "Starting merge: '%s' (primary) + '%s' (context)",
            primary_name, context_name,
        )
        logger.info(
            "Join keys: %s",
            [(jk["left_col"], jk["right_col"]) for jk in join_keys],
        )
        logger.info("Merge type: %s", how)
        logger.info("=" * 60)

        try:
            # --- Step 1: normalize join keys on copies --------------------
            primary_work = primary_df.copy()
            context_work = context_df.copy()

            for jk in join_keys:
                left_col = jk["left_col"]
                right_col = jk["right_col"]
                key_type = jk.get("key_type", "generic")

                logger.info(
                    "Normalizing key: %s (left) / %s (right) [%s]",
                    left_col, right_col, key_type,
                )
                primary_work[left_col] = self._normalizer.normalize_column(
                    primary_work[left_col], key_type,
                )
                context_work[right_col] = self._normalizer.normalize_column(
                    context_work[right_col], key_type,
                )

            # --- Step 2: perform merge ------------------------------------
            left_on = [jk["left_col"] for jk in join_keys]
            right_on = [jk["right_col"] for jk in join_keys]

            merged = pd.merge(
                primary_work,
                context_work,
                left_on=left_on,
                right_on=right_on,
                how=how,
                suffixes=("", "_ctx"),
            )

            # --- Step 3: drop duplicate right-side key columns ------------
            cols_to_drop = []
            for jk in join_keys:
                if jk["left_col"] != jk["right_col"]:
                    for candidate in (jk["right_col"], jk["right_col"] + "_ctx"):
                        if candidate in merged.columns:
                            cols_to_drop.append(candidate)
            if cols_to_drop:
                merged = merged.drop(columns=cols_to_drop, errors="ignore")
                logger.info("Dropped duplicate key columns: %s", cols_to_drop)

            # --- Step 4: validate -----------------------------------------
            report = self._validate(
                merged, primary_work, context_work, join_keys,
                primary_name=primary_name, context_name=context_name,
                how=how,
            )

            logger.info(
                "Merge complete — %d rows, %d columns",
                len(merged), len(merged.columns),
            )
            return MergeResult(
                merged_df=merged, report=report, success=True,
            )

        except Exception as exc:
            error_msg = f"Merge failed: {exc!r}"
            logger.exception(error_msg)
            report = {
                "status": "error",
                "error": error_msg,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            return MergeResult(
                merged_df=primary_df.copy(),
                report=report,
                success=False,
                error=error_msg,
            )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate(
        self,
        merged_df: pd.DataFrame,
        primary_df: pd.DataFrame,
        context_df: pd.DataFrame,
        join_keys: list[dict[str, Any]],
        *,
        primary_name: str,
        context_name: str,
        how: str,
    ) -> dict[str, Any]:
        """Generate a comprehensive validation report."""
        primary_rows = len(primary_df)
        context_rows = len(context_df)
        merged_rows = len(merged_df)

        # -- Key coverage: % of primary values that have context matches --
        key_coverage: dict[str, float] = {}
        for jk in join_keys:
            left_vals = set(primary_df[jk["left_col"]].dropna().unique())
            right_vals = set(context_df[jk["right_col"]].dropna().unique())
            if left_vals:
                key_coverage[jk["left_col"]] = round(
                    len(left_vals & right_vals) / len(left_vals), 4,
                )
            else:
                key_coverage[jk["left_col"]] = 0.0

        # -- NaN analysis on context-only columns -------------------------
        context_nan: dict[str, float] = {}
        for col in merged_df.columns:
            if col not in primary_df.columns:
                pct = round(
                    merged_df[col].isna().sum() / max(len(merged_df), 1) * 100, 2,
                )
                context_nan[col] = pct

        # -- Row multiplication check -------------------------------------
        if how == "left" and merged_rows > primary_rows:
            row_mult = True
            mult_factor = round(merged_rows / primary_rows, 2)
        else:
            row_mult = False
            mult_factor = 1.0

        report: dict[str, Any] = {
            "primary_dataset": primary_name,
            "context_dataset": context_name,
            "merge_type": how,
            "join_keys": [
                {
                    "left_col": jk["left_col"],
                    "right_col": jk["right_col"],
                    "key_type": jk.get("key_type", "generic"),
                }
                for jk in join_keys
            ],
            "primary_rows": primary_rows,
            "context_rows": context_rows,
            "merged_rows": merged_rows,
            "merged_columns": len(merged_df.columns),
            "key_coverage": key_coverage,
            "context_columns_nan_pct": context_nan,
            "row_multiplication_detected": row_mult,
            "row_multiplication_factor": mult_factor,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": "success",
        }

        # -- Emit warnings -------------------------------------------------
        if row_mult:
            logger.warning(
                "Row multiplication detected: %d → %d (%.1fx). "
                "Check for many-to-many key relationships.",
                primary_rows, merged_rows, mult_factor,
            )
        overall_cov = (
            sum(key_coverage.values()) / len(key_coverage)
            if key_coverage else 0
        )
        if overall_cov < 0.5:
            logger.warning(
                "Low key coverage (%.0f%%). "
                "Many primary rows may lack context matches.",
                overall_cov * 100,
            )
        high_nan = [c for c, p in context_nan.items() if p > 50]
        if high_nan:
            logger.warning(
                "High NaN%% (>50%%) in context columns after merge: %s",
                high_nan,
            )

        return report

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    @staticmethod
    def save_merged(
        merged_df: pd.DataFrame,
        output_dir: Path,
        primary_name: str,
        context_name: str,
    ) -> Path:
        """Write the merged DataFrame to CSV."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / f"{primary_name}_{context_name}_merged.csv"
        merged_df.to_csv(path, index=False, encoding="utf-8")
        logger.info("Merged dataset saved to %s", path)
        return path

    @staticmethod
    def save_report(
        report: dict[str, Any],
        output_dir: Path,
        primary_name: str,
        context_name: str,
    ) -> Path:
        """Write the merge validation report as JSON."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / f"{primary_name}_{context_name}_merge_report.json"
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, ensure_ascii=False)
        logger.info("Merge report saved to %s", path)
        return path
