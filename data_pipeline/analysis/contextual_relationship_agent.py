"""
Contextual Relationship Query Agent (W7–W8)
============================================

Autonomous grouped and correlational analysis on tabular data (typically a
merged primary + context dataset). All computation uses pandas only — no SQL
strings and no user-authored queries.

The agent infers numeric metrics and grouping columns from a schema profile,
produces correlation summaries and a heatmap, runs multiple groupby aggregate
views with simple bar charts, and writes JSON plus a plain-text merged summary.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from data_pipeline.config import ANALYSIS_OUTPUT_DIR
from data_pipeline.ingestion.schema import SchemaProfiler

logger = logging.getLogger(__name__)

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

_MAX_NUMERIC_FOR_CORR = 14
_MAX_GROUP_COLS = 4
_MIN_GROUP_CARDINALITY = 2
_MAX_GROUP_CARDINALITY = 48
_TOP_CORR_PAIRS = 12
_TOP_GROUPS_IN_CHART = 18


def _safe_stem(s: str) -> str:
    s = re.sub(r"[^\w\-]+", "_", s.strip(), flags=re.UNICODE)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "analysis"


safe_output_stem = _safe_stem


def _numeric_columns(df: pd.DataFrame, profile: dict[str, Any]) -> list[str]:
    roles = profile.get("column_roles") or {}
    out: list[str] = []
    for col in df.columns:
        if not pd.api.types.is_numeric_dtype(df[col]):
            continue
        if roles.get(col) == "key":
            if col in (profile.get("time_columns") or []) or col in (
                profile.get("geo_columns") or []
            ):
                continue
        out.append(col)
    seen: set[str] = set()
    ordered = [c for c in out if c not in seen and not seen.add(c)]
    return ordered[:_MAX_NUMERIC_FOR_CORR]


def _group_column_candidates(df: pd.DataFrame, profile: dict[str, Any]) -> list[str]:
    roles = profile.get("column_roles") or {}
    geo = list(profile.get("geo_columns") or [])
    time_c = list(profile.get("time_columns") or [])
    n_rows = max(len(df), 1)

    def card(col: str) -> int:
        return int(df[col].nunique(dropna=True))

    def ok(col: str) -> bool:
        if col not in df.columns:
            return False
        nu = card(col)
        if nu < _MIN_GROUP_CARDINALITY or nu > _MAX_GROUP_CARDINALITY:
            return False
        if nu > max(3, int(n_rows * 0.5)):
            return False
        return True

    ordered: list[str] = []
    for col in geo + time_c:
        if col not in ordered and ok(col):
            ordered.append(col)

    dims = [
        c
        for c, r in roles.items()
        if r == "dimension" and c in df.columns and ok(c)
    ]
    dims.sort(key=lambda c: (card(c), c))

    for col in dims:
        if col not in ordered:
            ordered.append(col)
    return ordered[:_MAX_GROUP_COLS]


def _correlation_section(
    df: pd.DataFrame,
    numeric_cols: list[str],
    out_dir: Path,
    stem: str,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "numeric_columns_used": numeric_cols,
        "top_abs_correlations": [],
        "heatmap_path": None,
    }
    if len(numeric_cols) < 2:
        result["note"] = "Need at least two numeric columns for correlation."
        return result

    sub = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    corr = sub.corr(method="pearson", min_periods=max(10, len(sub) // 20))
    result["matrix_labels"] = list(corr.columns)

    pairs: list[dict[str, Any]] = []
    for i, a in enumerate(corr.columns):
        for j, b in enumerate(corr.columns):
            if j <= i:
                continue
            v = corr.iloc[i, j]
            if pd.isna(v):
                continue
            pairs.append({"column_a": a, "column_b": b, "pearson_r": round(float(v), 4)})
    pairs.sort(key=lambda p: abs(p["pearson_r"]), reverse=True)
    result["top_abs_correlations"] = pairs[:_TOP_CORR_PAIRS]

    fig, ax = plt.subplots(figsize=(min(10, 0.6 * len(numeric_cols) + 4), 8))
    im = ax.imshow(corr.values, aspect="auto", cmap="RdBu_r", vmin=-1, vmax=1)
    ax.set_xticks(range(len(numeric_cols)))
    ax.set_yticks(range(len(numeric_cols)))
    short = [c[:22] + "…" if len(c) > 23 else c for c in numeric_cols]
    ax.set_xticklabels(short, rotation=45, ha="right", fontsize=8)
    ax.set_yticklabels(short, fontsize=8)
    ax.set_title("Pearson correlation (numeric columns)")
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    fig.tight_layout()
    heat_path = out_dir / f"{stem}_correlation_heatmap.png"
    fig.savefig(heat_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    result["heatmap_path"] = str(heat_path)
    logger.info("Saved correlation heatmap: %s", heat_path)
    return result


def _grouped_section(
    df: pd.DataFrame,
    group_col: str,
    numeric_cols: list[str],
    out_dir: Path,
    stem: str,
) -> dict[str, Any]:
    g = df.dropna(subset=[group_col]).copy()
    section: dict[str, Any] = {
        "group_by": group_col,
        "group_count": int(g[group_col].nunique(dropna=True)),
        "row_count": len(g),
        "mean_table_path": None,
        "chart_path": None,
        "summary_metrics": {},
    }
    metrics = [c for c in numeric_cols if c != group_col][:8]
    if not metrics:
        section["note"] = "No numeric metrics to aggregate for this grouping."
        return section

    agg = g.groupby(group_col, dropna=False)[metrics].agg(["mean", "std", "count"])
    agg.columns = [f"{a}_{b}" for a, b in agg.columns]
    agg = agg.reset_index()
    csv_path = out_dir / f"{stem}_grouped_{_safe_stem(group_col)}_aggregates.csv"
    agg.to_csv(csv_path, index=False, encoding="utf-8")
    section["mean_table_path"] = str(csv_path)

    means_only = g.groupby(group_col, dropna=False)[metrics].mean()
    best_metric = metrics[0]
    best_spread = 0.0
    for m in metrics:
        col_means = means_only[m].dropna()
        if len(col_means) < 2:
            continue
        spread = float(col_means.std())
        if spread > best_spread:
            best_spread = spread
            best_metric = m

    plot_means = (
        g.groupby(group_col, dropna=False)[best_metric]
        .mean()
        .dropna()
        .sort_values(ascending=False)
        .head(_TOP_GROUPS_IN_CHART)
    )
    fig, ax = plt.subplots(figsize=(9, max(4, 0.35 * len(plot_means))))
    y_pos = range(len(plot_means))
    ax.barh(list(y_pos), plot_means.values, color="steelblue")
    ax.set_yticks(list(y_pos))
    labels = [str(i)[:40] for i in plot_means.index]
    ax.set_yticklabels(labels, fontsize=8)
    ax.set_xlabel(f"Mean {best_metric}")
    ax.set_title(f"Mean {best_metric[:40]} by {group_col}")
    ax.invert_yaxis()
    fig.tight_layout()
    chart_path = out_dir / f"{stem}_grouped_{_safe_stem(group_col)}_chart.png"
    fig.savefig(chart_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    section["chart_path"] = str(chart_path)
    section["chart_metric"] = best_metric

    overall = g[best_metric].mean()
    gmin = plot_means.min()
    gmax = plot_means.max()
    section["summary_metrics"] = {
        "chart_metric": best_metric,
        "overall_mean": round(float(overall), 4) if pd.notna(overall) else None,
        "group_mean_min": round(float(gmin), 4),
        "group_mean_max": round(float(gmax), 4),
    }
    logger.info("Grouped analysis for %s → chart %s", group_col, chart_path)
    return section


def _build_merged_summary(
    corr_block: dict[str, Any],
    grouped_blocks: list[dict[str, Any]],
    *,
    source_label: str,
) -> str:
    lines: list[str] = []
    lines.append(f"Contextual relationship summary ({source_label})")
    lines.append(
        "This report was produced automatically from the merged dataset "
        "(no manual SQL or user prompts)."
    )
    lines.append("")

    tops = corr_block.get("top_abs_correlations") or []
    if tops:
        lines.append("Strongest linear relationships (Pearson |r|) among numeric fields:")
        for p in tops[:5]:
            lines.append(
                f"  • {p['column_a']} vs {p['column_b']}: r = {p['pearson_r']}"
            )
    else:
        lines.append("Correlations: not enough numeric columns or variation to summarize.")

    lines.append("")
    for gb in grouped_blocks:
        gcol = gb.get("group_by")
        sm = gb.get("summary_metrics") or {}
        if sm.get("chart_metric"):
            lines.append(
                f"Grouped context ({gcol}): mean {sm['chart_metric']} ranges "
                f"from {sm.get('group_mean_min')} to {sm.get('group_mean_max')} "
                f"across groups (overall mean {sm.get('overall_mean')})."
            )
        elif gb.get("note"):
            lines.append(f"Grouped context ({gcol}): {gb['note']}")

    lines.append("")
    lines.append(
        "Artifacts: JSON report, correlation heatmap PNG, grouped aggregate CSVs, "
        "and grouped bar charts under data/analysis/."
    )
    return "\n".join(lines)


def run_contextual_relationship_analysis(
    df: pd.DataFrame,
    *,
    output_stem: str,
    output_dir: Optional[Path] = None,
    source_description: str = "merged dataset",
) -> dict[str, Any]:
    """Run the full autonomous analysis pipeline and write outputs."""
    out_dir = Path(output_dir or ANALYSIS_OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = _safe_stem(output_stem)

    report: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": source_description,
        "row_count": len(df),
        "column_count": len(df.columns),
        "analysis_method": "pandas_groupby_and_correlation_no_sql",
    }

    if df.empty:
        report["status"] = "skipped_empty"
        report["merged_summary"] = "No rows to analyze."
        path = out_dir / f"{stem}_relationship_report.json"
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, ensure_ascii=False)
        return report

    profiler = SchemaProfiler(df)
    profile = profiler.generate_profile()
    report["schema_snapshot"] = {
        "time_columns": profile.get("time_columns"),
        "geo_columns": profile.get("geo_columns"),
        "column_roles": profile.get("column_roles"),
    }

    numeric_cols = _numeric_columns(df, profile)
    group_cols = _group_column_candidates(df, profile)
    report["numeric_columns_selected"] = numeric_cols
    report["group_columns_selected"] = group_cols

    corr_block = _correlation_section(df, numeric_cols, out_dir, stem)
    report["correlational_analysis"] = corr_block

    grouped_blocks: list[dict[str, Any]] = []
    for gc in group_cols:
        grouped_blocks.append(
            _grouped_section(df, gc, numeric_cols, out_dir, stem),
        )
    report["grouped_contextual_analyses"] = grouped_blocks

    summary = _build_merged_summary(
        corr_block,
        grouped_blocks,
        source_label=source_description,
    )
    report["merged_summary"] = summary
    report["status"] = "success"

    json_path = out_dir / f"{stem}_relationship_report.json"
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)
    summary_path = out_dir / f"{stem}_merged_summary.txt"
    with open(summary_path, "w", encoding="utf-8") as fh:
        fh.write(summary)
    logger.info("Relationship report: %s", json_path)
    logger.info("Merged summary: %s", summary_path)
    return report


__all__ = ["run_contextual_relationship_analysis", "safe_output_stem"]
