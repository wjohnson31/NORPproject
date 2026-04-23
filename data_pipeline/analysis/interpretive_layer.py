"""
Interpretive Layer + Robust Query Expansion (W9–W10)
=====================================================

Takes the raw correlational analysis output from the W7–W8 Contextual
Relationship Agent and applies heuristic ranking, filtering of trivial
relationships, and generates polished top-N findings with scatterplots
and plain-English explanations.

Key behaviours:
    - Ranks correlation pairs on a composite score (|r| weight + sample
      size bonus + cross-domain bonus).
    - Filters out trivial / weak relationships below configurable |r|
      thresholds, logging every dropped pair so the user can see what
      was ruled out and why.
    - Generates a scatterplot + linear-fit overlay for each top-N finding.
    - Produces a plain-text interpretive summary with explanations.
    - Outputs a JSON report with rankings, dropped list, and file paths.
"""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from data_pipeline.config import ANALYSIS_OUTPUT_DIR
from data_pipeline.analysis.contextual_relationship_agent import _safe_stem

logger = logging.getLogger(__name__)

# ── Configurable thresholds ──────────────────────────────────────────────
_WEAK_R_THRESHOLD = 0.25          # |r| below this → always dropped as trivial
_MODERATE_R_THRESHOLD = 0.40      # |r| below this → flagged as borderline
_TOP_N_FINDINGS = 8               # max findings to present
_MIN_OBSERVATIONS_FOR_BONUS = 30  # bonus for having enough data points


def _classify_strength(abs_r: float) -> str:
    """Human-readable classification of correlation strength."""
    if abs_r >= 0.8:
        return "very strong"
    if abs_r >= 0.6:
        return "strong"
    if abs_r >= 0.4:
        return "moderate"
    if abs_r >= _WEAK_R_THRESHOLD:
        return "weak"
    return "trivial"


def _is_cross_domain(col_a: str, col_b: str, profile: dict[str, Any]) -> bool:
    """Check whether two columns come from different semantic domains.

    Returns True when one column belongs to the primary dataset's metrics
    and the other to the context dataset's, hinting at a genuinely
    interesting cross-dataset relationship rather than a within-dataset
    tautology.
    """
    roles = profile.get("column_roles") or {}
    geo = set(profile.get("geo_columns") or [])
    time_c = set(profile.get("time_columns") or [])

    # Simple heuristic: if neither column is a join key and the two
    # columns have different "origin" prefixes we treat them as cross-
    # domain.  With small merged datasets we approximate by checking
    # whether at least one column name looks like it came from context
    # (common patterns: _ctx suffix, or external socioeconomic names).
    external_hints = {
        "unemployment", "labor", "income", "poverty", "gdp",
        "population", "crime", "disaster", "internet",
    }
    a_ext = any(h in col_a.lower() for h in external_hints)
    b_ext = any(h in col_b.lower() for h in external_hints)
    if a_ext != b_ext:
        return True  # one external, one internal → cross-domain
    return False


def _compute_score(
    abs_r: float,
    n_obs: int,
    cross_domain: bool,
) -> float:
    """Composite ranking score.

    Components:
        - base   = |r|  (0–1)
        - sample = log2(n_obs) / 20   (diminishing returns for large n)
        - cross  = +0.15 if cross-domain pair
    """
    base = abs_r
    sample_bonus = min(math.log2(max(n_obs, 2)) / 20.0, 0.25)
    cross_bonus = 0.15 if cross_domain else 0.0
    return round(base + sample_bonus + cross_bonus, 6)


def _generate_scatter(
    df: pd.DataFrame,
    col_a: str,
    col_b: str,
    r_val: float,
    rank: int,
    out_dir: Path,
    stem: str,
) -> Path:
    """Create a scatterplot with linear fit for a single finding."""
    sub = df[[col_a, col_b]].dropna()
    x = sub[col_a].astype(float)
    y = sub[col_b].astype(float)

    fig, ax = plt.subplots(figsize=(7, 5))
    ax.scatter(x, y, alpha=0.7, s=50, color="steelblue", edgecolors="white", linewidths=0.5)

    # Linear fit line
    if len(x) >= 2:
        coeffs = np.polyfit(x, y, 1)
        xs = np.linspace(x.min(), x.max(), 100)
        ax.plot(xs, np.polyval(coeffs, xs), color="tomato", linewidth=2, label=f"r = {r_val:.3f}")
        ax.legend(loc="best", fontsize=9)

    ax.set_xlabel(col_a, fontsize=10)
    ax.set_ylabel(col_b, fontsize=10)
    strength = _classify_strength(abs(r_val))
    ax.set_title(f"#{rank}: {col_a} vs {col_b}  ({strength}, r={r_val:.3f})", fontsize=11)
    fig.tight_layout()

    safe_a = _safe_stem(col_a)
    safe_b = _safe_stem(col_b)
    path = out_dir / f"{stem}_finding_{rank}_{safe_a}_vs_{safe_b}.png"
    fig.savefig(path, dpi=130, bbox_inches="tight")
    plt.close(fig)
    logger.info("  Scatter saved: %s", path)
    return path


def _explain_finding(
    col_a: str, col_b: str, r_val: float, score: float,
    n_obs: int, cross_domain: bool, rank: int,
) -> str:
    """Generate a plain-English interpretation of a finding."""
    direction = "positive" if r_val > 0 else "negative"
    strength = _classify_strength(abs(r_val))
    domain_note = (
        "This pairing links primary-dataset and context-dataset variables, "
        "suggesting a potentially meaningful cross-domain relationship."
        if cross_domain
        else "Both variables appear to originate from the same dataset domain."
    )
    return (
        f"Finding #{rank}: {col_a} vs {col_b}\n"
        f"  Pearson r = {r_val:.4f} ({strength} {direction} correlation)\n"
        f"  Based on {n_obs} overlapping observations.\n"
        f"  Composite ranking score = {score:.4f}\n"
        f"  {domain_note}"
    )


def run_interpretive_layer(
    df: pd.DataFrame,
    relationship_report: dict[str, Any],
    *,
    output_stem: str,
    output_dir: Optional[Path] = None,
    top_n: int = _TOP_N_FINDINGS,
    weak_threshold: float = _WEAK_R_THRESHOLD,
) -> dict[str, Any]:
    """Apply the interpretive layer to a relationship analysis report.

    Parameters
    ----------
    df : pd.DataFrame
        The merged dataset used for the analysis.
    relationship_report : dict
        The JSON report from ``run_contextual_relationship_analysis``.
    output_stem : str
        Base filename stem for output artifacts.
    output_dir : Path, optional
        Defaults to ``data/analysis/``.
    top_n : int
        Maximum number of top findings to present.
    weak_threshold : float
        Absolute Pearson r below which pairs are dropped as trivial.

    Returns
    -------
    dict
        The interpretive layer report (also saved as JSON).
    """
    out_dir = Path(output_dir or ANALYSIS_OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = _safe_stem(output_stem)

    logger.info("=" * 60)
    logger.info("INTERPRETIVE LAYER  (W9–W10)")
    logger.info("Weak-|r| threshold: %.2f   Top-N: %d", weak_threshold, top_n)
    logger.info("=" * 60)

    profile = relationship_report.get("schema_snapshot") or {}
    raw_pairs = (
        relationship_report
        .get("correlational_analysis", {})
        .get("top_abs_correlations", [])
    )

    if not raw_pairs:
        logger.warning("No correlation pairs found in report — nothing to rank.")
        result = {
            "status": "skipped_no_pairs",
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        _save_json(result, out_dir / f"{stem}_interpretive_report.json")
        return result

    # ── Rank + filter ────────────────────────────────────────────────────
    scored: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []

    for pair in raw_pairs:
        col_a = pair["column_a"]
        col_b = pair["column_b"]
        r_val = pair["pearson_r"]
        abs_r = abs(r_val)

        # Count overlapping observations
        n_obs = int(df[[col_a, col_b]].dropna().shape[0]) if (
            col_a in df.columns and col_b in df.columns
        ) else 0

        cross = _is_cross_domain(col_a, col_b, profile)
        score = _compute_score(abs_r, n_obs, cross)
        strength = _classify_strength(abs_r)

        entry = {
            "column_a": col_a,
            "column_b": col_b,
            "pearson_r": r_val,
            "abs_r": round(abs_r, 4),
            "strength": strength,
            "n_observations": n_obs,
            "cross_domain": cross,
            "composite_score": score,
        }

        if abs_r < weak_threshold:
            entry["drop_reason"] = (
                f"|r| = {abs_r:.4f} < {weak_threshold} — "
                f"classified as {strength}; insufficient evidence of association"
            )
            dropped.append(entry)
            logger.info(
                "  DROPPED: %s vs %s  (|r|=%.4f, %s)",
                col_a, col_b, abs_r, strength,
            )
        else:
            scored.append(entry)

    # Sort by composite score descending
    scored.sort(key=lambda e: e["composite_score"], reverse=True)
    top_findings = scored[:top_n]

    logger.info(
        "Ranked %d pairs → %d kept, %d dropped as weak/trivial",
        len(raw_pairs), len(scored), len(dropped),
    )

    # ── Generate scatter plots + explanations ────────────────────────────
    explanations: list[str] = []
    for rank_idx, finding in enumerate(top_findings, start=1):
        col_a = finding["column_a"]
        col_b = finding["column_b"]
        r_val = finding["pearson_r"]
        score = finding["composite_score"]
        n_obs = finding["n_observations"]
        cross = finding["cross_domain"]

        chart_path = _generate_scatter(
            df, col_a, col_b, r_val, rank_idx, out_dir, stem,
        )
        finding["scatter_path"] = str(chart_path)
        finding["rank"] = rank_idx

        explanation = _explain_finding(
            col_a, col_b, r_val, score, n_obs, cross, rank_idx,
        )
        finding["explanation"] = explanation
        explanations.append(explanation)

    # ── Build interpretive summary text ──────────────────────────────────
    summary_lines = [
        f"Interpretive Summary — {stem}",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        f"Weak-relationship threshold: |r| < {weak_threshold}",
        "",
        f"Top-{len(top_findings)} ranked correlational findings:",
        "=" * 55,
    ]
    for expl in explanations:
        summary_lines.append(expl)
        summary_lines.append("")

    if dropped:
        summary_lines.append("-" * 55)
        summary_lines.append(
            f"Dropped relationships ({len(dropped)} pair(s) below |r| < {weak_threshold}):",
        )
        for d in dropped:
            summary_lines.append(
                f"  ✕ {d['column_a']} vs {d['column_b']}  "
                f"(r={d['pearson_r']:.4f}, {d['strength']}) — {d['drop_reason']}"
            )
        summary_lines.append("")

    summary_lines.append(
        "Artifacts: scatter PNGs, interpretive JSON report, and this "
        "summary under data/analysis/."
    )
    summary_text = "\n".join(summary_lines)

    # ── Save outputs ─────────────────────────────────────────────────────
    summary_path = out_dir / f"{stem}_interpretive_summary.txt"
    with open(summary_path, "w", encoding="utf-8") as fh:
        fh.write(summary_text)
    logger.info("Interpretive summary: %s", summary_path)

    report: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "weak_r_threshold": weak_threshold,
        "top_n_requested": top_n,
        "total_pairs_evaluated": len(raw_pairs),
        "pairs_kept": len(scored),
        "pairs_dropped": len(dropped),
        "top_findings": top_findings,
        "dropped_relationships": dropped,
        "interpretive_summary_path": str(summary_path),
        "status": "success",
    }
    report_path = out_dir / f"{stem}_interpretive_report.json"
    _save_json(report, report_path)
    logger.info("Interpretive report: %s", report_path)

    return report


def _save_json(data: dict, path: Path) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)


__all__ = ["run_interpretive_layer"]
