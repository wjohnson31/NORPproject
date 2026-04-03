"""
Analysis sub-package (W7–W8)
============================

Contextual relationship analysis: correlations, grouped aggregates, simple
plots, and merged text summaries — without SQL or interactive user queries.
"""

from data_pipeline.analysis.contextual_relationship_agent import (
    run_contextual_relationship_analysis,
    safe_output_stem,
)

__all__ = ["run_contextual_relationship_analysis", "safe_output_stem"]
