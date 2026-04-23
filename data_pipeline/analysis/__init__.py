"""
Analysis sub-package (W7–W10)
==============================

* W7–W8: Contextual relationship analysis — correlations, grouped aggregates,
  simple plots, and merged text summaries (no SQL / no user prompts).
* W9–W10: Interpretive layer — heuristic ranking, trivial-relationship
  filtering, scatterplots + explanations for top-N findings, and a visible
  log of dropped weak relationships.
"""

from data_pipeline.analysis.llm_hypothesis_agent import LLMHypothesisAgent

__all__ = [
    "LLMHypothesisAgent",
]
