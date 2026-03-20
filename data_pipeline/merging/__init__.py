"""
Merging Sub-package
===================

Multi-dataset merge engine: join key detection, key normalization,
and controlled merging with validation.

    - key_normalizer : Standardize join key values (states, years, strings)
    - join_detector  : Compare profiles to find compatible join keys
    - join_agent     : LLM-based join key detection (OpenAI fallback)
    - merge_engine   : Controlled merge with post-merge validation
"""

from data_pipeline.merging.key_normalizer import KeyNormalizer
from data_pipeline.merging.join_detector import JoinDetector
from data_pipeline.merging.join_agent import JoinAgent
from data_pipeline.merging.merge_engine import MergeEngine

__all__ = ["KeyNormalizer", "JoinDetector", "JoinAgent", "MergeEngine"]
