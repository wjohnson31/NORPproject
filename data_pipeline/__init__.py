"""
NORP Data Pipeline
==================

A modular data ingestion infrastructure for nonprofit financial research.

This package provides the foundational skeleton for loading, profiling,
and registering nonprofit financial datasets (e.g., IRS 990 extracts)
and external contextual datasets (e.g., state-level unemployment).

Architecture:
    - ingestion/loader.py   : Raw file loading + column normalization
    - ingestion/schema.py   : Schema extraction + dataset_profile generation
    - ingestion/registry.py : Dataset registration + JSON persistence
    - cleaning/agent.py     : OpenAI-backed cleaning code generation
    - cleaning/executor.py  : Safe execution of cleaning code (restricted globals)
    - cleaning/transform_log.py : Transformation logging and JSON persistence
    - merging/key_normalizer.py : Join key value normalization (states, years)
    - merging/join_detector.py  : Cross-dataset join key detection
    - merging/merge_engine.py   : Controlled merge with validation
    - config.py             : Centralized path and logging configuration
    - main.py               : CLI entry point (ingest + clean + merge pipeline)

Future phases will add:
    - Contextual Relationship Query Agent (W7–W8)
    - Interpretive Layer + Query Expansion (W9–W10)
"""

__version__ = "0.1.0"
