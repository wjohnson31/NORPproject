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
    - config.py             : Centralized path and logging configuration
    - main.py               : CLI entry point (ingest + optional cleaning pipeline)

Future phases will add:
    - Harmonization (W5–W6)
    - Join compatibility detection (W5–W6)
    - Autonomous contextual discovery (W7+)
"""

__version__ = "0.1.0"
