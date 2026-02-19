"""
NORP Data Pipeline
==================

A modular data ingestion infrastructure for nonprofit financial research.

This package provides the foundational skeleton for loading, profiling,
and registering nonprofit financial datasets (e.g., IRS 990 extracts)
and external contextual datasets (e.g., state-level unemployment).

Architecture:
    - ingestion/loader.py   : Raw file loading + column normalization
    - ingestion/schema.py   : Schema extraction + metadata profiling
    - ingestion/registry.py : Dataset registration + JSON persistence
    - config.py             : Centralized path and logging configuration
    - main.py               : CLI entry point orchestrating the pipeline

Future phases will add:
    - Cleaning & harmonization (W3)
    - Join compatibility detection (W3–W4)
    - Autonomous contextual discovery (W4+)
"""

__version__ = "0.1.0"
