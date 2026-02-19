"""
Ingestion Sub-package
=====================

Contains the three core ingestion modules:

    - loader   : File I/O and column normalization
    - schema   : Schema extraction and metadata profiling
    - registry : Dataset registration and persistence
"""

from data_pipeline.ingestion.loader import DatasetLoader
from data_pipeline.ingestion.schema import SchemaProfiler
from data_pipeline.ingestion.registry import DatasetRegistry

__all__ = ["DatasetLoader", "SchemaProfiler", "DatasetRegistry"]
