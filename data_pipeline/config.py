"""
Configuration Module
====================

Centralizes all path definitions and logging setup for the data pipeline.

Design decisions:
    - Paths are resolved relative to the project root so the pipeline works
      regardless of the working directory at invocation time.
    - Logging is configured once at import time via ``setup_logging()``.
      All downstream modules import their loggers with
      ``logging.getLogger(__name__)`` and inherit this configuration.
    - Data directories are created eagerly on import so that downstream
      code never has to worry about missing folders.
"""

import logging
import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Load .env from project root (so ANTHROPIC_API_KEY etc. are available)
# ---------------------------------------------------------------------------

_project_root = Path(__file__).resolve().parent.parent
_env_file = _project_root / ".env"
if _env_file.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(_env_file)
    except ImportError:
        pass  # python-dotenv not installed; rely on system env only

# ---------------------------------------------------------------------------
# Path configuration
# ---------------------------------------------------------------------------

# Project root is two levels up from this file:
#   <project_root>/data_pipeline/config.py  →  <project_root>
PROJECT_ROOT: Path = _project_root

# Raw input data lives here.  Users should drop CSVs / Excel / JSON files
# into this directory before running the pipeline.
RAW_DATA_DIR: Path = PROJECT_ROOT / "data" / "raw"

# Processed outputs (schema profiles, registry, transformation logs) are written here.
PROCESSED_DATA_DIR: Path = PROJECT_ROOT / "data" / "processed"

# Cleaned datasets (output of the cleaning pipeline) are written here.
CLEANED_DATA_DIR: Path = PROJECT_ROOT / "data" / "cleaned"

# Merged datasets (output of the merge engine) are written here.
MERGED_DATA_DIR: Path = PROJECT_ROOT / "data" / "merged"

# The dataset registry is a single JSON file that accumulates metadata
# about every dataset that has been ingested.
REGISTRY_PATH: Path = PROCESSED_DATA_DIR / "registry.json"

# ---------------------------------------------------------------------------
# Ensure data directories exist
# ---------------------------------------------------------------------------

RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
CLEANED_DATA_DIR.mkdir(parents=True, exist_ok=True)
MERGED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------

LOG_FORMAT: str = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
LOG_DATE_FORMAT: str = "%Y-%m-%d %H:%M:%S"
LOG_LEVEL: int = logging.INFO


def setup_logging() -> None:
    """Configure the root logger for the entire pipeline.

    Call this once at application startup (``main.py``).  All modules that
    use ``logging.getLogger(__name__)`` will inherit this configuration.

    The handler writes to *stdout* so that output is visible in both
    interactive and CI/CD environments.
    """
    root_logger = logging.getLogger()

    # Avoid adding duplicate handlers if called more than once.
    if not root_logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT))
        root_logger.addHandler(handler)

    root_logger.setLevel(LOG_LEVEL)
