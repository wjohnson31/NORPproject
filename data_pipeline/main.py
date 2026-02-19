"""
Main Entry Script
=================

CLI entry point that orchestrates the full ingestion pipeline:

    1. Parse command-line arguments (file path + dataset name).
    2. Load the raw dataset via :class:`DatasetLoader`.
    3. Generate a schema profile via :class:`SchemaProfiler`.
    4. Register the dataset via :class:`DatasetRegistry`.
    5. Save the schema profile JSON to ``/data/processed/``.

Usage::

    python -m data_pipeline.main --file data/raw/irs_990_2020.csv --name irs_990_2020

Or equivalently::

    python data_pipeline/main.py --file data/raw/irs_990_2020.csv --name irs_990_2020

Exit codes:
    0  — success
    1  — error during ingestion
"""

import argparse
import json
import logging
import sys
from pathlib import Path

from data_pipeline.config import PROCESSED_DATA_DIR, setup_logging
from data_pipeline.ingestion.loader import DatasetLoader
from data_pipeline.ingestion.schema import SchemaProfiler
from data_pipeline.ingestion.registry import DatasetRegistry

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Parameters
    ----------
    argv : list[str], optional
        Argument list (defaults to ``sys.argv[1:]``).

    Returns
    -------
    argparse.Namespace
        Parsed arguments with ``file`` and ``name`` attributes.
    """
    parser = argparse.ArgumentParser(
        prog="norp-ingest",
        description="Ingest a raw dataset into the NORP data pipeline.",
    )
    parser.add_argument(
        "--file", "-f",
        required=True,
        type=str,
        help="Path to the raw data file (CSV, Excel, or JSON).",
    )
    parser.add_argument(
        "--name", "-n",
        required=True,
        type=str,
        help=(
            "A short, descriptive name for the dataset "
            "(e.g., 'irs_990_2020')."
        ),
    )
    return parser.parse_args(argv)


def ingest(file_path: str, dataset_name: str) -> None:
    """Run the full ingestion pipeline for a single dataset.

    This function is separated from ``main()`` so it can be called
    programmatically in tests or notebooks without touching ``sys.argv``.

    Parameters
    ----------
    file_path : str
        Path to the raw data file.
    dataset_name : str
        Human-readable identifier for the dataset.

    Raises
    ------
    FileNotFoundError
        If the source file does not exist.
    ValueError
        If the file type is unsupported.
    RuntimeError
        If loading fails after all encoding attempts.
    """
    logger.info("=" * 60)
    logger.info("Starting ingestion: %s", dataset_name)
    logger.info("Source file: %s", file_path)
    logger.info("=" * 60)

    # ---- Step 1: Load dataset ----------------------------------------
    loader = DatasetLoader(file_path)
    df = loader.load()

    # ---- Step 2: Generate schema profile -----------------------------
    profiler = SchemaProfiler(df)
    profile = profiler.generate_profile()

    # ---- Step 3: Register dataset ------------------------------------
    registry = DatasetRegistry()
    registry.register(
        dataset_name=dataset_name,
        file_path=file_path,
        profile=profile,
    )

    # ---- Step 4: Save profile JSON -----------------------------------
    profile_path = PROCESSED_DATA_DIR / f"{dataset_name}_profile.json"
    with open(profile_path, "w", encoding="utf-8") as fh:
        json.dump(profile, fh, indent=2, ensure_ascii=False)

    logger.info("Schema profile saved to: %s", profile_path)

    # ---- Summary -----------------------------------------------------
    logger.info("-" * 60)
    logger.info("Ingestion complete for '%s'", dataset_name)
    logger.info("  Rows       : %d", profile["num_rows"])
    logger.info("  Columns    : %d", profile["num_columns"])
    logger.info("  Time cols  : %s", profile["time_columns"])
    logger.info("  Geo cols   : %s", profile["geo_columns"])
    logger.info("-" * 60)


def main() -> None:
    """CLI entry point."""
    setup_logging()

    args = parse_args()

    try:
        ingest(file_path=args.file, dataset_name=args.name)
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        logger.error("Ingestion failed: %s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.exception("Unexpected error during ingestion: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
