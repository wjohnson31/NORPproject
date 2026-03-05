"""
Main Entry Script
=================

CLI entry point that orchestrates the full ingestion + cleaning pipeline:

    1. Parse command-line arguments (file path, dataset name, optional --no-clean).
    2. Load the raw dataset via :class:`DatasetLoader`.
    3. Generate schema profile (dataset_profile) via :class:`SchemaProfiler`.
    4. Optionally run cleaning: call Claude cleaning agent, execute code safely,
       log transformations, write cleaned dataset to data/cleaned/.
    5. Register the dataset via :class:`DatasetRegistry` (with cleaned/log paths if applicable).
    6. Save dataset_profile JSON to ``data/processed/{name}_profile.json``.
    7. Save transformation log to ``data/processed/{name}_transform_log.json`` when cleaning ran.

Usage::

    python -m data_pipeline --file data/raw/irs_990_2020.csv --name irs_990_2020
    python -m data_pipeline --file data/raw/irs_990_2020.csv --name irs_990_2020 --no-clean

Exit codes:
    0  — success
    1  — error during ingestion
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from data_pipeline.config import (
    CLEANED_DATA_DIR,
    PROCESSED_DATA_DIR,
    setup_logging,
)
from data_pipeline.ingestion.loader import DatasetLoader
from data_pipeline.ingestion.schema import SchemaProfiler
from data_pipeline.ingestion.registry import DatasetRegistry
from data_pipeline.cleaning.agent import CleaningAgent
from data_pipeline.cleaning.executor import SafeCleaningExecutor
from data_pipeline.cleaning.transform_log import TransformationLog

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
    parser.add_argument(
        "--no-clean",
        action="store_true",
        help="Skip the cleaning step (ingest + profile + register only).",
    )
    return parser.parse_args(argv)


def ingest(
    file_path: str,
    dataset_name: str,
    *,
    run_cleaning: bool = True,
) -> None:
    """Run the full ingestion + cleaning pipeline for a single dataset.

    This function is separated from ``main()`` so it can be called
    programmatically in tests or notebooks without touching ``sys.argv``.

    Parameters
    ----------
    file_path : str
        Path to the raw data file.
    dataset_name : str
        Human-readable identifier for the dataset.
    run_cleaning : bool, optional
        If True (default), run the cleaning agent and save cleaned output
        when ANTHROPIC_API_KEY is set. If False, only ingest and profile.

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

    # ---- Step 2: Generate schema profile (dataset_profile) ------------
    profiler = SchemaProfiler(df)
    profile = profiler.generate_profile()

    cleaned_file_path: Path | None = None
    transform_log_path: Path | None = None
    cleaning_succeeded = False
    if run_cleaning and os.environ.get("OPENAI_API_KEY"):
        # ---- Step 3a: Cleaning agent + safe execution + logging -------
        tlog = TransformationLog(dataset_name=dataset_name)
        tlog.start_run()
        agent = CleaningAgent()
        code = agent.generate_cleaning_code(profile, df, dataset_name=dataset_name)
        if code:
            executor = SafeCleaningExecutor()
            rows_before, cols_before = len(df), len(df.columns)
            cleaned_df, err = executor.execute(df, code)
            if err:
                tlog.append_step(
                    step_id="llm_cleaning",
                    description="OpenAI-generated cleaning code",
                    code_snippet=code,
                    rows_before=rows_before,
                    rows_after=len(df),
                    cols_before=cols_before,
                    cols_after=len(df.columns),
                    status="error",
                    error_message=err,
                )
                logger.warning("Cleaning failed; keeping raw data for output. %s", err)
            else:
                tlog.append_step(
                    step_id="llm_cleaning",
                    description="OpenAI-generated cleaning code",
                    code_snippet=code,
                    rows_before=rows_before,
                    rows_after=len(cleaned_df),
                    cols_before=cols_before,
                    cols_after=len(cleaned_df.columns),
                    status="success",
                )
                df = cleaned_df
                cleaning_succeeded = True
        else:
            logger.warning("No cleaning code from agent; skipping cleaning step.")
        tlog_path = tlog.save(PROCESSED_DATA_DIR)
        transform_log_path = tlog_path
        if cleaning_succeeded:
            CLEANED_DATA_DIR.mkdir(parents=True, exist_ok=True)
            out_name = f"{dataset_name}_cleaned.csv"
            cleaned_path = CLEANED_DATA_DIR / out_name
            df.to_csv(cleaned_path, index=False, encoding="utf-8")
            cleaned_file_path = cleaned_path
            logger.info("Cleaned dataset saved to: %s", cleaned_path)
    elif run_cleaning and not os.environ.get("OPENAI_API_KEY"):
        logger.info("OPENAI_API_KEY not set; skipping cleaning step.")

    # ---- Step 4: Register dataset ------------------------------------
    registry = DatasetRegistry()
    registry.register(
        dataset_name=dataset_name,
        file_path=file_path,
        profile=profile,
        cleaned_file_path=cleaned_file_path,
        transform_log_path=transform_log_path,
    )

    # ---- Step 5: Save dataset_profile JSON ---------------------------
    profile_path = PROCESSED_DATA_DIR / f"{dataset_name}_profile.json"
    with open(profile_path, "w", encoding="utf-8") as fh:
        json.dump(profile, fh, indent=2, ensure_ascii=False)

    logger.info("dataset_profile saved to: %s", profile_path)

    # ---- Summary -----------------------------------------------------
    logger.info("-" * 60)
    logger.info("Ingestion complete for '%s'", dataset_name)
    logger.info("  Rows       : %d", profile["num_rows"])
    logger.info("  Columns    : %d", profile["num_columns"])
    logger.info("  Time cols  : %s", profile["time_columns"])
    logger.info("  Geo cols   : %s", profile["geo_columns"])
    if cleaned_file_path:
        logger.info("  Cleaned    : %s", cleaned_file_path)
    if transform_log_path:
        logger.info("  Transforms : %s", transform_log_path)
    logger.info("-" * 60)


def main() -> None:
    """CLI entry point."""
    setup_logging()

    args = parse_args()

    try:
        ingest(
            file_path=args.file,
            dataset_name=args.name,
            run_cleaning=not args.no_clean,
        )
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        logger.error("Ingestion failed: %s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.exception("Unexpected error during ingestion: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
