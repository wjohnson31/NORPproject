"""
Main Entry Script
=================

CLI entry point that orchestrates the full ingestion + cleaning + merge pipeline:

    1. Parse command-line arguments (file path, dataset name, optional flags).
    2. Load the raw dataset via :class:`DatasetLoader`.
    3. Generate schema profile (dataset_profile) via :class:`SchemaProfiler`.
    4. Optionally run cleaning: call OpenAI cleaning agent, execute code safely,
       log transformations, write cleaned dataset to data/cleaned/.
    5. Register the dataset via :class:`DatasetRegistry`.
    6. Save dataset_profile JSON to ``data/processed/{name}_profile.json``.
    7. If ``--merge-with`` is given, detect join keys with the target dataset,
       normalize keys, merge, validate, and save the result.

Usage::

    python -m data_pipeline --file data/raw/irs_990_2020.csv --name irs_990_2020
    python -m data_pipeline --file data/raw/irs_990_2020.csv --name irs_990_2020 --no-clean
    python -m data_pipeline --file data/raw/unemp.csv --name unemp --merge-with irs_990_2020

Exit codes:
    0  — success
    1  — error during ingestion or merge
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from data_pipeline.config import (
    CLEANED_DATA_DIR,
    MERGED_DATA_DIR,
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
        Parsed arguments with ``file``, ``name``, and optional merge attributes.
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
    parser.add_argument(
        "--merge-with",
        type=str,
        default=None,
        metavar="DATASET",
        help=(
            "Name of an already-registered dataset to merge with after "
            "ingestion. The current dataset is treated as primary by default."
        ),
    )
    parser.add_argument(
        "--as-context",
        action="store_true",
        help=(
            "When merging, treat the newly ingested dataset as context "
            "(the --merge-with dataset becomes primary)."
        ),
    )
    return parser.parse_args(argv)


def ingest(
    file_path: str,
    dataset_name: str,
    *,
    run_cleaning: bool = True,
    merge_with: str | None = None,
    as_context: bool = False,
) -> None:
    """Run the full ingestion + cleaning + merge pipeline for a single dataset.

    This function is separated from ``main()`` so it can be called
    programmatically in tests or notebooks without touching ``sys.argv``.

    Parameters
    ----------
    file_path : str
        Path to the raw data file.
    dataset_name : str
        Human-readable identifier for the dataset.
    run_cleaning : bool, optional
        If True (default), run the cleaning agent when OPENAI_API_KEY is set.
    merge_with : str, optional
        Name of a registered dataset to merge with after ingestion.
    as_context : bool, optional
        If True, the newly ingested dataset is context (merge_with is primary).

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

    # ---- Step 6: Merge (if --merge-with) -----------------------------
    if merge_with:
        _run_merge(
            registry=registry,
            current_name=dataset_name,
            current_df=df,
            current_profile=profile,
            merge_with=merge_with,
            as_context=as_context,
        )


def _run_merge(
    *,
    registry: DatasetRegistry,
    current_name: str,
    current_df,
    current_profile: dict,
    merge_with: str,
    as_context: bool,
) -> None:
    """Execute the merge step after ingestion.

    Loads the target dataset from the registry, detects join keys,
    normalizes them, merges, validates, and saves results.
    """
    import pandas as pd  # noqa: F811 — used by DatasetLoader internally
    from data_pipeline.merging.join_detector import JoinDetector
    from data_pipeline.merging.merge_engine import MergeEngine

    logger.info("=" * 60)
    logger.info("MERGE STEP: merging '%s' with '%s'", current_name, merge_with)
    logger.info("=" * 60)

    # --- Load target dataset from registry ----------------------------
    target_entry = registry.get_dataset(merge_with)
    if target_entry is None:
        logger.error(
            "Cannot merge: dataset '%s' not found in registry. "
            "Available datasets: %s",
            merge_with,
            [d["name"] for d in registry.list_datasets()],
        )
        return

    # Prefer cleaned version, fall back to raw
    target_path = target_entry.get("cleaned_file_path") or target_entry["file_path"]
    logger.info("Loading target dataset from: %s", target_path)
    try:
        # Use DatasetLoader for consistent column normalization
        target_df = DatasetLoader(target_path).load()
    except Exception as exc:
        logger.error("Failed to load target dataset: %s", exc)
        return

    target_profile = target_entry.get("schema_profile", {})

    # --- Determine primary vs context ---------------------------------
    if as_context:
        primary_name, context_name = merge_with, current_name
        primary_df, context_df = target_df, current_df
        primary_profile, context_profile = target_profile, current_profile
    else:
        primary_name, context_name = current_name, merge_with
        primary_df, context_df = current_df, target_df
        primary_profile, context_profile = current_profile, target_profile

    logger.info("Primary dataset: '%s' (%d rows)", primary_name, len(primary_df))
    logger.info("Context dataset: '%s' (%d rows)", context_name, len(context_df))

    # --- Detect join keys ---------------------------------------------
    detector = JoinDetector()
    join_keys = detector.detect_join_keys(
        primary_profile, context_profile, primary_df, context_df,
    )

    if not join_keys:
        logger.warning(
            "No compatible join keys found between '%s' and '%s'. "
            "Skipping merge.",
            primary_name, context_name,
        )
        return

    # --- Merge --------------------------------------------------------
    engine = MergeEngine()
    result = engine.merge(
        primary_df, context_df, join_keys,
        primary_name=primary_name, context_name=context_name,
    )

    if not result.success:
        logger.error("Merge failed: %s", result.error)
        return

    # --- Save outputs -------------------------------------------------
    merged_path = engine.save_merged(
        result.merged_df, MERGED_DATA_DIR, primary_name, context_name,
    )
    report_path = engine.save_report(
        result.report, PROCESSED_DATA_DIR, primary_name, context_name,
    )

    # --- Final summary ------------------------------------------------
    logger.info("-" * 60)
    logger.info("Merge complete: '%s' + '%s'", primary_name, context_name)
    logger.info("  Merged rows : %d", len(result.merged_df))
    logger.info("  Merged cols : %d", len(result.merged_df.columns))
    logger.info("  Key coverage: %s", result.report.get("key_coverage", {}))
    logger.info("  Saved to    : %s", merged_path)
    logger.info("  Report      : %s", report_path)
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
            merge_with=args.merge_with,
            as_context=args.as_context,
        )
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        logger.error("Ingestion failed: %s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.exception("Unexpected error during ingestion: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
