"""
Run contextual relationship analysis on a CSV.

Usage::

    python -m data_pipeline.analysis path/to/merged.csv [--stem my_run]
"""

import argparse
import logging
import sys
from pathlib import Path

from data_pipeline.config import setup_logging
from data_pipeline.ingestion.loader import DatasetLoader
from data_pipeline.analysis.contextual_relationship_agent import (
    run_contextual_relationship_analysis,
    safe_output_stem,
)

logger = logging.getLogger(__name__)


def main() -> None:
    setup_logging()
    parser = argparse.ArgumentParser(
        description="Autonomous correlational and grouped analysis (no SQL).",
    )
    parser.add_argument(
        "csv_path",
        type=str,
        help="Path to a CSV (e.g. data/merged/primary_context_merged.csv).",
    )
    parser.add_argument(
        "--stem",
        type=str,
        default=None,
        help="Output file stem (default: derived from filename).",
    )
    args = parser.parse_args()
    path = Path(args.csv_path)
    if not path.is_file():
        logger.error("File not found: %s", path)
        sys.exit(1)
    stem = safe_output_stem(args.stem) if args.stem else safe_output_stem(path.stem)
    df = DatasetLoader(str(path)).load()
    run_contextual_relationship_analysis(
        df,
        output_stem=stem,
        source_description=f"file:{path.name}",
    )
    logger.info("Analysis complete (stem=%s)", stem)


if __name__ == "__main__":
    main()
