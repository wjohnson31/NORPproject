"""
Transformation Log
==================

Records each cleaning step (or full run) with before/after stats and optional
code snippet. Persisted as JSON for audit and reproducibility.

Architectural notes:
    - Log entries are append-only; the log is the source of truth for what
      transformations were applied and in what order.
    - Stored under PROCESSED_DATA_DIR as ``{dataset_name}_transform_log.json``.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


class TransformationLog:
    """In-memory log of cleaning steps with JSON persistence.

    Each entry records: step_id, timestamp, description, optional code_snippet,
    rows/cols before and after, and status (e.g. success, error, skipped).

    Usage::

        log = TransformationLog(dataset_name="irs_990_2020")
        log.append_step(
            step_id="clean_1",
            description="Drop duplicate EIN+year",
            code_snippet="df = df.drop_duplicates(subset=['ein','tax_year'])",
            rows_before=1000, rows_after=950,
            cols_before=9, cols_after=9,
            status="success",
        )
        log.save(processed_dir)
    """

    def __init__(self, dataset_name: str) -> None:
        self.dataset_name = dataset_name
        self.entries: list[dict[str, Any]] = []
        self._run_started_at: Optional[str] = None

    def start_run(self) -> None:
        """Mark the start of a cleaning run (single timestamp for the whole run)."""
        self._run_started_at = datetime.now(timezone.utc).isoformat()
        logger.info("Transformation log run started at %s", self._run_started_at)

    def append_step(
        self,
        step_id: str,
        description: str,
        *,
        code_snippet: Optional[str] = None,
        rows_before: Optional[int] = None,
        rows_after: Optional[int] = None,
        cols_before: Optional[int] = None,
        cols_after: Optional[int] = None,
        status: str = "success",
        error_message: Optional[str] = None,
    ) -> None:
        """Append one transformation step to the log."""
        entry: dict[str, Any] = {
            "step_id": step_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "description": description,
            "status": status,
        }
        if code_snippet is not None:
            entry["code_snippet"] = code_snippet
        if rows_before is not None:
            entry["rows_before"] = rows_before
        if rows_after is not None:
            entry["rows_after"] = rows_after
        if cols_before is not None:
            entry["cols_before"] = cols_before
        if cols_after is not None:
            entry["cols_after"] = cols_after
        if error_message is not None:
            entry["error_message"] = error_message

        self.entries.append(entry)
        logger.info(
            "Transform step '%s' logged — %s (rows %s → %s)",
            step_id,
            status,
            rows_before,
            rows_after,
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable dict of the full log."""
        return {
            "dataset_name": self.dataset_name,
            "run_started_at": self._run_started_at,
            "run_finished_at": datetime.now(timezone.utc).isoformat(),
            "num_steps": len(self.entries),
            "entries": self.entries,
        }

    def save(self, processed_dir: Path) -> Path:
        """Write the log to ``{processed_dir}/{dataset_name}_transform_log.json``."""
        processed_dir = Path(processed_dir)
        processed_dir.mkdir(parents=True, exist_ok=True)
        path = processed_dir / f"{self.dataset_name}_transform_log.json"
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(self.to_dict(), fh, indent=2, ensure_ascii=False)
        logger.info("Transformation log saved to %s", path)
        return path

    @classmethod
    def load(cls, path: Path) -> "TransformationLog":
        """Load a transformation log from JSON. Entries are preserved; new steps can be appended."""
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        log = cls(dataset_name=data["dataset_name"])
        log._run_started_at = data.get("run_started_at")
        log.entries = data.get("entries", [])
        return log
