"""
Dataset Registry
================

Maintains a persistent JSON registry of all ingested datasets.  Each entry
records the dataset name, source file path, schema profile, load timestamp,
and basic shape information.

Architectural notes:
    - The registry is stored as a single JSON file rather than a database.
      This keeps the dependency footprint at zero and is adequate for the
      current scale.  If the number of registered datasets grows into the
      hundreds, migration to SQLite would be straightforward.
    - Concurrency is NOT handled — this module assumes single-process
      execution.  A file-lock wrapper can be added later if needed.
    - The registry uses dataset *name* as the primary key.  Re-registering
      a name overwrites the previous entry (with a warning).  This allows
      re-ingestion of updated source files without manual cleanup.
    - The ``list_datasets()`` method returns a lightweight summary suitable
      for display or programmatic enumeration.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from data_pipeline.config import REGISTRY_PATH

logger = logging.getLogger(__name__)


class DatasetRegistry:
    """Persistent registry of ingested datasets.

    The registry is backed by a JSON file at ``REGISTRY_PATH``.  Datasets
    are keyed by their user-supplied name.

    Usage::

        registry = DatasetRegistry()
        registry.register(
            dataset_name="irs_990_2020",
            file_path="/data/raw/irs_990_2020.csv",
            profile=schema_profile_dict,
        )
        registry.list_datasets()

    Parameters
    ----------
    registry_path : Path, optional
        Override the default registry file location (useful for testing).
    """

    def __init__(self, registry_path: Optional[Path] = None) -> None:
        self._path: Path = registry_path or REGISTRY_PATH
        self._registry: dict[str, Any] = self._load_registry()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def register(
        self,
        dataset_name: str,
        file_path: str | Path,
        profile: dict[str, Any],
    ) -> None:
        """Register a new dataset or update an existing entry.

        Parameters
        ----------
        dataset_name : str
            A human-readable identifier for the dataset (e.g.,
            ``"irs_990_2020"``).
        file_path : str or Path
            Absolute path to the source file.
        profile : dict
            The schema profile produced by :class:`SchemaProfiler`.
        """
        if dataset_name in self._registry:
            logger.warning(
                "Dataset '%s' already registered — overwriting.", dataset_name
            )

        entry: dict[str, Any] = {
            "file_path": str(Path(file_path).resolve()),
            "schema_profile": profile,
            "num_rows": profile.get("num_rows", 0),
            "num_columns": profile.get("num_columns", 0),
            "registered_at": datetime.now(timezone.utc).isoformat(),
        }

        self._registry[dataset_name] = entry
        self._save_registry()

        logger.info(
            "Registered dataset '%s' — rows=%d, cols=%d",
            dataset_name,
            entry["num_rows"],
            entry["num_columns"],
        )

    def list_datasets(self) -> list[dict[str, Any]]:
        """Return a lightweight summary of all registered datasets.

        Returns
        -------
        list[dict]
            Each dict contains ``name``, ``file_path``, ``num_rows``,
            ``num_columns``, and ``registered_at``.
        """
        summaries: list[dict[str, Any]] = []
        for name, entry in self._registry.items():
            summaries.append(
                {
                    "name": name,
                    "file_path": entry["file_path"],
                    "num_rows": entry["num_rows"],
                    "num_columns": entry["num_columns"],
                    "registered_at": entry["registered_at"],
                }
            )

        logger.info("Registry contains %d dataset(s).", len(summaries))
        return summaries

    def get_dataset(self, dataset_name: str) -> Optional[dict[str, Any]]:
        """Retrieve the full registry entry for a dataset.

        Parameters
        ----------
        dataset_name : str
            The registered name of the dataset.

        Returns
        -------
        dict or None
            The full entry including schema profile, or ``None`` if the
            dataset is not registered.
        """
        entry = self._registry.get(dataset_name)
        if entry is None:
            logger.warning("Dataset '%s' not found in registry.", dataset_name)
        return entry

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    def _load_registry(self) -> dict[str, Any]:
        """Load the registry from disk, or return an empty dict if the
        file does not exist or is corrupt."""
        if not self._path.exists():
            logger.info("No existing registry found — starting fresh.")
            return {}

        try:
            with open(self._path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
                logger.info(
                    "Loaded registry with %d dataset(s).", len(data)
                )
                return data
        except (json.JSONDecodeError, OSError) as exc:
            logger.error(
                "Failed to load registry at %s: %s — starting fresh.",
                self._path,
                exc,
            )
            return {}

    def _save_registry(self) -> None:
        """Persist the registry dict to disk as formatted JSON."""
        self._path.parent.mkdir(parents=True, exist_ok=True)

        with open(self._path, "w", encoding="utf-8") as fh:
            json.dump(self._registry, fh, indent=2, ensure_ascii=False)

        logger.info("Registry saved to %s", self._path)
