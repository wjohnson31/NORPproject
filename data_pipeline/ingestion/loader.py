"""
Dataset Loader
==============

Responsible for reading raw data files into pandas DataFrames with minimal,
deterministic normalization applied to column names.

Architectural notes:
    - File-type detection is based on extension rather than content sniffing.
      This keeps the logic transparent and avoids silent misdetection.
    - Encoding fallback uses ``latin-1`` after ``utf-8`` fails, which covers
      the vast majority of government / nonprofit data exports.
    - Column normalization is intentionally limited to cosmetic formatting
      (lowercase, strip, underscore) so that downstream cleaning modules
      retain full control over semantic transformations.
    - The loader returns a *raw* DataFrame — no rows are dropped, no types
      are cast, and no values are modified beyond column headers.
"""

import logging
import os
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Supported file extensions mapped to their canonical type name.
_EXTENSION_MAP: dict[str, str] = {
    ".csv": "csv",
    ".tsv": "csv",       # TSV is handled as CSV with tab separator
    ".xls": "excel",
    ".xlsx": "excel",
    ".json": "json",
}


class DatasetLoader:
    """Load a raw dataset from disk and apply column-name normalization.

    Supported formats:
        - CSV  (``.csv``, ``.tsv``)
        - Excel (``.xls``, ``.xlsx``)
        - JSON (``.json``)

    Usage::

        loader = DatasetLoader("data/raw/irs_990_extract.csv")
        df = loader.load()

    Parameters
    ----------
    file_path : str or Path
        Path to the raw data file.

    Attributes
    ----------
    file_path : Path
        Resolved absolute path to the data file.
    file_type : str or None
        Detected file type (``"csv"``, ``"excel"``, ``"json"``).
        Set after calling :meth:`load`.
    """

    def __init__(self, file_path: str | Path) -> None:
        self.file_path: Path = Path(file_path).resolve()
        self.file_type: Optional[str] = None

        if not self.file_path.exists():
            raise FileNotFoundError(f"Data file not found: {self.file_path}")

        logger.info("DatasetLoader initialized for: %s", self.file_path)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self) -> pd.DataFrame:
        """Load the dataset and return a column-normalized DataFrame.

        Returns
        -------
        pd.DataFrame
            The loaded dataset with normalized column names.

        Raises
        ------
        ValueError
            If the file extension is not supported.
        RuntimeError
            If loading fails after all encoding attempts.
        """
        self.file_type = self._detect_file_type()
        logger.info("Detected file type: %s", self.file_type)

        # Dispatch to the appropriate reader.
        reader = {
            "csv": self._load_csv,
            "excel": self._load_excel,
            "json": self._load_json,
        }[self.file_type]

        df: pd.DataFrame = reader()

        # Apply column normalization.
        df = self._normalize_columns(df)

        # Log summary statistics.
        file_size = os.path.getsize(self.file_path)
        logger.info(
            "Load complete — file_size=%s bytes, rows=%d, columns=%d",
            f"{file_size:,}",
            len(df),
            len(df.columns),
        )

        return df

    # ------------------------------------------------------------------
    # File-type detection
    # ------------------------------------------------------------------

    def _detect_file_type(self) -> str:
        """Determine file type from extension.

        Returns
        -------
        str
            One of ``"csv"``, ``"excel"``, ``"json"``.

        Raises
        ------
        ValueError
            If the extension is not in the supported set.
        """
        ext = self.file_path.suffix.lower()
        file_type = _EXTENSION_MAP.get(ext)

        if file_type is None:
            supported = ", ".join(sorted(_EXTENSION_MAP.keys()))
            raise ValueError(
                f"Unsupported file extension '{ext}'. "
                f"Supported extensions: {supported}"
            )

        return file_type

    # ------------------------------------------------------------------
    # Format-specific readers
    # ------------------------------------------------------------------

    def _load_csv(self) -> pd.DataFrame:
        """Load a CSV/TSV file with encoding fallback.

        Tries UTF-8 first, then falls back to Latin-1 (ISO 8859-1) which
        can decode any byte sequence and covers most government data exports.
        """
        sep = "\t" if self.file_path.suffix.lower() == ".tsv" else ","

        for encoding in ("utf-8", "latin-1"):
            try:
                df = pd.read_csv(
                    self.file_path,
                    sep=sep,
                    encoding=encoding,
                    low_memory=False,
                )
                logger.info("CSV loaded with encoding: %s", encoding)
                return df
            except UnicodeDecodeError:
                logger.warning(
                    "Encoding '%s' failed for %s, trying next...",
                    encoding,
                    self.file_path.name,
                )

        raise RuntimeError(
            f"Failed to load CSV after trying all supported encodings: "
            f"{self.file_path}"
        )

    def _load_excel(self) -> pd.DataFrame:
        """Load an Excel file (.xls or .xlsx).

        Uses the default ``openpyxl`` engine for .xlsx and ``xlrd`` for .xls.
        Only the first sheet is loaded; multi-sheet support can be added in
        a future iteration.
        """
        try:
            df = pd.read_excel(self.file_path, sheet_name=0)
            logger.info("Excel file loaded successfully.")
            return df
        except Exception as exc:
            raise RuntimeError(
                f"Failed to load Excel file: {self.file_path}"
            ) from exc

    def _load_json(self) -> pd.DataFrame:
        """Load a JSON file into a DataFrame.

        Expects either a JSON array of records or a JSON object whose values
        are column arrays (pandas ``read_json`` auto-detects orientation).
        """
        for encoding in ("utf-8", "latin-1"):
            try:
                df = pd.read_json(self.file_path, encoding=encoding)
                logger.info("JSON loaded with encoding: %s", encoding)
                return df
            except UnicodeDecodeError:
                logger.warning(
                    "Encoding '%s' failed for %s, trying next...",
                    encoding,
                    self.file_path.name,
                )

        raise RuntimeError(
            f"Failed to load JSON after trying all supported encodings: "
            f"{self.file_path}"
        )

    # ------------------------------------------------------------------
    # Column normalization
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Normalize DataFrame column names.

        Transformations applied (in order):
            1. Convert to string (handles numeric column headers).
            2. Strip leading/trailing whitespace.
            3. Convert to lowercase.
            4. Replace interior whitespace sequences with a single underscore.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame whose columns will be normalized **in place**.

        Returns
        -------
        pd.DataFrame
            The same DataFrame with updated column names.
        """
        import re

        df.columns = [
            re.sub(r"\s+", "_", str(col).strip().lower())
            for col in df.columns
        ]

        logger.debug("Normalized columns: %s", list(df.columns))
        return df
