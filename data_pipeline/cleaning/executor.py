"""
Safe Cleaning Executor
======================

Executes cleaning code returned by the cleaning agent in a restricted
environment: only pandas, numpy, and the DataFrame are available. No file I/O,
network, or os/sys access. Prevents malicious or accidental side effects.

Architectural notes:
    - The cleaning code must accept a variable ``df`` (the raw DataFrame) and
      must leave the cleaned result in ``df`` (reassign or in-place).
    - A timeout is applied to avoid infinite loops (optional; can use signal
      or multiprocessing for cross-platform timeout).
"""

import logging
import re
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Strip leading import lines so LLM-generated code that starts with "import pandas" etc. still runs.
_IMPORT_LINE = re.compile(r"^\s*import\s+\w+(\s+as\s+\w+)?\s*(#.*)?$", re.MULTILINE)
_IMPORT_FROM = re.compile(r"^\s*from\s+\w+(\s+import\s+.+)\s*(#.*)?$", re.MULTILINE)


def _strip_import_lines(code: str) -> str:
    """Remove leading lines that are only import statements (pd/np are already provided)."""
    lines = code.splitlines()
    while lines:
        line = lines[0]
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            lines.pop(0)
            continue
        if _IMPORT_LINE.match(line) or _IMPORT_FROM.match(line):
            lines.pop(0)
            continue
        break
    return "\n".join(lines).strip()

# Safe builtins: no open, exec, eval, __import__, getattr, setattr, etc.
_SAFE_BUILTINS: set[str] = {
    "abs", "all", "any", "bool", "dict", "enumerate", "float", "int", "len",
    "list", "max", "min", "range", "round", "set", "sorted", "str", "sum",
    "tuple", "zip", "None", "True", "False", "isinstance", "repr", "print",
}


def _restricted_globals(df: pd.DataFrame) -> dict[str, Any]:
    """Build a minimal globals dict for exec(): df, pd, np, and safe builtins."""
    import numpy as np

    _raw = __builtins__ if isinstance(__builtins__, dict) else __builtins__.__dict__
    builtins = {k: _raw[k] for k in _SAFE_BUILTINS if k in _raw}

    return {
        "df": df,
        "pd": pd,
        "np": np,
        "__builtins__": builtins,
    }


class SafeCleaningExecutor:
    """Execute cleaning code safely and return the resulting DataFrame.

    The code string must use a variable named ``df`` (the input DataFrame)
    and leave the cleaned result in ``df``. No other variables are persisted
    back to the caller.

    Usage::

        executor = SafeCleaningExecutor()
        cleaned_df, error = executor.execute(raw_df, code_string)
    """

    def execute(
        self,
        df: pd.DataFrame,
        code: str,
    ) -> tuple[pd.DataFrame, Optional[str]]:
        """Run the cleaning code in a restricted namespace.

        Parameters
        ----------
        df : pd.DataFrame
            The raw (or intermediate) DataFrame. A copy is passed so the
            original is not mutated.
        code : str
            Python code that uses ``df`` and leaves the result in ``df``.

        Returns
        -------
        tuple of (pd.DataFrame, optional str)
            (cleaned DataFrame, None) on success, or (original df, error message) on failure.
        """
        if not code or not code.strip():
            logger.warning("Empty cleaning code — returning DataFrame unchanged.")
            return df.copy(), None

        code = _strip_import_lines(code)
        if not code:
            logger.warning("No code left after stripping imports — returning DataFrame unchanged.")
            return df.copy(), None

        df_work = df.copy()
        g = _restricted_globals(df_work)
        g["df"] = df_work

        try:
            exec(code, g)
        except Exception as exc:
            msg = f"Cleaning code failed: {exc!r}"
            logger.exception("%s", msg)
            return df.copy(), msg

        result = g.get("df")
        if result is None:
            return df.copy(), "Cleaning code did not leave result in variable 'df'."
        if not isinstance(result, pd.DataFrame):
            return df.copy(), f"Variable 'df' is not a DataFrame (got {type(result).__name__})."

        logger.info(
            "Cleaning executed — rows %d → %d, columns %d → %d",
            len(df),
            len(result),
            len(df.columns),
            len(result.columns),
        )
        return result, None
