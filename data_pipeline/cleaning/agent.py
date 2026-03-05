"""
Cleaning Agent (OpenAI)
=======================

Calls OpenAI with the dataset profile and a sample of the data to generate
cleaning code. The code is expected to use a variable ``df`` and leave the
cleaned result in ``df`` for safe execution by :class:`SafeCleaningExecutor`.

Requires environment variable OPENAI_API_KEY to be set (or in .env).
"""

import json
import logging
import os
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Default model for code generation; can be overridden via env (e.g. gpt-4o, gpt-4-turbo).
CLEANING_MODEL = os.environ.get("NORP_CLEANING_MODEL", "gpt-4o")


def _extract_python_code(content: str) -> Optional[str]:
    """Extract the first fenced Python code block from markdown-style content."""
    pattern = r"```(?:python)?\s*\n(.*?)```"
    match = re.search(pattern, content, re.DOTALL)
    if match:
        return match.group(1).strip()
    if "def " in content or "df[" in content or "df." in content or "pd." in content:
        return content.strip()
    return None


def _profile_to_text(profile: dict[str, Any]) -> str:
    """Format profile dict as readable text for the prompt."""
    return json.dumps(profile, indent=2)


class CleaningAgent:
    """Generate data-cleaning code via OpenAI from a schema profile and data sample.

    Usage::

        agent = CleaningAgent()
        code = agent.generate_cleaning_code(profile, df_head)
        if code:
            cleaned_df, err = SafeCleaningExecutor().execute(df, code)
    """

    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None) -> None:
        self._api_key = api_key or os.environ.get("OPENAI_API_KEY")
        self._model = model or CLEANING_MODEL
        if not self._api_key:
            logger.warning(
                "OPENAI_API_KEY not set — cleaning agent will not call OpenAI."
            )

    def generate_cleaning_code(
        self,
        dataset_profile: dict[str, Any],
        df_sample: Any,
        dataset_name: Optional[str] = None,
    ) -> Optional[str]:
        """Ask OpenAI to generate Python code that cleans the DataFrame.

        Parameters
        ----------
        dataset_profile : dict
            Schema profile from :class:`SchemaProfiler.generate_profile()`.
        df_sample : pd.DataFrame
            Small sample of the data (e.g. head(30)) for context.
        dataset_name : str, optional
            Name of the dataset for context in the prompt.

        Returns
        -------
        str or None
            The extracted Python code string, or None if API key is missing,
            the API call fails, or no code block is found.
        """
        if not self._api_key:
            logger.error("Cannot generate cleaning code: OPENAI_API_KEY not set.")
            return None

        try:
            from openai import OpenAI
        except ImportError:
            logger.error("openai package not installed. pip install openai")
            return None

        try:
            sample_str = df_sample.head(30).to_csv(index=False)
        except Exception:
            sample_str = str(df_sample)

        name_part = f"Dataset name: {dataset_name}.\n\n" if dataset_name else ""
        user_content = f"""{name_part}Below is the schema profile (columns, dtypes, missingness, time/geo hints) and a small sample of the data in CSV form.

Schema profile (JSON):
{_profile_to_text(dataset_profile)}

Sample data (CSV):
{sample_str}

Generate a single Python code block that cleans this DataFrame. The code will run on the FULL dataset (not just this sample), so columns may have NaNs/missing values even if the sample looks complete. Sandbox: only `df`, `pd` (pandas), and `np` (numpy) are available — do NOT use any import statements. Rules:
1. The input DataFrame is in the variable `df`. Leave the result in `df`.
2. Do NOT write import pandas, import numpy, or any other import — pd and np are already in scope.
3. No file I/O, network, or os/sys. Only pandas and numpy.
4. Prefer: fix missing values, normalize strings (strip, lowercase where appropriate), standardize date columns, drop full duplicates, fix numeric columns read as object.
5. CRITICAL — avoid IntCastingNaNError: never use astype(int) on columns that might have NaNs. Use astype('Int64') for nullable integers, or fillna/dropna before casting to int.
6. For date columns use pd.to_datetime(..., errors='coerce') so invalid values become NaT instead of raising.
7. Output only the code in a markdown fenced block: ```python ... ```
"""

        system_content = """You are a data cleaning assistant. Output only one Python code block that cleans the DataFrame variable `df` and leaves the result in `df`. Do NOT use any import statements — pd (pandas) and np (numpy) are already available. Handle NaNs safely: use astype('Int64') not astype(int) for integer columns with missing values; use pd.to_datetime(..., errors='coerce') for dates. No explanations outside the code block."""

        try:
            client = OpenAI(api_key=self._api_key)
            response = client.chat.completions.create(
                model=self._model,
                max_tokens=2048,
                messages=[
                    {"role": "system", "content": system_content},
                    {"role": "user", "content": user_content},
                ],
            )
        except Exception as exc:
            logger.exception("OpenAI API call failed: %s", exc)
            return None

        text = (response.choices[0].message.content or "").strip()
        code = _extract_python_code(text)
        if not code:
            logger.warning("No Python code block found in OpenAI response.")
        return code
