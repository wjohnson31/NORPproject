"""
Join Agent (OpenAI)
===================

Calls OpenAI with two dataset profiles to determine which columns can be
used as join keys for merging.  Mirrors the pattern of
:class:`CleaningAgent` — sends structured context, receives a JSON response.

Requires environment variable OPENAI_API_KEY to be set (or in .env).

Design decisions:
    - The LLM receives only the profile JSONs and a small data sample,
      never the full datasets.
    - The response is constrained to a JSON array of join key objects.
    - Results use the same format as :class:`JoinDetector` so they can
      be passed directly to :class:`MergeEngine`.
    - If the API key is missing or the call fails, returns an empty list
      so the pipeline degrades gracefully.
"""

import json
import logging
import os
import re
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Reuse the same model as the cleaning agent (configurable via env).
_JOIN_MODEL = os.environ.get("NORP_CLEANING_MODEL", "gpt-4o")

_SYSTEM_PROMPT = """\
You are a data integration assistant.  You will receive the schema profiles \
of two datasets (Dataset A and Dataset B).  Each profile contains the column \
names, data types, missingness percentages, and detected time/geo columns.

Your job is to determine which columns from Dataset A can be joined with \
which columns from Dataset B.

Rules:
1. Only suggest joins that are semantically meaningful (same real-world concept).
2. For each pair, specify a key_type from this list:
   - "time_year" — both represent a year
   - "time_date" — both represent a date
   - "geo_state" — both represent a US state
   - "geo_area" — both represent a geographic area / neighborhood / district
   - "geo_county" — both represent a county
   - "geo_city" — both represent a city
   - "geo_zip" — both represent a zip code
   - "categorical" — both represent the same categorical dimension
   - "generic" — other shared key
3. Set confidence between 0.0 and 1.0 based on how sure you are.
4. If no columns can be joined, return an empty array.
5. Output ONLY a JSON array, no explanation.  Example:
[
  {"left_col": "state", "right_col": "state_code", "key_type": "geo_state", "confidence": 0.95},
  {"left_col": "tax_year", "right_col": "year", "key_type": "time_year", "confidence": 0.9}
]
"""


def _extract_json_array(text: str) -> Optional[list]:
    """Extract and parse a JSON array from LLM output (may be fenced)."""
    # Try to find a fenced code block first
    match = re.search(r"```(?:json)?\s*\n(.*?)```", text, re.DOTALL)
    candidate = match.group(1).strip() if match else text.strip()
    try:
        parsed = json.loads(candidate)
        if isinstance(parsed, list):
            return parsed
    except json.JSONDecodeError:
        pass
    # Try to find the first [ ... ] in the text
    bracket_match = re.search(r"\[.*\]", text, re.DOTALL)
    if bracket_match:
        try:
            parsed = json.loads(bracket_match.group(0))
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            pass
    return None


class JoinAgent:
    """Ask OpenAI to identify join keys between two datasets.

    Usage::

        agent = JoinAgent()
        keys = agent.detect_join_keys(profile_a, profile_b, df_a, df_b)

    Parameters
    ----------
    api_key : str, optional
        OpenAI API key.  Falls back to ``OPENAI_API_KEY`` env var.
    model : str, optional
        Model to use.  Falls back to ``NORP_CLEANING_MODEL`` or ``gpt-4o``.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
    ) -> None:
        self._api_key = api_key or os.environ.get("OPENAI_API_KEY")
        self._model = model or _JOIN_MODEL
        if not self._api_key:
            logger.warning(
                "OPENAI_API_KEY not set — JoinAgent will not call OpenAI."
            )

    def detect_join_keys(
        self,
        profile_left: dict[str, Any],
        profile_right: dict[str, Any],
        df_left: Optional[pd.DataFrame] = None,
        df_right: Optional[pd.DataFrame] = None,
        *,
        name_left: str = "Dataset A",
        name_right: str = "Dataset B",
    ) -> list[dict[str, Any]]:
        """Send two profiles to OpenAI and return join key recommendations.

        Parameters
        ----------
        profile_left, profile_right : dict
            Schema profiles from :class:`SchemaProfiler`.
        df_left, df_right : pd.DataFrame, optional
            If provided, a small sample (5 rows) is included for context.
        name_left, name_right : str
            Dataset names for the prompt.

        Returns
        -------
        list[dict]
            Join key pairs in the same format as :class:`JoinDetector`.
            Empty list if the API key is missing or the call fails.
        """
        if not self._api_key:
            logger.error("Cannot detect join keys: OPENAI_API_KEY not set.")
            return []

        try:
            from openai import OpenAI
        except ImportError:
            logger.error("openai package not installed. pip install openai")
            return []

        # Build the user prompt
        user_content = self._build_prompt(
            profile_left, profile_right,
            df_left, df_right,
            name_left, name_right,
        )

        try:
            client = OpenAI(api_key=self._api_key)
            response = client.chat.completions.create(
                model=self._model,
                max_tokens=1024,
                temperature=0.0,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": user_content},
                ],
            )
        except Exception as exc:
            logger.exception("OpenAI API call failed: %s", exc)
            return []

        text = (response.choices[0].message.content or "").strip()
        logger.info("JoinAgent raw response: %s", text[:500])

        keys = _extract_json_array(text)
        if keys is None:
            logger.warning("Could not parse JSON from JoinAgent response.")
            return []

        # Validate and normalize each entry
        valid_keys: list[dict[str, Any]] = []
        for entry in keys:
            if not isinstance(entry, dict):
                continue
            if "left_col" not in entry or "right_col" not in entry:
                continue
            valid_keys.append({
                "left_col": str(entry["left_col"]),
                "right_col": str(entry["right_col"]),
                "key_type": str(entry.get("key_type", "generic")),
                "confidence": float(entry.get("confidence", 0.8)),
                "match_method": "llm",
            })

        logger.info(
            "JoinAgent returned %d join key pair(s): %s",
            len(valid_keys),
            [(k["left_col"], k["right_col"]) for k in valid_keys],
        )
        return valid_keys

    @staticmethod
    def _build_prompt(
        profile_left: dict[str, Any],
        profile_right: dict[str, Any],
        df_left: Optional[pd.DataFrame],
        df_right: Optional[pd.DataFrame],
        name_left: str,
        name_right: str,
    ) -> str:
        """Build the user message with profiles and optional samples."""
        parts = [
            f"Dataset A name: {name_left}",
            f"Dataset A profile:\n{json.dumps(profile_left, indent=2)}",
        ]
        if df_left is not None:
            try:
                sample_a = df_left.head(5).to_csv(index=False)
                parts.append(f"Dataset A sample (5 rows):\n{sample_a}")
            except Exception:
                pass

        parts.append(f"\nDataset B name: {name_right}")
        parts.append(
            f"Dataset B profile:\n{json.dumps(profile_right, indent=2)}"
        )
        if df_right is not None:
            try:
                sample_b = df_right.head(5).to_csv(index=False)
                parts.append(f"Dataset B sample (5 rows):\n{sample_b}")
            except Exception:
                pass

        parts.append(
            "\nWhich columns from Dataset A can be joined with columns "
            "from Dataset B?  Return a JSON array."
        )
        return "\n\n".join(parts)
