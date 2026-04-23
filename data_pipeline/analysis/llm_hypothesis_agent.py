"""
LLM Hypothesis Agent
====================

An alternative to the brute-force Contextual Relationship Agent.
This agent uses an LLM to generate targeted hypotheses, writes precise Pandas
code to calculate the statistics, executes the code safely, and synthesizes
the mathematical results into plain-English insights.
"""

import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from data_pipeline.config import ANALYSIS_OUTPUT_DIR
from data_pipeline.ingestion.schema import SchemaProfiler
from data_pipeline.analysis.contextual_relationship_agent import safe_output_stem

logger = logging.getLogger(__name__)

# Default model for hypothesis generation
ANALYSIS_MODEL = os.environ.get("NORP_CLEANING_MODEL", "gpt-4o")


def _execute_analysis(df: pd.DataFrame, code: str, plot_path: str, agg_csv_path: str) -> tuple[Any, Optional[str]]:
    """Safely execute analysis code that writes to a 'result' variable and optionally saves a plot and csv."""
    import numpy as np
    
    # Strip basic imports
    clean_code = re.sub(r"^(import|from)\s+.*$", "", code, flags=re.MULTILINE).strip()
    
    # safe builtins
    _safe_builtins = {"abs", "all", "any", "bool", "dict", "enumerate", "float", "int", "len",
                      "list", "max", "min", "range", "round", "set", "sorted", "str", "sum",
                      "tuple", "zip", "None", "True", "False", "isinstance", "print"}
    
    _raw = __builtins__ if isinstance(__builtins__, dict) else __builtins__.__dict__
    builtins = {k: _raw[k] for k in _safe_builtins if k in _raw}

    g = {
        "df": df.copy(),
        "pd": pd,
        "np": np,
        "plt": plt,
        "plot_path": plot_path,
        "agg_csv_path": agg_csv_path,
        "__builtins__": builtins,
        "result": None,
    }

    try:
        exec(clean_code, g)
        return g.get("result"), None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


def _extract_json(text: str) -> list[dict[str, str]]:
    """Attempt to extract the JSON array from an LLM response."""
    try:
        # direct parse
        return json.loads(text)
    except json.JSONDecodeError:
        # try to find json block
        match = re.search(r"\[.*\]", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
    return []


class LLMHypothesisAgent:
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        self._api_key = api_key or os.environ.get("OPENAI_API_KEY")
        self._model = model or ANALYSIS_MODEL

    def run_analysis(
        self,
        df: pd.DataFrame,
        output_stem: str,
        output_dir: Optional[Path] = None,
        source_description: str = "merged dataset",
    ) -> dict[str, Any]:
        """Runs the LLM hypothesis generation, executes, and synthesizes."""
        out_dir = Path(output_dir or ANALYSIS_OUTPUT_DIR)
        out_dir.mkdir(parents=True, exist_ok=True)
        stem = safe_output_stem(output_stem)

        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": source_description,
            "analysis_method": "llm_hypothesis_engine",
            "status": "started",
        }

        if not self._api_key:
            logger.error("OPENAI_API_KEY is required for LLM Hypothesis Analysis.")
            report["status"] = "error_no_api_key"
            return report

        try:
            from openai import OpenAI
        except ImportError:
            report["status"] = "error_missing_openai"
            return report

        client = OpenAI(api_key=self._api_key)
        profiler = SchemaProfiler(df)
        profile = profiler.generate_profile()
        
        # We drop metric-heavy components to keep prompt concise
        profile.pop("missingness", None)

        try:
            sample_str = df.head(15).to_csv(index=False)
        except Exception:
            sample_str = str(df.head(15))

        logger.info("Generating hypotheses via LLM...")
        
        prompt = f"""You are an expert Data Scientist specializing in urban sociology. You are analyzing a MERGED dataset that was created by joining two completely separate city datasets: {source_description}.

CRITICAL CONTEXT ABOUT THIS DATA:
- This is a MANY-TO-MANY merge of two event-level datasets. Each row is NOT a unique observation — it is a cross-product artifact.
- Raw row-level correlations between columns from different source datasets are MATHEMATICALLY MEANINGLESS on this merged table.
- The ONLY valid analytical approach is to AGGREGATE first, then correlate the aggregated summaries.

YOUR TASK: Generate EXACTLY 10 hypotheses. Every single one MUST follow this pattern:
1. Group the data by a shared geographic or temporal dimension (e.g., `area_name`, `zipcode`, `month`, `council_district`)
2. Aggregate metrics from EACH source dataset separately within that groupby (e.g., count of traffic collisions, count of 311 requests by type, mean victim age)
3. Correlate the AGGREGATED columns against each other
4. This produces one data point per neighborhood/month/zip — which IS a valid statistical unit

COLUMNS TO ABSOLUTELY NEVER CORRELATE DIRECTLY:
- Any ID column (dr_no, srnumber, objectid, etc.)
- Any column that is constant or near-constant (e.g., crime_code if all rows share the same value)
- Any latitude/longitude columns (these are spatial, not metrics)
- Any raw date/time strings — extract month or year integers first

GOOD HYPOTHESIS EXAMPLES FOR TWO MERGED CITY DATASETS:
- "Neighborhoods with more traffic collisions also generate more illegal dumping complaints" → groupby area_name, count collisions vs count illegal dumping requests
- "Areas with older average collision victims have higher bulky item pickup request volumes" → groupby area_name, mean victim_age vs count of bulky item requests
- "Monthly spikes in traffic collisions correlate with monthly spikes in 311 requests" → extract month, groupby month, count each

BAD HYPOTHESIS EXAMPLES (DO NOT DO THESE):
- Correlating crime_code with anything (it's constant)
- Correlating raw row-level columns across datasets without groupby
- Using latitude or longitude as a metric
- Any hypothesis that doesn't start with a groupby aggregation

CODE REQUIREMENTS:
- Do NOT import anything. `pd`, `np`, `plt`, and `df` are pre-loaded.
- Do NOT redefine `plot_path` or `agg_csv_path`. They are pre-loaded strings.
- Store your final scalar correlation value in a variable named exactly `result`.
- Your aggregated dataframe MUST call `.dropna()` before correlating.
- Save your aggregated dataframe to CSV: `df_agg.to_csv(agg_csv_path)`
- Create a scatter plot of the two aggregated columns with a trendline using `np.polyfit`.
- Use `plt.scatter(x, y)` — do NOT use `df.plot.scatter()` with Series objects.
- Always include `plt.title()`, `plt.xlabel()`, `plt.ylabel()`.
- Always call `plt.savefig(plot_path, dpi=150, bbox_inches='tight')` then `plt.close()`.
- Minimum 5 data points in the scatter — if your groupby produces fewer, pick a different grouping dimension.

Output ONLY a raw JSON array. No markdown, no explanation, no backticks. Just the array.
Each element must have exactly two keys: "hypothesis" and "code".

Schema JSON:
{json.dumps(profile, indent=2)}

Data Sample (15 rows):
{sample_str}
"""
        try:
            resp1 = client.chat.completions.create(
                model=self._model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
            )
            llm_text = resp1.choices[0].message.content or ""
            hypotheses = _extract_json(llm_text)
        except Exception as exc:
            logger.exception("Failed to generate hypotheses: %s", exc)
            report["status"] = "error_hypothesis_generation"
            return report

        if not hypotheses:
            logger.error("LLM did not return a valid JSON array of hypotheses.")
            report["status"] = "error_invalid_json"
            return report

        executed_results = []
        logger.info("Executing LLM-generated hypothesis queries...")
        for i, item in enumerate(hypotheses):
            hypo = item.get("hypothesis", "")
            code = item.get("code", "")
            if not hypo or not code:
                continue

            logger.info("  Testing: %s", hypo)
            
            is_agg = "[AGGREGATED" in hypo.upper()
            suffix = "_aggregated" if is_agg else ""
            
            plot_file = str(out_dir / f"{stem}_hypothesis_{i}{suffix}_chart.png")
            agg_csv_file = str(out_dir / f"{stem}_hypothesis_{i}_aggregated.csv")
            
            result_val, err = _execute_analysis(df, code, plot_path=plot_file, agg_csv_path=agg_csv_file)
            
            # Cast pandas types to native python for JSON logging
            if hasattr(result_val, "item"):
                result_val = result_val.item()
            
            executed_results.append({
                "hypothesis": hypo,
                "code": code,
                "executed_result": result_val if err is None else f"ERROR: {err}",
                "plot_saved": os.path.exists(plot_file),
                "agg_csv_saved": os.path.exists(agg_csv_file)
            })

        report["hypotheses_tested"] = executed_results

        logger.info("Synthesizing final insights via LLM...")
        synth_prompt = f"""You are a Sociologist and Data Scientist analyzing pipeline findings.
Review the following correlation hypotheses that were generated and then executed locally via pandas:

{json.dumps(executed_results, indent=2, default=str)}

Synthesize these findings into a plain-English, formatted executive summary.
For EACH of the executed hypotheses that succeeded, you MUST explicitly output:
- The Datasets being analyzed
- The specific Question / Hypothesis being addressed
- The exact correlation number computed
- A plain-English explanation of exactly what this correlation means in the real world and how strong it is.

Format this as a numbered list.
CRITICAL: Since all queries are now aggregated, formally organize the list by grouping the findings under headers based on their logical category (e.g., "Geographic Findings", "Temporal Findings", "Demographic Findings"). 
Do not include the raw python code or JSON dictionaries. Explain what the numbers actually mean in context.
        """
        try:
            resp2 = client.chat.completions.create(
                model=self._model,
                messages=[{"role": "user", "content": synth_prompt}],
                temperature=0.3,
            )
            synthesis = resp2.choices[0].message.content or ""
        except Exception as exc:
            logger.exception("Failed to synthesize results: %s", exc)
            synthesis = "Error synthesizing results."

        report["merged_summary"] = synthesis
        report["status"] = "success"

        # Save artifacts
        json_path = out_dir / f"{stem}_llm_hypothesis_report.json"
        summary_path = out_dir / f"{stem}_llm_insight_summary.txt"
        
        with open(json_path, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, ensure_ascii=False)
        with open(summary_path, "w", encoding="utf-8") as fh:
            fh.write(synthesis)

        logger.info("LLM Hypothesis analysis complete.")
        logger.info("Report JSON: %s", json_path)
        logger.info("Insight summary: %s", summary_path)

        return report

