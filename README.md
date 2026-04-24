# NORP — Nonprofit Research Pipeline

An LLM-assisted data pipeline for nonprofit financial research and contextual discovery.

> **For AI/LLM context:** See [INSTRUCTIONS.md](INSTRUCTIONS.md) for a machine-readable
> guide to building, running, testing, and extending this project.

> **For a Proof of Concept/Demo:** See the [Proof of Concept Directory](proof_of_concept/) for previous results of our implementation

## Overview

NORP - Frontier LLM Integration for Automated Dataset Cleaning and Correlation Query Results -  ingests nonprofit financial datasets and external socioeconomic data,
cleans them using LLM-generated code, and will ultimately link them to
autonomously surface correlational insights.



## Project Structure

```
norp/
├── data_pipeline/
│   ├── __init__.py          # Package root
│   ├── __main__.py          # python -m entry point
│   ├── config.py            # Paths & logging configuration
│   ├── main.py              # CLI orchestrator (ingest + clean + merge)
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── loader.py        # DatasetLoader — CSV/Excel/JSON + column normalization
│   │   ├── schema.py        # SchemaProfiler — dataset_profile + column roles
│   │   └── registry.py      # DatasetRegistry — JSON persistence
│   ├── cleaning/
│   │   ├── __init__.py
│   │   ├── agent.py         # CleaningAgent — OpenAI-generated cleaning code
│   │   ├── executor.py      # SafeCleaningExecutor — restricted execution
│   │   └── transform_log.py # TransformationLog — step logging + JSON
│   ├── merging/
│   │   ├── __init__.py
│   │   ├── join_detector.py  # Heuristic join key detection (synonym groups)
│   │   ├── join_agent.py     # LLM-based join key detection (OpenAI fallback)
│   │   ├── key_normalizer.py # Join key value normalization (states, years)
│   │   └── merge_engine.py   # Controlled merge with post-merge validation
│   └── analysis/
│       ├── __init__.py
│       └── llm_hypothesis_agent.py  # LLM Hypothesis Engine generating the correlational queries
├── data/
│   ├── raw/                 # Drop source files here
│   ├── processed/           # Profiles, registry, transform logs, merge reports
│   ├── cleaned/             # Cleaned output CSVs (after cleaning pipeline)
│   ├── merged/              # Merged output CSVs (after merge step)
│   ├── analysis/            # Generated charts, CSVs, and final text reports
│   └── proof_of_concept/    # Previous high-quality results & POC demos
├── requirements.txt
├── README.md
└── INSTRUCTIONS.md          # AI-facing project context for LLM workflows
```

## Setup

### 1. Create and activate the virtual environment

```bash
# Create the virtual environment (one-time setup)
python3 -m venv .venv

# Activate it
source .venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

This installs:
- **pandas** — DataFrame loading and manipulation
- **openpyxl** — Excel (.xlsx) file support
- **openai** — OpenAI API for the cleaning agent
- **python-dotenv** — Loads API keys from `.env` file

### 3. Set up your API key (for cleaning)

Create a `.env` file in the project root:

```bash
echo 'OPENAI_API_KEY=your-key-here' > .env
```

The pipeline works without this — it just skips the cleaning step.

## Usage

```bash
# Activate Your Venv
source .venv/bin/activate
```

### Automated Synthesis Mode

The pipeline is now fully automated. You no longer need to manage cleaning, merging, or hypothesis testing manually with flags. Provide exactly two raw files, and the engine will orchestrate the rest.

```bash
python -m data_pipeline <dataset_a.csv> <dataset_b.csv>
```

**Example:**

```bash
python -m data_pipeline data/raw/Traffic_Collision_Data_from_2010_to_Present.csv data/raw/MyLA311_Service_Request_Data_2020_20260423.csv
```

This will autonomously:
1. **Ingest**: Load both files into memory, capping massive datasets at 15,000 rows for memory safety.
2. **Profile**: Generate `dataset_profile` (schema, missingness, time/geo columns) and save them to `data/processed/`.
3. **Clean**: Use OpenAI to clean and standardize the data (saving to `data/cleaned/`).
4. **Merge**: Detect the broadest compatible join key (like a Police Precinct or Area) and execute the merge.
5. **Analyze**: An AI Sociologist agent generates 10 relationships, tests them using Pandas, plots charts, and writes an Executive Summary.

After a run is complete, navigate to `data/analysis/` to find your findings:
- **`*_llm_insight_summary.txt`**: The final plain-English report.
- **`*_llm_hypothesis_report.json`**: The technical breakdown of every test run.
- **`*_hypothesis_N_chart.png`**: Visual scatter plots for every successful correlation.

## Pipeline Flow
When you run the synthesis command, it coordinates two ingestion cycles followed by a merge:

```
You run: python -m data_pipeline <dataset_a> <dataset_b>
         │
         ▼
    1. LOADER (loader.py)
       Reads the file (CSV, Excel, or JSON) into memory.
       Cleans up column names: "Tax Year" → "tax_year"
         │
         ▼
    2. PROFILER (schema.py)
       Looks at the loaded data and writes a summary:
       - How many rows and columns
       - What % of each column is empty (missingness)
       - Which columns look like dates (tax_year, fiscal_year, etc.)
       - Which columns look like locations (state, fips, zip, etc.)
       - Column roles: key (joinable) vs metric (numeric) vs dimension (categorical)
       Saves this summary → data/processed/<name>_profile.json
         │
         ▼
    3. REGISTRY (registry.py)
       Records that this dataset exists in a catalog:
       - Name, file path, column info, timestamp
       Saves the catalog → data/processed/registry.json
         │
         ▼
    4. CLEANER (cleaning/agent.py + executor.py + transform_log.py)
       If OPENAI_API_KEY is set and not --no-clean: OpenAI suggests cleaning code,
       it runs in a sandbox, and the cleaned dataset + transformation log are saved.
       Fixes bad values, fills gaps, standardizes formats (e.g., state abbreviations).
         │
         ▼ (if --merge-with is provided)
    5. JOIN DETECTOR (merging/join_detector.py + join_agent.py)
       Compares both dataset profiles to find compatible join keys.
       First tries heuristic synonym groups (e.g., tax_year ↔ year).
       If no matches found, falls back to LLM-based detection via OpenAI.
         │
         ▼
    6. KEY NORMALIZER (merging/key_normalizer.py)
       Standardizes join key values so they match across datasets.
       States: "California" → "CA". Years: 202012 → 2020.
         │
         ▼
    7. MERGE ENGINE (merging/merge_engine.py)
       Performs a controlled left join, validates the result (key coverage,
       NaN inflation, row multiplication), and saves:
       - Merged CSV → data/merged/<primary>_<context>_merged.csv
       - Validation report → data/processed/<primary>_<context>_merge_report.json
         │
         ▼ (future)
    8. LLM HYPOTHESIS AGENT (W7–W10)
       Agent reviews schema profile + sample data and autonomously hypothesizes
       sociological relationships between columns. Generates precise Pandas code 
       to mathematically test them, executes it in a sandbox, and writes a synthesized 
       plain-English summary report.
```

## Testing the Ingestion Pipeline

Activate the virtual environment first: `source .venv/bin/activate`

```bash
python -m data_pipeline data/raw/sample_for_testing_extract.csv data/raw/state_unemployment_sample.csv
```

Look for: INFO logs confirming load success, detection of geographic/time columns, and the final generation of sociological charts in the `data/analysis/` folder.