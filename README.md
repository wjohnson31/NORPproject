# NORP — Nonprofit Research Pipeline

An LLM-assisted data pipeline for nonprofit financial research and contextual discovery.

> **For AI/LLM context:** See [INSTRUCTIONS.md](INSTRUCTIONS.md) for a machine-readable
> guide to building, running, testing, and extending this project.

## Overview

NORP - Frontier LLM Integration for Automated Dataset Cleaning and Correlation Query Results -  ingests nonprofit financial datasets and external socioeconomic data,
cleans them using LLM-generated code, and will ultimately link them to
autonomously surface correlational insights.

**Current status (W5–W6 complete):** Raw datasets go in → profiled → cleaned
via OpenAI → optionally merged with existing datasets → cleaned/merged CSV +
transformation log + merge validation report come out.

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
│   └── merging/
│       ├── __init__.py
│       ├── join_detector.py  # Heuristic join key detection (synonym groups)
│       ├── join_agent.py     # LLM-based join key detection (OpenAI fallback)
│       ├── key_normalizer.py # Join key value normalization (states, years)
│       └── merge_engine.py   # Controlled merge with post-merge validation
├── data/
│   ├── raw/                 # Drop source files here
│   ├── processed/           # Profiles, registry, transform logs, merge reports
│   ├── cleaned/             # Cleaned output CSVs (after cleaning pipeline)
│   └── merged/              # Merged output CSVs (after merge step)
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

### Ingest + clean a dataset

```bash
python -m data_pipeline --file <path-to-file> --name <dataset-name>
```

**Example:**

```bash
python -m data_pipeline --file data/raw/sample_for_testing_extract.csv --name sample_for_testing
```

This will:
1. Load the file (CSV, Excel, or JSON) into a DataFrame with normalized column names
2. Generate a **dataset_profile** (schema, dtypes, missingness, time/geo columns, column roles) and save to `data/processed/<name>_profile.json`
3. Send the profile + a data sample to OpenAI, receive cleaning code, execute it in a sandbox, and save the cleaned dataset to `data/cleaned/<name>_cleaned.csv` with a transformation log at `data/processed/<name>_transform_log.json`
4. Register the dataset in `data/processed/registry.json`

> **Ingest only (no cleaning):** If you want to skip the cleaning step, add `--no-clean`:
> ```bash
> python -m data_pipeline --file <path-to-file> --name <dataset-name> --no-clean
> ```

### Ingest + merge with an existing dataset

```bash
python -m data_pipeline --file <path-to-file> --name <dataset-name> --merge-with <existing-dataset>
```

**Example:**

```bash
# First, make sure the target dataset is already in the registry
python -m data_pipeline --file data/raw/sample_for_testing_extract.csv --name sample_for_testing --no-clean

# Then ingest a new dataset and merge it with the existing one
python -m data_pipeline --file data/raw/state_unemployment_sample.csv --name state_unemployment --no-clean --merge-with sample_for_testing
```

This will perform all the standard steps above, then:
5. Detect compatible join keys between the two datasets (heuristic synonym matching first, LLM fallback if needed)
6. Normalize join key values (e.g., state names → abbreviations, date formats → years)
7. Merge the datasets and validate the result (key coverage, NaN inflation, row multiplication)
8. Save the merged CSV to `data/merged/` and a validation report to `data/processed/`

> **Swap primary/context roles:** By default the newly ingested dataset is primary. Add `--as-context` to make the `--merge-with` dataset primary instead:
> ```bash
> python -m data_pipeline --file data/raw/state_unemployment_sample.csv --name state_unemployment --no-clean --merge-with sample_for_testing --as-context
> ```

### CLI flags

| Flag | Short | Required | Description |
|------|-------|----------|-------------|
| `--file` | `-f` | Yes | Path to the raw data file (CSV, Excel, or JSON) |
| `--name` | `-n` | Yes | A short identifier for the dataset (e.g., `irs_990_2020`) |
| `--no-clean` | — | No | Skip the cleaning step (ingest + profile + register only) |
| `--merge-with` | — | No | Name of an existing registered dataset to merge with |
| `--as-context` | — | No | Treat the newly ingested dataset as context (right-side) in the merge |

## Pipeline Flow
When you run the command on a fresh dataset:

```
You run: python -m data_pipeline --file <file> --name <name> [--merge-with <dataset>]
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
    8. QUERY AGENT (W7–W8)
       Generates and runs correlational queries across merged datasets.
```

## Testing the Ingestion Pipeline

Activate the virtual environment first: `source .venv/bin/activate`

```bash
python -m data_pipeline --file data/raw/sample_for_testing_extract.csv --name sample_for_testing
```

Look for: INFO logs confirming load success, 10 rows / 9 columns,
detected time columns (`tax_year`, `fiscal_year_end`), detected geo column
(`state`), and profile saved. After running, `data/processed/` should contain
`sample_for_testing_profile.json` and `registry.json`.

## Roadmap

### W1–W2 — Workflow research + dataset scouting
- Research previous data integration structure vs structured agent pipeline design
- Finalize architecture
- Evaluate Claude Opus 4.6 v Codex API/tool use
- Scout nonprofit datasets and total results goal
- Initialize repo structure
- Select majority of datasets with documented schema
- Finalize system architecture diagram
- Initialize repo with data ingestion skeleton
- Select LLM and editor for project use

### W3–W4 — Data ingestion + cleaning pipeline (v1)
- Implement file ingestion (CSV/XLSX/JSON)
- Auto-generate schema + dataset_profile object
- Connect cleaning agent to Claude
- Execute returned cleaning code safely
- Implement transformation logging
- Raw dataset → cleaned dataset pipeline running
- Transformation logs stored
- dataset_profile JSON generated

### W5–W6 — Multi-dataset capabilities + join engine ✅
- ✅ Implement dataset registry structure (keys/metrics) — `column_roles` in SchemaProfiler
- ✅ Normalize join keys (states, years) — `KeyNormalizer` with 50-state + territory mapping
- ✅ Build controlled merge engine — `MergeEngine` with validation + reporting
- ✅ Allow user to choose 'primary' and 'context' datasets — `--merge-with` + `--as-context` flags
- ✅ Two cleaned datasets can be merged reliably — sample nonprofit + state unemployment merged at 100% key coverage
- ✅ Merged dataset checked and saved — CSV + JSON validation report
- ✅ LLM-based join key detection — `JoinAgent` uses OpenAI as fallback when heuristics fail

### W7–W8 — Contextual Relationship Query Agent (Grouped Analysis)
- Design Query Agent to generate correlational queries and results (no manual SQL queries)
- Generate simple visualizations and correlational outputs
- System autonomously produces multiple grouped contextual results
- Confirm no need for user prompting for analysis
- Merged summaries generated

### W9–W10 — Interpretive Layer + Robust Query Expansion
- Implement heuristics to rank findings
- Filter out trivial correlational relationships
- System outputs top-n correlational findings
- Outputs both graphs and explanation
- Weak relationships can be seen dropped

### W11–W12 — Scaled Testing across 10+ Datasets + Compile results
- Stress test across additional context (GDP, poverty, disaster rates, internet rates, etc.)
- Improve ranking heuristics
- Document end-to-end runs of 5 pairs of datasets
- Record best results for final report

