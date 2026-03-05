# NORP — Nonprofit Research Pipeline

An LLM-assisted data pipeline for nonprofit financial research and contextual discovery.

> **For AI/LLM context:** See [INSTRUCTIONS.md](INSTRUCTIONS.md) for a machine-readable
> guide to building, running, testing, and extending this project.

## Overview

NORP - Frontier LLM Integration for Automated Dataset Cleaning and Correlation Query Results -  ingests nonprofit financial datasets and external socioeconomic data,
cleans them using LLM-generated code, and will ultimately link them to
autonomously surface correlational insights.

**Current status (W3–W4 complete):** Raw datasets go in → profiled → cleaned
via OpenAI → cleaned CSV + transformation log come out.

## Project Structure

```
norp/
├── data_pipeline/
│   ├── __init__.py          # Package root
│   ├── __main__.py          # python -m entry point
│   ├── config.py            # Paths & logging configuration
│   ├── main.py              # CLI orchestrator (ingest + cleaning)
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── loader.py        # DatasetLoader — CSV/Excel/JSON + column normalization
│   │   ├── schema.py        # SchemaProfiler — dataset_profile (schema + stats)
│   │   └── registry.py     # DatasetRegistry — JSON persistence
│   └── cleaning/
│       ├── __init__.py
│       ├── agent.py         # CleaningAgent — Claude-generated cleaning code
│       ├── executor.py      # SafeCleaningExecutor — restricted execution
│       └── transform_log.py # TransformationLog — step logging + JSON
├── data/
│   ├── raw/                 # Drop source files here
│   ├── processed/           # dataset_profile JSON, registry, transform logs
│   └── cleaned/             # Cleaned output CSVs (after cleaning pipeline)
├── requirements.txt
├── README.md
└── INSTRUCTIONS.md       # AI-facing project context for LLM workflows
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
2. Generate a **dataset_profile** (schema, dtypes, missingness, time/geo columns) and save to `data/processed/<name>_profile.json`
3. Send the profile + a data sample to OpenAI, receive cleaning code, execute it in a sandbox, and save the cleaned dataset to `data/cleaned/<name>_cleaned.csv` with a transformation log at `data/processed/<name>_transform_log.json`
4. Register the dataset in `data/processed/registry.json`

> **Ingest only (no cleaning):** If you want to skip the cleaning step, add `--no-clean`:
> ```bash
> python -m data_pipeline --file <path-to-file> --name <dataset-name> --no-clean
> ```

### CLI flags

| Flag | Short | Required | Description |
|------|-------|----------|-------------|
| `--file` | `-f` | Yes | Path to the raw data file (CSV, Excel, or JSON) |
| `--name` | `-n` | Yes | A short identifier for the dataset (e.g., `irs_990_2020`) |
| `--no-clean` | — | No | Skip the cleaning step (ingest + profile + register only) |

## Pipeline Flow
When you run the command on a fresh dataset:

```
You run: python -m data_pipeline --file <file> --name <name>
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
         ▼
    5. HARMONIZER
       Makes different datasets speak the same language (matching column names, types, units)
         │
         ▼
    6. JOIN DETECTOR
       Compares profiles to find shared keys (e.g., both have state + year → joinable)
         │
         ▼
    7. LLM ORCHESTRATOR
       Generates and runs queries across linked datasets in the background, reports final result to user.
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

### W5–W6 — Multi-dataset capabilities + join engine
- Implement dataset registry structure (keys/metrics)
- Normalize join keys (states, years, ages?)
- Build controlled merge engine
- Allow user to choose 'primary' and 'context' datasets
- Two cleaned datasets can be merged reliably (ex goal nonprofit financials + L.A. unemployment data)
- Merged dataset checked and saved

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

