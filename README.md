# NORP — Nonprofit Research Pipeline

A modular data ingestion infrastructure for nonprofit financial research.

## Overview

NORP provides the foundational skeleton for loading, profiling, and registering
nonprofit financial datasets (e.g., IRS 990 extracts) and external contextual
datasets (e.g., state-level unemployment data).

**Current phase (W1–W2):** Data ingestion skeleton only.

## Project Structure

```
norp/
├── data_pipeline/
│   ├── __init__.py          # Package root
│   ├── __main__.py          # python -m entry point
│   ├── config.py            # Paths & logging configuration
│   ├── main.py              # CLI orchestrator
│   └── ingestion/
│       ├── __init__.py      # Sub-package exports
│       ├── loader.py        # DatasetLoader — file I/O + column normalization
│       ├── schema.py        # SchemaProfiler — metadata extraction
│       └── registry.py      # DatasetRegistry — JSON persistence
├── data/
│   ├── raw/                 # Drop source files here
│   │   └── sample_for_testing_extract.csv
│   └── processed/           # Profiles & registry written here
├── requirements.txt
└── README.md
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

## Usage

```bash
# Activate Your Venv
source .venv/bin/activate
```

### Ingest a dataset

```bash
python -m data_pipeline --file <path-to-file> --name <dataset-name>
```

**Example with the included sample data:**

```bash
python -m data_pipeline --file data/raw/sample_for_testing_extract.csv --name sample_for_testing
```

This will:
1. Load the CSV into a DataFrame with normalized column names
2. Generate a schema profile (dtypes, missingness, detected time/geo columns)
3. Register the dataset in `data/processed/registry.json`
4. Save the profile to `data/processed/sample_for_testing_profile.json`

### CLI flags

| Flag | Short | Required | Description |
|------|-------|----------|-------------|
| `--file` | `-f` | Yes | Path to the raw data file (CSV, Excel, or JSON) |
| `--name` | `-n` | Yes | A short identifier for the dataset (e.g., `irs_990_2020`) |

## Data Ingestion Pipeline
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
    4. CLEANER
       Fixes bad values, fills gaps, standardizes formats (e.g., state abbreviations)
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

