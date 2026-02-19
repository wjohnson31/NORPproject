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

## Modules

### `DatasetLoader` (`ingestion/loader.py`)
Loads CSV, Excel, or JSON files with automatic type detection and encoding
fallback (UTF-8 → Latin-1). Normalizes column names to lowercase with
underscores. Returns a raw pandas DataFrame — no values are modified.

### `SchemaProfiler` (`ingestion/schema.py`)
Extracts column names, dtypes, row/column counts, and per-column missingness.
Uses keyword heuristics to flag potential time columns (`year`, `date`,
`fiscal_year`, etc.) and geography columns (`state`, `fips`, `zip`, etc.).

### `DatasetRegistry` (`ingestion/registry.py`)
Maintains a persistent JSON registry of all ingested datasets with file paths,
schema profiles, timestamps, and shape information. Re-ingesting with the
same name overwrites the previous entry.

## Testing the Ingestion Pipeline

Activate the virtual environment first: `source .venv/bin/activate`

### Full Pipeline (End-to-End)

```bash
python -m data_pipeline --file data/raw/sample_for_testing_extract.csv --name sample_for_testing
```

Look for: INFO logs confirming load success, 10 rows / 9 columns,
detected time columns (`tax_year`, `fiscal_year_end`), detected geo column
(`state`), and profile saved. After running, `data/processed/` should contain
`sample_for_testing_profile.json` and `registry.json`.

## Roadmap

| Phase | Focus |
|-------|-------|
| **W1–W2** | Ingestion skeleton (complete) |
| **W3** | Cleaning & harmonization, join compatibility detection |
| **W4** | Autonomous contextual discovery, LLM orchestration |
