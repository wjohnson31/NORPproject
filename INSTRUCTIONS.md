# INSTRUCTIONS.md — AI Workflow Context

This file is designed to be ingested by an LLM (Claude, GPT, Gemini, etc.)
to understand how to build, run, test, and extend this project.

## What This Project Is

NORP (Nonprofit Research Pipeline) is a modular Python pipeline that:
1. Ingests raw datasets (CSV, Excel, JSON)
2. Generates schema profiles (column types, missingness, time/geo detection, column roles)
3. Sends profile + data sample to OpenAI to generate cleaning code
4. Executes cleaning code in a sandboxed environment
5. Logs every transformation for auditability
6. Registers datasets in a JSON catalog
7. Detects compatible join keys across datasets (heuristic synonym groups + LLM fallback)
8. Normalizes join key values (state names, year formats)
9. Merges datasets with post-merge validation and reporting

The end goal is an LLM-assisted contextual discovery engine
that links nonprofit financial data with socioeconomic context datasets and
autonomously surfaces correlational insights.

## Project Structure

```
norp/
├── data_pipeline/                # Main Python package
│   ├── __init__.py               # Package metadata (v0.1.0)
│   ├── __main__.py               # Enables `python -m data_pipeline`
│   ├── config.py                 # All paths, logging, .env loading
│   ├── main.py                   # CLI entry point — orchestrates full pipeline
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── loader.py             # DatasetLoader class
│   │   ├── schema.py             # SchemaProfiler class (incl. column_roles)
│   │   └── registry.py           # DatasetRegistry class
│   ├── cleaning/
│   │   ├── __init__.py
│   │   ├── agent.py              # CleaningAgent — calls OpenAI API
│   │   ├── executor.py           # SafeCleaningExecutor — sandboxed exec()
│   │   └── transform_log.py      # TransformationLog — JSON audit trail
│   └── merging/
│       ├── __init__.py
│       ├── join_detector.py      # Heuristic join key detection (synonym groups)
│       ├── join_agent.py         # LLM-based join key detection (OpenAI fallback)
│       ├── key_normalizer.py     # Join key value normalization (states, years)
│       └── merge_engine.py       # Controlled merge + validation + reporting
├── data/
│   ├── raw/                      # Input: drop raw files here
│   ├── processed/                # Output: profiles, registry, transform logs, merge reports
│   ├── cleaned/                  # Output: cleaned CSVs
│   └── merged/                   # Output: merged CSVs
├── requirements.txt              # Python dependencies
├── .env                          # API keys (not committed, gitignored)
├── .gitignore
├── README.md                     # Human-facing project docs
└── INSTRUCTIONS.md               # This file (AI-facing project docs)
```

## Environment Setup

### Prerequisites
- Python 3.11+ (tested on 3.13)
- macOS or Linux

### Setup Steps

```bash
# 1. Clone the repo
git clone https://github.com/wjohnson31/NORPproject.git
cd NORPproject

# 2. Create virtual environment
python3 -m venv .venv

# 3. Activate it
source .venv/bin/activate

# 4. Install dependencies
pip install -r requirements.txt

# 5. Set up API key for cleaning (optional)
echo 'OPENAI_API_KEY=sk-your-key-here' > .env
```

### Dependencies (requirements.txt)
- `pandas>=2.0.0` — DataFrame operations
- `openpyxl>=3.1.0` — Excel file support
- `openai>=1.0.0` — OpenAI API client
- `python-dotenv>=1.0.0` — Load .env files

## How to Run

### Full pipeline (ingest + clean)
```bash
source .venv/bin/activate
python -m data_pipeline --file data/raw/<filename>.csv --name <dataset_name>
```

### Ingest only (no cleaning)
```bash
python -m data_pipeline --file data/raw/<filename>.csv --name <dataset_name> --no-clean
```

### Ingest + merge with existing dataset
```bash
python -m data_pipeline --file data/raw/<filename>.csv --name <dataset_name> --merge-with <existing_name>
```

### Ingest + merge (swap primary/context roles)
```bash
python -m data_pipeline --file data/raw/<filename>.csv --name <dataset_name> --merge-with <existing_name> --as-context
```

### CLI Arguments
| Argument | Short | Required | Description |
|----------|-------|----------|-------------|
| `--file` | `-f` | Yes | Path to raw data file (CSV, Excel, JSON) |
| `--name` | `-n` | Yes | Identifier for the dataset |
| `--no-clean` | — | No | Skip OpenAI cleaning step |
| `--merge-with` | — | No | Name of an existing registered dataset to merge with |
| `--as-context` | — | No | Treat newly ingested dataset as context (right-side) in merge |

### Environment Variables
| Variable | Required | Description |
|----------|----------|-------------|
| `OPENAI_API_KEY` | No | Enables LLM-assisted cleaning and LLM join key detection fallback. Without it, cleaning is skipped and join detection uses heuristics only. |
| `NORP_CLEANING_MODEL` | No | Override the OpenAI model (default: `gpt-4o`). Used by both the cleaning agent and the join agent. |

## Pipeline Execution Flow

When `python -m data_pipeline --file <file> --name <name> [--merge-with <dataset>]` runs:

```
__main__.py
  → main.py: parse_args() → ingest()
    → Step 1: DatasetLoader(file_path).load()
        - Auto-detects file type from extension
        - Tries UTF-8, falls back to Latin-1
        - Normalizes column names: lowercase, strip, spaces→underscores
        - Returns pd.DataFrame

    → Step 2: SchemaProfiler(df).generate_profile()
        - Extracts: columns, dtypes, num_rows, num_columns
        - Computes: missingness % per column
        - Detects: time columns (keyword match: year, date, fiscal_year, etc.)
        - Detects: geo columns (keyword match: state, fips, zip, county, etc.)
        - Classifies: column_roles (key vs metric vs dimension)
        - Returns: dict (JSON-serializable)

    → Step 3 (if OPENAI_API_KEY set and not --no-clean):
        - CleaningAgent.generate_cleaning_code(profile, df.head(30))
            - Sends profile JSON + 30-row CSV sample to OpenAI
            - System prompt constrains output to one Python code block
            - Extracts code from markdown fencing
        - SafeCleaningExecutor.execute(df, code)
            - Runs code via exec() in restricted namespace
            - Only df, pd, np, and safe builtins available
            - No open(), __import__, os, sys, eval
            - Operates on df.copy() — original data is safe
        - TransformationLog records: code, rows before/after, status
        - Cleaned CSV written to data/cleaned/<name>_cleaned.csv
        - Transform log written to data/processed/<name>_transform_log.json

    → Step 4: DatasetRegistry.register(name, path, profile)
        - Adds/overwrites entry in data/processed/registry.json
        - Stores: file_path, schema_profile, row/col count, timestamp,
          cleaned_file_path, transform_log_path

    → Step 5: Save profile JSON
        - Written to data/processed/<name>_profile.json

    → Step 6 (if --merge-with is provided):
        - Loads target dataset from registry (prefers cleaned version)
        - Uses DatasetLoader to normalize column names consistently

        - JoinDetector.detect_join_keys(profile_left, profile_right)
            Pass 1: Synonym group matching (state/year/county/zip synonyms)
            Pass 2: Exact column name matching
            Pass 3 (if no keys found): LLM fallback via JoinAgent
              - JoinAgent sends both profiles + 5-row samples to OpenAI
              - OpenAI returns JSON array of join key recommendations
              - No API cost if heuristics succeed

        - KeyNormalizer normalizes join key values:
            - geo_state: "california" → "CA", "ga" → "GA" (50 states + DC + territories)
            - time_year: 202012 → 2020, "2020-01-15" → 2020
            - generic: lowercase, strip whitespace, collapse spaces

        - MergeEngine.merge(primary_df, context_df, join_keys)
            - Left join by default (primary keeps all rows)
            - Validates: key coverage, NaN inflation, row multiplication
            - Saves merged CSV to data/merged/<primary>_<context>_merged.csv
            - Saves report to data/processed/<primary>_<context>_merge_report.json
```

## Output Files

After a successful run, these files are created/updated:

| File | Location | Contents |
|------|----------|----------|
| Profile | `data/processed/<name>_profile.json` | Schema, dtypes, missingness, time/geo columns, column roles |
| Registry | `data/processed/registry.json` | Catalog of all ingested datasets |
| Transform log | `data/processed/<name>_transform_log.json` | Cleaning steps with code, before/after stats |
| Cleaned data | `data/cleaned/<name>_cleaned.csv` | The cleaned dataset |
| Merged data | `data/merged/<primary>_<context>_merged.csv` | The merged dataset (if --merge-with used) |
| Merge report | `data/processed/<primary>_<context>_merge_report.json` | Merge validation: key coverage, NaN %, row multiplication |

## How to Test

### Quick smoke test (no API key needed)
```bash
source .venv/bin/activate
python -m data_pipeline --file data/raw/sample_for_testing_extract.csv --name sample_for_testing --no-clean
```
Expected: INFO logs showing 10 rows, 9 columns, time columns detected
(`tax_year`, `fiscal_year_end`), geo column detected (`state`), column roles
(6 keys, 3 metrics).
Check: `data/processed/sample_for_testing_profile.json` and `registry.json` exist.

### Merge test (no API key needed for heuristic detection)
```bash
# Step 1: Register the first dataset
python -m data_pipeline --file data/raw/sample_for_testing_extract.csv --name sample_for_testing --no-clean

# Step 2: Ingest the second dataset and merge
python -m data_pipeline --file data/raw/state_unemployment_sample.csv --name state_unemployment --no-clean --merge-with sample_for_testing
```
Expected: INFO logs showing:
- `Detected 2 join key pair(s): [('state', 'state', 'geo_state'), ('year', 'tax_year', 'time_year')]`
- `Merge complete — 10 rows, 12 columns`
- `Key coverage: {'state': 1.0, 'year': 1.0}`
Check: `data/merged/state_unemployment_sample_for_testing_merged.csv` and
`data/processed/state_unemployment_sample_for_testing_merge_report.json` exist.

### Full pipeline test (requires OPENAI_API_KEY in .env)
```bash
python -m data_pipeline --file data/raw/sample_for_testing_extract.csv --name sample_for_testing
```
Expected: Same as above, plus cleaned CSV in `data/cleaned/` and transform
log in `data/processed/`.

### Verify individual components in Python REPL
```python
from data_pipeline.ingestion.loader import DatasetLoader
from data_pipeline.ingestion.schema import SchemaProfiler
from data_pipeline.ingestion.registry import DatasetRegistry

# Load
df = DatasetLoader('data/raw/sample_for_testing_extract.csv').load()
print(df.columns.tolist())  # Should be lowercase with underscores

# Profile
profile = SchemaProfiler(df).generate_profile()
print(profile['time_columns'])  # ['tax_year', 'fiscal_year_end']
print(profile['geo_columns'])   # ['state']

# Registry
registry = DatasetRegistry()
print(registry.list_datasets())  # Shows all registered datasets
```

## Key Design Decisions

1. **Column normalization at ingestion** — All column names are lowercased
   with underscores at load time so downstream code never deals with
   inconsistent naming.

2. **Profile-driven cleaning** — The schema profile is the input to the LLM,
   not the raw data. This gives the model structured context (dtypes,
   missingness %, time/geo hints) to make smart cleaning decisions.

3. **Sandboxed execution** — LLM-generated code runs in a restricted
   `exec()` with only `df`, `pd`, `np`, and safe builtins. No filesystem,
   network, or import access. Operates on a DataFrame copy.

4. **JSON persistence** — Registry and profiles are plain JSON files.
   No database dependency. Easy to inspect, version, and parse.

5. **Graceful degradation** — If `OPENAI_API_KEY` is not set, the pipeline
   runs without cleaning and uses heuristic-only join detection. If cleaning
   code fails, the original data is preserved. If merge fails, the primary
   dataset is returned unchanged.

6. **Heuristic-first join detection** — The JoinDetector uses predefined
   synonym groups (time_year, geo_state, etc.) first, which is fast and
   costs nothing. Only falls back to the LLM when heuristics find no keys.
   This keeps API costs low for standard datasets.

7. **Non-destructive normalization** — Key normalization operates on copies
   of the DataFrames, never modifying the original cleaned data.

8. **Merge validation** — Every merge produces a JSON report checking key
   coverage, NaN inflation in context columns, and row multiplication.
   This ensures merges are auditable and quality is measurable.

## How to Extend This Project

### Adding a new ingestion format
Edit `data_pipeline/ingestion/loader.py`:
- Add the extension to `_EXTENSION_MAP`
- Add a `_load_<format>()` method
- Add it to the dispatch dict in `load()`

### Adding new profile heuristics
Edit `data_pipeline/ingestion/schema.py`:
- Add keywords to `_TIME_KEYWORDS` or `_GEO_KEYWORDS`
- Or add a new detection method (e.g., `_detect_id_columns()`)
- Add keywords to `_ID_KEYWORDS` for column role classification

### Adding new join key synonym groups
Edit `data_pipeline/merging/join_detector.py`:
- Add a new synonym set (e.g., `_GEO_DISTRICT_SYNONYMS`)
- Add it to the `_SYNONYM_GROUPS` list

### Adding new key normalization types
Edit `data_pipeline/merging/key_normalizer.py`:
- Add a new static method (e.g., `_normalize_county()`)
- Add the dispatch case in `normalize_column()`

### Adding a new pipeline step
1. Create a new module in `data_pipeline/` (e.g., `query_agent/`)
2. Add the step to `ingest()` in `data_pipeline/main.py`
3. Optionally add a CLI flag to skip it

### Planned future modules (not yet implemented)
- **Query Agent** (W7–W8) — Generate correlational queries across merged datasets
- **Interpretive Layer** (W9–W10) — Rank and filter findings, generate visualizations

## Common Issues

| Problem | Solution |
|---------|----------|
| `OPENAI_API_KEY not set; skipping cleaning` | Add key to `.env` file or `export OPENAI_API_KEY=...` |
| `openai package not installed` | Run `pip install -r requirements.txt` in venv |
| `ModuleNotFoundError: dotenv` | Run `pip install python-dotenv` in venv |
| `UnicodeDecodeError` | Loader auto-falls back to Latin-1; if both fail, check file encoding |
| Cleaning code crashes | Original data is preserved; check transform log for error details |
