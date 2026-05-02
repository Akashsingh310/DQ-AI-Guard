# AI-Powered Data Quality Guard
## Overview

AI-Powered Data Quality Guard is a framework for monitoring and improving data quality. It combines deterministic validation rules with large language model (LLM)-based root cause analysis to provide actionable insights into data issues.

The system ingests CSV files, runs predefined quality checks, and on failure automatically invokes an AI model to explain the issues, assign severity, and recommend concrete fixes. Results are persisted as timestamped JSON reports and can be explored interactively via the monitoring dashboard.

> Note: This is not a fully completed enterprise‑grade solution. It is functional and usable, but some pragmatic design choices have been made

## High‑Level Architecture
> The diagram below shows how a developer starts, defines datasets, runs the validation + AI pipeline, and then explores results in the Streamlit dashboard.

![System Architecture](https://github.com/Akashsingh310/DQ-AI-Guard/blob/dev/img/high-level_new.png)

## Features

* **Rule‑based validation** – schema, null checks, duplicates, numeric/date types, regex patterns, min/max ranges.
* **AI root‑cause analysis** – sends failed checks and sample data to Gemini or Grok; returns structured JSON with severity, root cause, and concrete fixes.
* **Multi‑dataset support** – each CSV can have its own validation rules defined in a single YAML configuration.
* **Pipeline‑ready** – CLI with --dataset argument
* **Dashboard** – Streamlit app with dataset filtering, health score trends, pass/fail breakdowns, and downloadable run history.
* **Historical audit trail** – every run writes a timestamped JSON report, enabling long‑term data quality monitoring.
* **Polars-backed pipeline** – CSVs are read with Polars (UTF-8 string columns, streaming collection where available) and validation runs on Polars, which keeps memory use lower than a monolithic pandas load for larger files.
* **Skip unchanged runs** – after a successful pipeline run, a fingerprint of the input file (size + modification time) and of the dataset’s validation rules is stored. The next run exits immediately if nothing changed, so you do not re-validate the same data by accident. Use `--force` to run anyway (for example after changing only AI settings or when you want a fresh report timestamp).

## Installation

### Prerequisites
- Python 3.11 or 3.12
- pip
- Gemini API key (or Grok API key)

### Clone the Repository
```bash
git clone https://github.com/Akashsingh310/DQ-AI-Guard.git
cd dq-ai-guard
```

### Create and Activate Virtual Environment
```bash
python3 -m venv .venv
```

Activate (Linux/macOS):
```bash
source .venv/bin/activate
```

Activate (Windows):
```bash
.venv\Scripts\activate
```

### Install Dependencies
```bash
pip install -r requirements.txt
```

Key runtime libraries: **Polars** (main pipeline and validation), **PyYAML**, LLM clients (`google-genai`, `openai`), and **Streamlit** / **Plotly** for the dashboard. **Pandas** is included for the dashboard only; the core CLI path does not depend on pandas for loading or checks.

### Set Environment Variables

Windows
```bash
set GEMINI_API_KEY=your-key-here
```

macOS
```bash
export GEMINI_API_KEY="your-key-here"
```

> Alternatively, you can store keys in a `.env` file.

## Configuration

All settings are defined in `config/config.yaml`. Add your CSV file to the `data/` folder and update the configuration accordingly by defining the dataset, required columns, validation rules, and optional AI settings.


![Config file](https://github.com/Akashsingh310/DQ-AI-Guard/blob/dev/img/config.png)


## Running the Pipeline

Execute the pipeline from the project root:

```bash
python -m src.main --dataset customer_data
```

To **always** run validation and write a new report even when the input CSV and validation rules are unchanged (same as last run):

```bash
python -m src.main --dataset customer_data --force
```

### What Happens

- Skips work if the dataset file is unchanged (same path, size, and modification time) **and** the validation section for that dataset in `config/config.yaml` is unchanged—unless you pass `--force`. When skipped, the CLI prints the path to the last JSON report and exits successfully.
- Loads the CSV with Polars (`scan_csv` + streaming collect) using string columns end-to-end with validation.
- Runs all validation checks  
- If any check fails and AI is enabled, calls the configured LLM for root-cause analysis  
- Prints a color-coded console summary  
- Writes a JSON report to `results/dq_report_<timestamp>.json`  
- Updates `results/.dq_run_cache.json` with fingerprints used for the skip-if-unchanged behaviour (one entry per dataset name under your configured `results_dir`).

## Monitoring Dashboard

Launch the interactive dashboard:

```bash
streamlit run src/dashboard/app.py
```
>The dashboard will start at local only

### Features

- **Sidebar Filters**  
  Dataset selection (if multiple exist), time range, and option to show/hide successful runs  

- **KPI Cards**  
  Total runs, failed runs, latest health score, and current severity  

- **Trend Charts**  
  Health score line chart (color-coded by status) and pass-rate bar chart  

- **Latest Run**  
  AI root-cause analysis with severity badges and expandable details  

- **Run History**  
  Complete table with timestamps, dataset name, and health scores; downloadable as CSV  

![Monitoring](https://github.com/Akashsingh310/DQ-AI-Guard/blob/dev/img/monitoring.png)

> The dashboard is read-only and does not trigger new validation jobs.

