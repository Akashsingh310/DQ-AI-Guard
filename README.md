# AI-Powered Data Quality Guard
## Overview

AI-Powered Data Quality Guard is a framework for monitoring and improving data quality. It combines deterministic validation rules with large language model (LLM)-based root cause analysis to provide actionable insights into data issues.

The system ingests CSV files, runs predefined quality checks, and on failure automatically invokes an AI model to explain the issues, assign severity, and recommend concrete fixes.Results are persisted as timestamped JSON reports and can be explored interactively via monitoring dashboard.

## High‑Level Architecture
> The diagram below shows how a developer starts, defines datasets, runs the validation + AI pipeline, and then explores results in the Streamlit dashboard.

![System Architecture](https://github.com/Akashsingh310/DQ-AI-Guard/blob/main/img/high-level.png)

## Features

* Rule‑based validation – schema, null checks, duplicates, numeric/date types, regex patterns, min/max ranges.
* AI root‑cause analysis – sends failed checks and sample data to Gemini or Grok; returns structured JSON with severity, root cause, and concrete fixes.
* Multi‑dataset support – each CSV can have its own validation rules defined in a single YAML configuration.
* Pipeline‑ready – CLI with --dataset argument
* Dashboard – Streamlit app with dataset filtering, health score trends, pass/fail breakdowns, and downloadable run history.
* Historical audit trail – every run writes a timestamped JSON report, enabling long‑term data quality monitoring.

## Installation

### Prerequisites
- Python 3.11 or 3.12
- pip or conda
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

### Set Environment Variables
```bash
export GEMINI_API_KEY="your-key-here"
```

> Alternatively, you can store keys in a `.env` file.
