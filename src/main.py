"""
DQ AI Guard - Pipeline.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.ai.ai_analyzer import analyze_failures
from src.reporting.reporter import generate_report
from src.utils.config_loader import load_config, get_dataset_config, list_dataset_names
from src.utils.logger import get_logger
from src.validation.dq_validator import run_validation

load_dotenv()
logger = get_logger(__name__)

_EXIT_SUCCESS     = 0
_EXIT_FAILURE     = 1
_EXIT_FATAL_ERROR = 2


def _load_dataframe(input_file: str) -> pd.DataFrame:
    path = Path(input_file)
    if not path.exists():
        logger.error("Input file not found: %s", path)
        sys.exit(_EXIT_FATAL_ERROR)
    try:
        df = pd.read_csv(path, dtype=str)
    except Exception as exc:
        logger.error("Failed to read input file: %s", exc)
        sys.exit(_EXIT_FATAL_ERROR)
    logger.info("Data loaded: %d rows, %d columns.", len(df), len(df.columns))
    return df


def run_pipeline(config: dict[str, Any]) -> int:
    data_cfg     = config["data"]
    valid_cfg    = config["validation"]
    ai_cfg       = config.get("ai", {})
    reporting_cfg = config.get("reporting", {})

    input_file = data_cfg["input_file"]
    df = _load_dataframe(input_file)

    validation_summary = run_validation(df, valid_cfg)

    validation_summary["source_file"] = config["dataset_name"]

    failures = [r for r in validation_summary["results"] if not r["success"]]

    if not failures:
        logger.info("All validation checks passed. AI analysis skipped.")
        ai_analysis = {
            "analysis_summary": "All validation checks passed. No issues detected.",
            "issues": [],
            "overall_severity": "low",
            "data_health_score": 100,
        }
    elif ai_cfg.get("enabled", True):
        logger.info("%d check(s) failed. Invoking AI analysis.", len(failures))
        try:
            ai_analysis = analyze_failures(failures, df, ai_cfg)
        except EnvironmentError as exc:
            logger.error("AI analysis skipped: %s", exc)
            ai_analysis = {
                "error": True,
                "reason": "Missing API key",
                "detail": str(exc),
                "analysis_summary": "AI analysis skipped due to environment error.",
                "issues": [],
            }
    else:
        logger.info("AI analysis disabled in config.")
        ai_analysis = {}

    generate_report(
        validation_summary=validation_summary,
        ai_analysis=ai_analysis,
        results_dir=data_cfg["results_dir"],
        reporting_config=reporting_cfg,
    )

    return _EXIT_SUCCESS if validation_summary["overall_success"] else _EXIT_FAILURE


def main() -> None:
    parser = argparse.ArgumentParser(description="DQ AI Guard - Dataset Pipeline")
    parser.add_argument(
        "--dataset",
        type=str,
        required=True,
        help="Dataset name as defined in config.yaml (datasets list).",
    )
    args = parser.parse_args()

    logger.info("DQ AI Guard initialising for dataset: %s", args.dataset)

    try:
        raw_config = load_config()
        config = get_dataset_config(raw_config, args.dataset)
    except (FileNotFoundError, KeyError) as exc:
        logger.error("Configuration error: %s", exc)
        sys.exit(_EXIT_FATAL_ERROR)

    try:
        exit_code = run_pipeline(config)
    except Exception as exc:
        logger.exception("Unhandled exception: %s", exc)
        sys.exit(_EXIT_FATAL_ERROR)

    logger.info("Pipeline finished. Exit code %d.", exit_code)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()