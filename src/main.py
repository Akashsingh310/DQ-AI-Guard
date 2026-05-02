"""
DQ AI Guard - Pipeline.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.data.loader import load_csv_dataset
from src.ai.ai_analyzer import analyze_failures
from src.reporting.reporter import generate_report
from src.utils.config_loader import load_config, get_dataset_config, list_dataset_names
from src.utils.logger import get_logger
from src.utils.run_cache import (
    dataset_unchanged_since_last_run,
    record_successful_pipeline_run,
)
from src.validation.dq_validator import run_validation

load_dotenv()
logger = get_logger(__name__)

_EXIT_SUCCESS     = 0
_EXIT_FAILURE     = 1
_EXIT_FATAL_ERROR = 2


def run_pipeline(config: dict[str, Any]) -> int:
    data_cfg     = config["data"]
    valid_cfg    = config["validation"]
    ai_cfg       = config.get("ai", {})
    reporting_cfg = config.get("reporting", {})

    input_file = data_cfg["input_file"]
    path = Path(input_file)
    if not path.exists():
        logger.error("Input file not found: %s", path)
        sys.exit(_EXIT_FATAL_ERROR)
    try:
        df = load_csv_dataset(path)
    except Exception as exc:
        logger.error("Failed to read input file: %s", exc)
        sys.exit(_EXIT_FATAL_ERROR)

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

    report_path = generate_report(
        validation_summary=validation_summary,
        ai_analysis=ai_analysis,
        results_dir=data_cfg["results_dir"],
        reporting_config=reporting_cfg,
    )

    record_successful_pipeline_run(
        dataset_name=config["dataset_name"],
        input_path=path,
        validation_config=valid_cfg,
        results_dir=Path(data_cfg["results_dir"]),
        report_path=report_path,
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
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run even if the input file and validation config match the last run.",
    )
    args = parser.parse_args()

    logger.info("DQ AI Guard initialising for dataset: %s", args.dataset)

    try:
        raw_config = load_config()
        config = get_dataset_config(raw_config, args.dataset)
    except (FileNotFoundError, KeyError) as exc:
        logger.error("Configuration error: %s", exc)
        sys.exit(_EXIT_FATAL_ERROR)

    data_cfg = config["data"]
    input_path = Path(data_cfg["input_file"])
    results_dir = Path(data_cfg["results_dir"])
    valid_cfg = config["validation"]

    if not args.force and input_path.is_file():
        unchanged, cache_entry = dataset_unchanged_since_last_run(
            config["dataset_name"],
            input_path,
            valid_cfg,
            results_dir,
        )
        if unchanged and cache_entry:
            last_report = cache_entry.get("last_report_path") or cache_entry.get(
                "last_report", ""
            )
            logger.info(
                "Skipping pipeline: input file and validation config unchanged since last run."
            )
            print(
                "\n  Dataset unchanged (same file size, modification time, and validation rules).\n"
                f"  Last report: {last_report}\n"
                "  Run with --force to re-run validation and regenerate the report.\n"
            )
            sys.exit(_EXIT_SUCCESS)

    try:
        exit_code = run_pipeline(config)
    except Exception as exc:
        logger.exception("Unhandled exception: %s", exc)
        sys.exit(_EXIT_FATAL_ERROR)

    logger.info("Pipeline finished. Exit code %d.", exit_code)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()