"""
Console and JSON reporting.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.utils.logger import get_logger

logger = get_logger(__name__)


class _Colour:
    _IS_TTY = sys.stdout.isatty()
    RESET  = "\033[0m"   if _IS_TTY else ""
    BOLD   = "\033[1m"   if _IS_TTY else ""
    DIM    = "\033[2m"   if _IS_TTY else ""
    RED    = "\033[91m"  if _IS_TTY else ""
    YELLOW = "\033[93m"  if _IS_TTY else ""
    GREEN  = "\033[92m"  if _IS_TTY else ""
    CYAN   = "\033[96m"  if _IS_TTY else ""


_SEVERITY_COLOUR = {
    "high":   _Colour.RED,
    "medium": _Colour.YELLOW,
    "low":    _Colour.GREEN,
}


def _kv(label: str, value: str, indent: int = 2) -> str:
    pad = " " * indent
    return f"{pad}{_Colour.DIM}{label:<22}{_Colour.RESET}{value}"


def _section_header(title: str, width: int) -> str:
    bar = "-" * width
    return (
        f"\n{_Colour.BOLD}{_Colour.CYAN}{bar}{_Colour.RESET}\n"
        f"{_Colour.BOLD}{_Colour.CYAN}  {title}{_Colour.RESET}\n"
        f"{_Colour.BOLD}{_Colour.CYAN}{bar}{_Colour.RESET}"
    )


def _print_validation_section(summary: dict[str, Any], width: int, show_passed: bool) -> None:
    print(_section_header("VALIDATION RESULTS", width))

    overall = summary["overall_success"]
    print(_kv("Run timestamp:", summary.get("run_timestamp", "N/A")))
    print(_kv(
        "Overall status:",
        f"{_Colour.GREEN}PASSED{_Colour.RESET}" if overall else f"{_Colour.RED}FAILED{_Colour.RESET}"
    ))
    print(_kv("Total checks:", str(summary["total_checks"])))
    print(_kv("Passed:", f"{_Colour.GREEN}{summary['passed_checks']}{_Colour.RESET}"))
    print(_kv("Failed:", f"{_Colour.RED}{summary['failed_checks']}{_Colour.RESET}"))

    # Table header
    col_check  = 48
    col_status = 8
    col_failed = 9
    col_total  = 9
    col_pct    = 8
    header = (
        f"  {'CHECK':<{col_check}} "
        f"{'STATUS':<{col_status}} "
        f"{'FAILED':>{col_failed}} "
        f"{'TOTAL':>{col_total}} "
        f"{'% FAIL':>{col_pct}}"
    )
    print(f"\n{_Colour.BOLD}{header}{_Colour.RESET}")
    print(f"  {'-' * (col_check + col_status + col_failed + col_total + col_pct + 4)}")

    for result in summary["results"]:
        if not show_passed and result["success"]:
            continue
        colour = _Colour.GREEN if result["success"] else _Colour.RED
        status = "PASS" if result["success"] else "FAIL"
        print(
            f"  {result['check_name']:<{col_check}} "
            f"{colour}{status:<{col_status}}{_Colour.RESET} "
            f"{result['failed_count']:>{col_failed}} "
            f"{result['total_count']:>{col_total}} "
            f"{result['percentage_failed']:>{col_pct}.2f}%"
        )


def _print_ai_section(analysis: dict[str, Any], width: int) -> None:
    print(_section_header("AI ROOT-CAUSE ANALYSIS", width))

    if analysis.get("error"):
        print(f"\n  {_Colour.RED}Analysis unavailable{_Colour.RESET}: {analysis.get('reason')}")
        if detail := analysis.get("detail"):
            print(f"  Detail: {detail}")
        return

    print(f"\n{_kv('Summary:', analysis.get('analysis_summary', 'N/A'))}")
    sev = analysis.get("overall_severity", "N/A")
    colour = _SEVERITY_COLOUR.get(sev, "")
    print(_kv("Overall severity:", f"{colour}{sev.upper()}{_Colour.RESET}"))

    score = analysis.get("data_health_score")
    if score is not None:
        sc_col = _Colour.GREEN if score >= 80 else _Colour.YELLOW if score >= 50 else _Colour.RED
        print(_kv("Data health score:", f"{sc_col}{score} / 100{_Colour.RESET}"))

    issues = analysis.get("issues", [])
    if not issues:
        print("\n  No specific issues reported.")
        return

    print()
    for idx, issue in enumerate(issues, 1):
        sev_issue = issue.get("severity", "unknown")
        sev_colour = _SEVERITY_COLOUR.get(sev_issue, "")
        check = issue.get("check_name", "N/A")
        print(
            f"  {_Colour.BOLD}[{idx:02d}] {sev_colour}{sev_issue.upper()}{_Colour.RESET}"
            f"  {check}"
        )
        print(_kv("Summary:", issue.get("issue_summary", "N/A"), indent=7))
        print(_kv("Root cause:", issue.get("root_cause", "N/A"), indent=7))
        print(_kv("Recommended fix:", issue.get("recommended_fix", "N/A"), indent=7))
        if col := issue.get("affected_column"):
            print(_kv("Affected column:", col, indent=7))
        if ex := issue.get("example_bad_values"):
            print(_kv("Example bad values:", ", ".join(str(v) for v in ex), indent=7))
        print()


def generate_report(
    validation_summary: dict[str, Any],
    ai_analysis: dict[str, Any],
    results_dir: str,
    reporting_config: dict[str, Any] | None = None,
) -> Path:
    
    cfg = reporting_config or {}
    console_cfg = cfg.get("console", {})
    json_cfg = cfg.get("json", {})
    width = int(console_cfg.get("width", 80))
    show_passed = bool(console_cfg.get("show_passed_checks", True))
    indent = int(json_cfg.get("indent", 2))

    _print_validation_section(validation_summary, width, show_passed)
    if ai_analysis:
        _print_ai_section(ai_analysis, width)

    # JSON file
    out_dir = Path(results_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_path = out_dir / f"dq_report_{timestamp}.json"

    payload = {
        "schema_version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "validation": validation_summary,
        "ai_analysis": ai_analysis,
    }
    with report_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=indent, default=str, ensure_ascii=False)

    logger.info("JSON report saved to %s", report_path)
    print(_section_header("REPORT OUTPUT", width))
    print(f"\n  JSON report: {report_path}\n")
    return report_path