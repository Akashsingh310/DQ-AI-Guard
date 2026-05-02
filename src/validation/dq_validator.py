"""
Deterministic rule-based data quality checks.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

import polars as pl

from src.utils.logger import get_logger

logger = get_logger(__name__)


def _make_result(
    check_name: str,
    success: bool,
    failed_count: int,
    total_count: int,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Standardised validation result dictionary."""
    pct = round((failed_count / total_count) * 100, 4) if total_count else 0.0
    return {
        "check_name": check_name,
        "success": success,
        "failed_count": failed_count,
        "total_count": total_count,
        "percentage_failed": pct,
        "details": details or {},
    }


def _check_schema(
    df: pl.DataFrame, required_columns: list[str]
) -> list[dict[str, Any]]:
    missing = [c for c in required_columns if c not in df.columns]
    return [
        _make_result(
            check_name="schema::required_columns",
            success=len(missing) == 0,
            failed_count=len(missing),
            total_count=len(required_columns),
            details={"missing_columns": missing},
        )
    ]


def _check_nulls(
    df: pl.DataFrame, required_columns: list[str], max_null_pct: float
) -> list[dict[str, Any]]:
    results = []
    total = df.height
    for col in required_columns:
        if col not in df.columns:
            continue
        null_cnt = int(df.select(pl.col(col).null_count()).item())
        actual_pct = null_cnt / total if total else 0.0
        results.append(
            _make_result(
                check_name=f"null::{col}",
                success=actual_pct <= max_null_pct,
                failed_count=null_cnt,
                total_count=total,
                details={
                    "column": col,
                    "actual_null_pct": round(actual_pct * 100, 4),
                    "threshold_pct": round(max_null_pct * 100, 4),
                },
            )
        )
    return results


def _check_duplicates(
    df: pl.DataFrame, max_dup_pct: float
) -> list[dict[str, Any]]:
    total = df.height
    dup_cnt = int(df.is_duplicated().sum())
    actual_pct = dup_cnt / total if total else 0.0
    return [
        _make_result(
            check_name="duplicate::all_columns",
            success=actual_pct <= max_dup_pct,
            failed_count=dup_cnt,
            total_count=total,
            details={
                "actual_duplicate_pct": round(actual_pct * 100, 4),
                "threshold_pct": round(max_dup_pct * 100, 4),
            },
        )
    ]


def _check_numeric_types(
    df: pl.DataFrame, numeric_columns: list[str], max_non_numeric_pct: float
) -> list[dict[str, Any]]:
    results = []
    total = df.height
    for col in numeric_columns:
        if col not in df.columns:
            continue
        parsed = df[col].cast(pl.Float64, strict=False)
        original_notna = df[col].is_not_null()
        coerced_na = parsed.is_null()
        failed_cnt = int((original_notna & coerced_na).sum())
        actual_pct = failed_cnt / total if total else 0.0
        results.append(
            _make_result(
                check_name=f"type::numeric::{col}",
                success=actual_pct <= max_non_numeric_pct,
                failed_count=failed_cnt,
                total_count=total,
                details={
                    "column": col,
                    "non_numeric_pct": round(actual_pct * 100, 4),
                    "threshold_pct": round(max_non_numeric_pct * 100, 4),
                },
            )
        )
    return results


def _check_date_columns(
    df: pl.DataFrame, date_columns: list[str]
) -> list[dict[str, Any]]:
    """
    Verify that declared date columns contain parsable date/datetime strings.

    Uses Polars ``str.to_datetime`` with ``strict=False``; non-null values that
    fail to parse are counted as failures.
    """
    results: list[dict[str, Any]] = []
    total = df.height

    for col in date_columns:
        if col not in df.columns:
            continue

        s = df[col]
        parsed = s.str.to_datetime(strict=False)
        original_non_null = s.is_not_null()
        failed_count = int((original_non_null & parsed.is_null()).sum())
        actual_pct = failed_count / total if total else 0.0

        results.append(
            _make_result(
                check_name=f"type::date::{col}",
                success=failed_count == 0,
                failed_count=failed_count,
                total_count=total,
                details={
                    "column": col,
                    "unparsable_pct": round(actual_pct * 100, 4),
                },
            )
        )

    return results


def _check_patterns(
    df: pl.DataFrame, pattern_config: dict[str, str]
) -> list[dict[str, Any]]:
    results = []
    for col, pattern in pattern_config.items():
        if col not in df.columns:
            logger.warning("Pattern check skipped: column '%s' not found.", col)
            continue
        try:
            compiled = re.compile(pattern)
        except re.error as exc:
            logger.error("Invalid regex for column '%s': %s", col, exc)
            continue
        non_null = df.filter(pl.col(col).is_not_null())
        # Match pandas ``Series.str.match`` (``re.match`` from string start); Polars has no ``str.match``.
        failed_cnt = non_null.filter(
            pl.col(col).map_elements(
                lambda s: compiled.match(str(s)) is None,
                return_dtype=pl.Boolean,
            )
        ).height
        total = non_null.height
        results.append(
            _make_result(
                check_name=f"pattern::{col}",
                success=failed_cnt == 0,
                failed_count=failed_cnt,
                total_count=total,
                details={"column": col, "pattern": pattern},
            )
        )
    return results


def _check_ranges(
    df: pl.DataFrame, range_config: dict[str, dict[str, float]]
) -> list[dict[str, Any]]:
    results = []
    total = df.height
    for col, bounds in range_config.items():
        if col not in df.columns:
            logger.warning("Range check skipped: column '%s' not found.", col)
            continue
        min_val = bounds.get("min", float("-inf"))
        max_val = bounds.get("max", float("inf"))
        numeric_series = df[col].cast(pl.Float64, strict=False)
        out_of_range = int(
            (
                numeric_series.is_not_null()
                & ((numeric_series < min_val) | (numeric_series > max_val))
            ).sum()
        )
        results.append(
            _make_result(
                check_name=f"range::{col}",
                success=out_of_range == 0,
                failed_count=out_of_range,
                total_count=total,
                details={
                    "column": col,
                    "min_allowed": min_val,
                    "max_allowed": max_val,
                },
            )
        )
    return results


def run_validation(
    df: pl.DataFrame, validation_config: dict[str, Any]
) -> dict[str, Any]:
    """Execute full validation suite and return a structured summary."""
    logger.info(
        "Validation suite started. Rows: %d, Columns: %d.",
        df.height,
        len(df.columns),
    )

    thresholds = validation_config.get("thresholds", {})
    columns_cfg = validation_config.get("columns", {})
    range_cfg = validation_config.get("ranges", {})
    pattern_cfg = validation_config.get("patterns", {})

    required_cols = columns_cfg.get("required", [])
    numeric_cols = columns_cfg.get("numeric", [])
    date_cols = columns_cfg.get("date", [])

    max_null_pct = thresholds.get("max_null_percentage", 0.05)
    max_dup_pct = thresholds.get("max_duplicate_percentage", 0.02)
    max_non_num_pct = thresholds.get("max_non_numeric_percentage", 0.00)

    all_results: list[dict[str, Any]] = []

    # Run in dependency order
    all_results.extend(_check_schema(df, required_cols))
    all_results.extend(_check_nulls(df, required_cols, max_null_pct))
    all_results.extend(_check_duplicates(df, max_dup_pct))
    if numeric_cols:
        all_results.extend(_check_numeric_types(df, numeric_cols, max_non_num_pct))
    if date_cols:
        all_results.extend(_check_date_columns(df, date_cols))
    if pattern_cfg:
        all_results.extend(_check_patterns(df, pattern_cfg))
    if range_cfg:
        all_results.extend(_check_ranges(df, range_cfg))

    total_checks = len(all_results)
    passed = sum(1 for r in all_results if r["success"])
    failed = total_checks - passed

    summary = {
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "total_checks": total_checks,
        "passed_checks": passed,
        "failed_checks": failed,
        "overall_success": failed == 0,
        "results": all_results,
    }

    logger.info(
        "Validation completed. Passed: %d/%d. Overall success: %s.",
        passed,
        total_checks,
        failed == 0,
    )
    return summary
