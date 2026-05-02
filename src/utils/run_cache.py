"""
Skip pipeline runs when the input file and validation rules are unchanged since last run.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from src.utils.logger import get_logger

logger = get_logger(__name__)

_CACHE_FILENAME = ".dq_run_cache.json"


def _cache_file(results_dir: Path) -> Path:
    return results_dir / _CACHE_FILENAME


def _validation_hash(validation_config: dict[str, Any]) -> str:
    payload = json.dumps(validation_config, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


def _read_cache(results_dir: Path) -> dict[str, Any]:
    path = _cache_file(results_dir)
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Could not read run cache (%s): %s", path, exc)
        return {}


def _write_cache(results_dir: Path, data: dict[str, Any]) -> None:
    results_dir.mkdir(parents=True, exist_ok=True)
    path = _cache_file(results_dir)
    tmp = path.with_suffix(".tmp")
    text = json.dumps(data, indent=2, default=str)
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def dataset_unchanged_since_last_run(
    dataset_name: str,
    input_path: Path,
    validation_config: dict[str, Any],
    results_dir: Path,
) -> tuple[bool, dict[str, Any] | None]:
    """
    Return (True, cache_entry) if we can skip the pipeline; else (False, None).

    Match criteria: same resolved input path, size, mtime, and validation config hash.
    """
    if not input_path.is_file():
        return False, None

    cache = _read_cache(results_dir)
    entry = cache.get(dataset_name)
    if not entry:
        return False, None

    st = input_path.stat()
    resolved = str(input_path.resolve())
    v_hash = _validation_hash(validation_config)

    if (
        entry.get("input_path") == resolved
        and entry.get("size_bytes") == st.st_size
        and entry.get("mtime_ns") == st.st_mtime_ns
        and entry.get("validation_hash") == v_hash
    ):
        return True, entry
    return False, None


def record_successful_pipeline_run(
    dataset_name: str,
    input_path: Path,
    validation_config: dict[str, Any],
    results_dir: Path,
    report_path: Path,
) -> None:
    """Persist fingerprints after a completed run so unchanged inputs can be skipped."""
    st = input_path.stat()
    resolved = str(input_path.resolve())
    entry = {
        "input_path": resolved,
        "size_bytes": st.st_size,
        "mtime_ns": st.st_mtime_ns,
        "validation_hash": _validation_hash(validation_config),
        "last_report": report_path.name,
        "last_report_path": str(report_path.resolve()),
    }

    cache = _read_cache(results_dir)
    cache[dataset_name] = entry
    _write_cache(results_dir, cache)
    logger.debug("Updated run cache for dataset '%s'.", dataset_name)
