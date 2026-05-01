"""
YAML configuration loader with path resolution.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from src.utils.logger import get_logger

logger = get_logger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_CONFIG_PATH = _PROJECT_ROOT / "config" / "config.yaml"

_RELATIVE_PATH_KEYS = ("input_file", "results_dir")


def load_config(config_path: Path = _DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    """Load raw YAML configuration (without merging)."""
    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}. "
            "Ensure config/config.yaml exists at the project root."
        )

    logger.info("Loading configuration from: %s", config_path)

    with config_path.open("r", encoding="utf-8") as fh:
        config: dict[str, Any] = yaml.safe_load(fh)

    _validate_required_sections(config)
    return config


def _validate_required_sections(config: dict[str, Any]) -> None:
    required = ("datasets", "ai", "reporting")
    missing = [k for k in required if k not in config]
    if missing:
        raise KeyError(
            f"Configuration is missing required section(s): {missing}. "
            "Check config/config.yaml."
        )


def _resolve_paths(merged_config: dict[str, Any]) -> None:
    """Resolve relative paths inside a per‑dataset config dict."""
    data_block = merged_config.get("data", {})
    for key in _RELATIVE_PATH_KEYS:
        if key in data_block:
            raw = data_block[key]
            resolved = str(_PROJECT_ROOT / raw)
            data_block[key] = resolved
            logger.debug("Resolved path '%s': %s -> %s", key, raw, resolved)


def get_dataset_config(config: dict[str, Any], dataset_name: str) -> dict[str, Any]:
    """
    Return a fully resolved configuration for a specific dataset.
    Merges global AI / reporting / logging with the dataset's data and validation.
    """
    datasets = config.get("datasets", [])
    ds = next((d for d in datasets if d["name"] == dataset_name), None)
    if not ds:
        raise KeyError(f"Dataset '{dataset_name}' not found in configuration.")

    merged = {
        "data": {
            "input_file": ds["input_file"],
            "results_dir": ds.get("results_dir", "results"),
        },
        "validation": ds.get("validation", config.get("validation", {})),
        "ai": config.get("ai", {}),
        "reporting": config.get("reporting", {}),
        "logging": config.get("logging", {}),
        "dataset_name": ds["name"],
    }
    _resolve_paths(merged)
    return merged


def list_dataset_names(config: dict[str, Any]) -> list[str]:
    """Return all dataset names defined in the configuration."""
    return [d["name"] for d in config.get("datasets", [])]