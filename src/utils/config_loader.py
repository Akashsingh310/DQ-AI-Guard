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
    """Load, validate and resolve paths from YAML configuration."""
    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}. "
            "Ensure config/config.yaml exists at the project root."
        )

    logger.info("Loading configuration from: %s", config_path)

    with config_path.open("r", encoding="utf-8") as fh:
        config: dict[str, Any] = yaml.safe_load(fh)

    _validate_required_sections(config)
    _resolve_paths(config)

    logger.debug("Configuration loaded. Top-level keys: %s", list(config.keys()))
    return config


def _validate_required_sections(config: dict[str, Any]) -> None:
    required = ("data", "validation", "ai", "reporting")
    missing = [k for k in required if k not in config]
    if missing:
        raise KeyError(
            f"Configuration is missing required section(s): {missing}. "
            "Check config/config.yaml."
        )


def _resolve_paths(config: dict[str, Any]) -> None:
    data_block = config.get("data", {})
    for key in _RELATIVE_PATH_KEYS:
        if key in data_block:
            raw = data_block[key]
            resolved = str(_PROJECT_ROOT / raw)
            data_block[key] = resolved
            logger.debug("Resolved path '%s': %s -> %s", key, raw, resolved)