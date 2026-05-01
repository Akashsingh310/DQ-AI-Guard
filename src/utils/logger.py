"""
Centralised logging for DQ AI Guard.
"""

import logging
import sys
from typing import Optional


_DEFAULT_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_logger(
    name: str,
    level: Optional[str] = None,
    fmt: str = _DEFAULT_FORMAT,
    date_fmt: str = _DEFAULT_DATE_FORMAT,
) -> logging.Logger:
    """Return a configured logger with a single stdout stream handler."""
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    resolved_level = getattr(logging, (level or "INFO").upper(), logging.INFO)
    logger.setLevel(resolved_level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(resolved_level)
    handler.setFormatter(logging.Formatter(fmt=fmt, datefmt=date_fmt))

    logger.addHandler(handler)
    logger.propagate = False

    return logger