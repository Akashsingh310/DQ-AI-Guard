"""
Load CSV datasets with Polars — UTF-8 columns, streaming collect for large files.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from src.utils.logger import get_logger

logger = get_logger(__name__)


def load_csv_dataset(path: str | Path) -> pl.DataFrame:
    """
    Read a CSV with every column as Utf8 (same spirit as pandas ``dtype=str``).

    Uses ``scan_csv`` + streaming engine where supported so large files are
    processed in chunks instead of one monolithic pandas allocation.
    """
    path = Path(path)
    header = pl.read_csv(path, n_rows=0)
    utf8_schema = {name: pl.Utf8 for name in header.columns}
    lf = pl.scan_csv(
        path,
        schema_overrides=utf8_schema,
        try_parse_dates=False,
        encoding="utf8",
    )
    try:
        df = lf.collect(engine="streaming")
    except TypeError:
        df = lf.collect(streaming=True)
    logger.info("Data loaded: %d rows, %d columns.", df.height, len(df.columns))
    return df
