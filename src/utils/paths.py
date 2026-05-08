"""Centralized filesystem paths for the local data center."""

from __future__ import annotations

import os
from pathlib import Path


def project_root() -> Path:
    """Return the repository root.

    QDC_ROOT is useful for tests and one-off scripts; otherwise this file lives
    under <root>/src/utils/paths.py, so parents[2] is the project root.
    """

    env_root = os.getenv("QDC_ROOT")
    if env_root:
        return Path(env_root).expanduser().resolve()
    return Path(__file__).resolve().parents[2]


ROOT = project_root()
CONFIG_DIR = ROOT / "config"
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PARQUET_DIR = DATA_DIR / "parquet"
METADATA_DIR = DATA_DIR / "metadata"
DUCKDB_DIR = DATA_DIR / "duckdb"
LOGS_DIR = ROOT / "logs"
DUCKDB_FILE = DUCKDB_DIR / "quant.duckdb"


def resolve_path(path: str | Path, base: Path | None = None) -> Path:
    """Resolve a config path relative to the project root unless absolute."""

    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    return ((base or ROOT) / candidate).resolve()


def ensure_dir(path: str | Path) -> Path:
    """Create a directory and return it as a resolved Path."""

    directory = resolve_path(path) if not isinstance(path, Path) else path
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def parquet_dataset_dir(dataset: str, root: Path | None = None) -> Path:
    """Return the Parquet directory for a named dataset."""

    base = root or PARQUET_DIR
    return base / dataset


def baostock_daily_bar_file(dataset: str, code: str, root: Path | None = None) -> Path:
    """Return the single Parquet file for a daily_bar dataset and stock code."""

    return parquet_dataset_dir(dataset, root) / f"code={code}" / "data.parquet"


def baostock_cn_trading_calendar_file(root: Path | None = None) -> Path:
    """Return the trading baostock_cn_trading_calendar Parquet file."""

    return parquet_dataset_dir("baostock_cn_trading_calendar", root) / "data.parquet"
