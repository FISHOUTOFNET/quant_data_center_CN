"""Shared AkShare endpoint normalization helpers."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime

import pandas as pd

from src.api.akshare.errors import AkShareSchemaDriftError


def date_iso(value: str | date | datetime | None, default: str) -> str:
    if value is None:
        return default
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    parsed = pd.to_datetime(value, errors="raise")
    return parsed.date().isoformat()


def akshare_date(value: str | date | datetime) -> str:
    return date_iso(value, datetime.now().date().isoformat()).replace("-", "")


def normalize_adjustment(adjustment: str) -> str:
    normalized = str(adjustment).strip().lower()
    if normalized in {"", "none", "不复权"}:
        return "unadjusted"
    if normalized == "unadjusted":
        return normalized
    if normalized not in {"qfq", "hfq"}:
        raise ValueError(f"Unsupported AkShare daily bar adjustment: {adjustment}")
    return normalized


def to_numeric(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.to_numeric(series, errors="coerce")
    values = series.mask(series.isin(["", "-", "--", "None", "nan"]), pd.NA)
    return pd.to_numeric(values, errors="coerce")


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result.columns = [str(column).strip() for column in result.columns]
    return result


def select_required_columns(
    df: pd.DataFrame,
    aliases: Mapping[str, tuple[str, ...]],
    endpoint: str,
) -> pd.DataFrame:
    missing: list[str] = []
    selected = pd.DataFrame(index=df.index)
    columns = set(df.columns)
    for target, candidates in aliases.items():
        source = next((candidate for candidate in candidates if candidate in columns), None)
        if source is None:
            missing.append(target)
            continue
        selected[target] = df[source]
    if missing:
        raise AkShareSchemaDriftError(f"{endpoint} missing required fields: {missing}; actual={list(df.columns)}")
    return selected
