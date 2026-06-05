"""AkShare code and source-symbol normalization."""

from __future__ import annotations

import re
from typing import Any, cast

import pandas as pd

AKSHARE_PREFIXED_CODE_PATTERN = re.compile(r"^(?P<market>sh|sz|bj)[\.\s_-]?(?P<symbol>\d{6})$", re.IGNORECASE)
AKSHARE_STORAGE_CODE_PATTERN = re.compile(r"^\d{6}$")


def normalize_akshare_code(code: object) -> str:
    """Validate and return a 6-digit AkShare code for explicit user input."""

    if pd.isna(cast(Any, code)):
        raise ValueError("AkShare stock code must be a 6-digit string")
    value = str(code).strip()
    if not AKSHARE_STORAGE_CODE_PATTERN.fullmatch(value):
        raise ValueError(f"AkShare stock code must be 6 digits, got: {value!r}")
    return value


def normalize_source_code(symbol: object) -> str:
    """Normalize source-provided AkShare/Sina code shapes to storage code."""

    value = clean_source_symbol(symbol)
    if value == "":
        return ""
    prefixed_match = AKSHARE_PREFIXED_CODE_PATTERN.match(value.lower())
    if prefixed_match is not None:
        return prefixed_match.group("symbol")
    return value.zfill(6) if value.isdigit() else value


def clean_source_symbol(symbol: object) -> str:
    if pd.isna(cast(Any, symbol)):
        return ""
    value = str(symbol).strip()
    if re.fullmatch(r"\d+\.0", value):
        value = value.split(".", 1)[0]
    return value
