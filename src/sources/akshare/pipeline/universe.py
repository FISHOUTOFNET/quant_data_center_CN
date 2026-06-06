"""AkShare A-share universe helpers backed by local AkShare datasets."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from src.sources.akshare.client import normalize_akshare_code
from src.storage.parquet_store import ParquetStore


def normalize_akshare_code_list(codes: Iterable[object]) -> list[str]:
    normalized: list[str] = []
    for item in codes:
        code = normalize_akshare_code(item)
        if code:
            normalized.append(code)
    return list(dict.fromkeys(normalized))


def resolve_akshare_universe_codes(
    store: ParquetStore,
    code: tuple[str, ...] | list[str] | str | None = None,
    include_delisted: bool = False,
    context: str = "AkShare",
) -> list[str]:
    """Resolve AkShare stock codes from explicit input or local AkShare universe."""

    if isinstance(code, str):
        return normalize_akshare_code_list([code])
    if code:
        return normalize_akshare_code_list(code)

    spot_codes = _latest_dataset_codes(store.read_latest_dataset("akshare_cn_stock_spot_quote_eastmoney"))
    sh_delisted_codes = _latest_dataset_codes(store.read_latest_dataset("akshare_cn_stock_delist_sh"))
    sz_delisted_codes = _latest_dataset_codes(store.read_latest_dataset("akshare_cn_stock_delist_sz"))
    delisted_codes = list(dict.fromkeys([*sh_delisted_codes, *sz_delisted_codes]))
    if not spot_codes and not delisted_codes:
        raise ValueError(
            f"No local AkShare stock universe found for {context}; "
            "run akshare update --target spot_quote and/or akshare update --target delist first"
        )

    delisted = set(delisted_codes)
    active_spot_codes = [item for item in spot_codes if item not in delisted]
    codes = [*active_spot_codes, *delisted_codes] if include_delisted else active_spot_codes

    resolved = list(dict.fromkeys(codes))
    if not resolved:
        raise ValueError(f"No active AkShare stock codes found for {context}")
    return resolved


def resolve_akshare_valuation_universe_codes(
    store: ParquetStore,
    include_delisted: bool = False,
    context: str = "AkShare valuation",
) -> list[str]:
    """Resolve stock codes suitable for AkShare Eastmoney valuation history."""

    spot_df = store.read_latest_dataset("akshare_cn_stock_spot_quote_eastmoney")
    sh_delisted_codes = _latest_dataset_codes(store.read_latest_dataset("akshare_cn_stock_delist_sh"))
    sz_delisted_codes = _latest_dataset_codes(store.read_latest_dataset("akshare_cn_stock_delist_sz"))
    delisted_codes = list(dict.fromkeys([*sh_delisted_codes, *sz_delisted_codes]))
    if spot_df.empty and not delisted_codes:
        raise ValueError(
            f"No local AkShare stock universe found for {context}; "
            "run akshare update --target spot_quote and/or akshare update --target delist first"
        )

    active_spot_codes = _latest_valuation_active_codes(spot_df, set(delisted_codes))
    codes = [*active_spot_codes, *delisted_codes] if include_delisted else active_spot_codes

    resolved = list(dict.fromkeys(codes))
    if not resolved:
        raise ValueError(f"No active AkShare stock codes found for {context}")
    return resolved


def latest_active_akshare_codes(store: ParquetStore) -> set[str]:
    try:
        return set(resolve_akshare_universe_codes(store, include_delisted=False, context="active AkShare pool"))
    except ValueError:
        return set()


def latest_active_akshare_valuation_codes(store: ParquetStore) -> set[str]:
    try:
        return set(
            resolve_akshare_valuation_universe_codes(
                store,
                include_delisted=False,
                context="active AkShare valuation pool",
            )
        )
    except ValueError:
        return set()


def _latest_dataset_codes(df: pd.DataFrame) -> list[str]:
    if df.empty or "code" not in df.columns:
        return []
    return normalize_akshare_code_list(df["code"].dropna().astype(str).tolist())


def _latest_valuation_active_codes(df: pd.DataFrame, delisted: set[str]) -> list[str]:
    if df.empty or "code" not in df.columns:
        return []

    working = df[df["code"].notna()].copy()
    working["code"] = working["code"].map(normalize_akshare_code)
    working = working[working["code"].notna() & (working["code"] != "")]
    working = working[~working["code"].isin(delisted)]
    if "last_price" in working.columns:
        working = working[working["last_price"].notna()]
    if "name" in working.columns:
        names = working["name"].fillna("").astype(str)
        unsupported_name = names.str.contains("定转|转换", regex=True) | names.str.endswith("退")
        working = working[~unsupported_name]
    return list(dict.fromkeys(working["code"].tolist()))
