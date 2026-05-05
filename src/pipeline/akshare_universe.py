"""AkShare A-share universe helpers backed by local AkShare datasets."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from src.api.akshare_client import normalize_akshare_code
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

    spot_codes = _latest_dataset_codes(store.read_latest_stock_zh_a_spot_em())
    sh_delisted_codes = _latest_dataset_codes(store.read_latest_stock_info_sh_delist())
    sz_delisted_codes = _latest_dataset_codes(store.read_latest_stock_info_sz_delist())
    delisted_codes = list(dict.fromkeys([*sh_delisted_codes, *sz_delisted_codes]))
    if not spot_codes and not delisted_codes:
        raise ValueError(
            f"No local AkShare stock universe found for {context}; "
            "run update-akshare-spot and/or update-akshare-delist first"
        )

    delisted = set(delisted_codes)
    active_spot_codes = [item for item in spot_codes if item not in delisted]
    if include_delisted:
        codes = [*active_spot_codes, *delisted_codes]
    else:
        codes = active_spot_codes

    resolved = list(dict.fromkeys(codes))
    if not resolved:
        raise ValueError(f"No active AkShare stock codes found for {context}")
    return resolved


def latest_active_akshare_codes(store: ParquetStore) -> set[str]:
    try:
        return set(resolve_akshare_universe_codes(store, include_delisted=False, context="active AkShare pool"))
    except ValueError:
        return set()


def _latest_dataset_codes(df: pd.DataFrame) -> list[str]:
    if df.empty or "code" not in df.columns:
        return []
    return normalize_akshare_code_list(df["code"].dropna().astype(str).tolist())
