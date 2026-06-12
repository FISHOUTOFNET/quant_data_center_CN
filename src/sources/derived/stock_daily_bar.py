"""Build the partitioned canonical daily bar dataset."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.sources.derived.common import (
    cleanup_derived_dataset_staging,
    commit_derived_dataset_staging,
    create_derived_dataset_staging_area,
    refresh_derived_registry,
)
from src.sources.derived.security_master import build_security_master
from src.storage.dataset_catalog import DATASET_CATALOG
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore
from src.storage.schema import CN_STOCK_DAILY_BAR_SCHEMA

BAOSTOCK_DAILY_SOURCES = {
    "baostock_cn_stock_daily_bar_unadjusted": "unadjusted",
    "baostock_cn_stock_daily_bar_qfq": "qfq",
    "baostock_cn_stock_daily_bar_hfq": "hfq",
}
AKSHARE_DAILY_SOURCES = {
    "akshare_cn_stock_daily_bar_unadjusted": "unadjusted",
    "akshare_cn_stock_daily_bar_qfq": "qfq",
    "akshare_cn_stock_daily_bar_hfq": "hfq",
}
CN_STOCK_DAILY_BAR_COLUMNS = tuple(field.name for field in CN_STOCK_DAILY_BAR_SCHEMA)


def build_cn_stock_daily_bar(
    *,
    root: Path | None = None,
    build_views: bool = True,
    refresh_registry: bool = True,
    now: Callable[[], datetime] | None = None,
) -> dict[str, object]:
    store = ParquetStore(root=root)
    staging = create_derived_dataset_staging_area(store, "cn_stock_daily_bar")
    write_store = ParquetStore(root=store.root, parquet_dir=staging.staging_root, metadata_dir=store.metadata_dir)

    rows = 0
    partitions = 0
    try:
        master = _read_or_build_master(store, now)
        partition_cache = _build_daily_source_partition_cache(store)
        master = _filter_master_to_daily_source_candidates(master, partition_cache)
        updated_at = (now or datetime.now)()
        for _, security in master.iterrows():
            security_id = _clean_string(security.get("security_id"))
            if not security_id:
                continue
            security_df = _materialize_security_daily_bar(store, security, updated_at, partition_cache)
            if security_df.empty:
                continue
            result = write_store.write_dataset(
                "cn_stock_daily_bar",
                security_df,
                partition={"security_id": security_id},
                mode="replace",
            )
            rows += result.row_count
            partitions += result.updated_partitions
        commit_derived_dataset_staging(staging)
    except Exception:
        cleanup_derived_dataset_staging(staging)
        raise

    if refresh_registry:
        refresh_derived_registry(store, ["cn_stock_daily_bar"])
    if build_views:
        DuckDBStore(root=store.root).build_views()
    return {
        "dataset": "cn_stock_daily_bar",
        "status": "success",
        "rows": rows,
        "partitions": partitions,
    }


def _read_or_build_master(store: ParquetStore, now: Callable[[], datetime] | None) -> pd.DataFrame:
    master = store.read_dataset("cn_security_master")
    if not store.dataset_exists("cn_security_master") or master.empty:
        build_security_master(root=store.root, build_views=False, refresh_registry=False, now=now)
        master = store.read_dataset("cn_security_master")
    return master


def _build_daily_source_partition_cache(store: ParquetStore) -> dict[str, set[str]]:
    partition_cache: dict[str, set[str]] = {}
    for dataset_id in (*BAOSTOCK_DAILY_SOURCES, *AKSHARE_DAILY_SOURCES):
        if DATASET_CATALOG[dataset_id].partition_column is None:
            partition_cache[dataset_id] = set()
            continue
        partition_cache[dataset_id] = set(store.list_dataset_partitions(dataset_id))
    return partition_cache


def _read_partition_or_empty_cached(
    store: ParquetStore,
    dataset_id: str,
    partition_value: str,
    partition_cache: Mapping[str, set[str]],
) -> pd.DataFrame:
    partition_column = DATASET_CATALOG[dataset_id].partition_column
    if partition_column is None:
        return store.read_dataset(dataset_id)
    if partition_value not in partition_cache.get(dataset_id, set()):
        return store.empty_dataset_frame(dataset_id)
    return store.read_dataset(dataset_id, {partition_column: partition_value})


def _filter_master_to_daily_source_candidates(
    master: pd.DataFrame,
    partition_cache: Mapping[str, set[str]],
) -> pd.DataFrame:
    if master.empty:
        return master

    baostock_partitions = set().union(
        *(partition_cache.get(dataset_id, set()) for dataset_id in BAOSTOCK_DAILY_SOURCES)
    )
    akshare_partitions = set().union(*(partition_cache.get(dataset_id, set()) for dataset_id in AKSHARE_DAILY_SOURCES))

    baostock_codes = [_clean_string(value) for value in _column_or_none(master, "baostock_code")]
    akshare_codes = [_clean_string(value) for value in _column_or_none(master, "akshare_code")]
    keep = [
        (bool(baostock_code) and baostock_code in baostock_partitions)
        or (bool(akshare_code) and akshare_code in akshare_partitions)
        for baostock_code, akshare_code in zip(baostock_codes, akshare_codes, strict=True)
    ]
    return master.loc[keep].reset_index(drop=True)


def _materialize_security_daily_bar(
    store: ParquetStore,
    security: pd.Series,
    updated_at: datetime,
    partition_cache: Mapping[str, set[str]],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    baostock_code = _clean_string(security.get("baostock_code"))
    akshare_code = _clean_string(security.get("akshare_code"))

    if baostock_code:
        for dataset_id, adjustment in BAOSTOCK_DAILY_SOURCES.items():
            source = _read_partition_or_empty_cached(store, dataset_id, baostock_code, partition_cache)
            frames.append(_map_baostock_daily(source, dataset_id, adjustment, security, updated_at))

    if akshare_code:
        for dataset_id, adjustment in AKSHARE_DAILY_SOURCES.items():
            source = _read_partition_or_empty_cached(store, dataset_id, akshare_code, partition_cache)
            frames.append(_map_akshare_daily(source, dataset_id, adjustment, security, updated_at))

    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame()
    combined = pd.concat(non_empty, ignore_index=True)
    combined["_source_rank"] = _assign_source_rank(combined)
    combined = (
        combined.sort_values(["date", "security_id", "adjustment", "_source_rank"], kind="mergesort")
        .drop_duplicates(["date", "security_id", "adjustment"], keep="first")
        .drop(columns=["_source_rank"])
        .sort_values(["security_id", "adjustment", "date"])
        .reset_index(drop=True)
    )
    return combined


def _map_baostock_daily(
    df: pd.DataFrame,
    dataset_id: str,
    adjustment: str,
    security: pd.Series,
    updated_at: datetime,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=CN_STOCK_DAILY_BAR_COLUMNS)

    out = pd.DataFrame(index=df.index)
    out["date"] = _column_or_none(df, "date")
    out["security_id"] = security.get("security_id")
    out["code"] = security.get("code")
    out["exchange"] = security.get("exchange")
    out["name"] = security.get("name")
    out["adjustment"] = adjustment
    out["open"] = _column_or_none(df, "open")
    out["high"] = _column_or_none(df, "high")
    out["low"] = _column_or_none(df, "low")
    out["close"] = _column_or_none(df, "close")
    out["prev_close"] = _column_or_none(df, "prev_close")
    out["volume"] = _column_or_none(df, "volume")
    out["amount"] = _column_or_none(df, "amount")
    out["turnover_rate"] = _column_or_none(df, "turnover_rate")
    out["pct_change"] = _column_or_none(df, "pct_change")
    out["trade_status"] = _column_or_none(df, "trade_status")
    out["is_st"] = _column_or_none(df, "is_st")
    out["is_active"] = security.get("is_active")
    out["source_dataset"] = dataset_id
    out["source_endpoint"] = "query_history_k_data_plus"
    out["quality_status"] = "daily_bar_confirmed"
    out["updated_at"] = updated_at
    return out.loc[:, CN_STOCK_DAILY_BAR_COLUMNS]


def _map_akshare_daily(
    df: pd.DataFrame,
    dataset_id: str,
    adjustment: str,
    security: pd.Series,
    updated_at: datetime,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=CN_STOCK_DAILY_BAR_COLUMNS)

    out = pd.DataFrame(index=df.index)
    out["date"] = _column_or_none(df, "date")
    out["security_id"] = security.get("security_id")
    out["code"] = security.get("code")
    out["exchange"] = security.get("exchange")
    out["name"] = security.get("name")
    out["adjustment"] = adjustment
    out["open"] = _column_or_none(df, "open")
    out["high"] = _column_or_none(df, "high")
    out["low"] = _column_or_none(df, "low")
    out["close"] = _column_or_none(df, "close")
    out["prev_close"] = None
    out["volume"] = _column_or_none(df, "volume")
    out["amount"] = _column_or_none(df, "amount")
    out["turnover_rate"] = _column_or_none(df, "turnover_rate")
    out["pct_change"] = _column_or_none(df, "pct_change")
    out["trade_status"] = None
    out["is_st"] = None
    out["is_active"] = security.get("is_active")
    out["source_dataset"] = dataset_id
    out["source_endpoint"] = _string_column_with_default(df, "source_endpoint", "stock_zh_a_hist")
    out["quality_status"] = _string_column_with_default(df, "quality_status", "daily_bar_confirmed")
    out["updated_at"] = updated_at
    return out.loc[:, CN_STOCK_DAILY_BAR_COLUMNS]


def _assign_source_rank(combined: pd.DataFrame) -> pd.Series:
    source_dataset = _string_column_with_default(combined, "source_dataset", "")
    quality_status = _string_column_with_default(combined, "quality_status", "")
    rank = pd.Series(3, index=combined.index)
    rank.loc[quality_status == "spot_quote_close"] = 2
    rank.loc[quality_status == "daily_bar_confirmed"] = 1
    rank.loc[source_dataset.str.startswith("baostock_")] = 0
    return rank


def _column_or_none(df: pd.DataFrame, column: str) -> pd.Series:
    if column in df.columns:
        return df[column]
    return pd.Series([None] * len(df), index=df.index)


def _string_column_with_default(df: pd.DataFrame, column: str, default: str) -> pd.Series:
    if column in df.columns:
        series = df[column].astype("string").str.strip()
    else:
        series = pd.Series(pd.NA, index=df.index, dtype="string")
    return series.mask(series.isna() | (series == ""), default)


def _clean_string(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()
