"""Build the partitioned canonical daily bar dataset."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.sources.derived.common import (
    cleanup_derived_dataset_staging,
    commit_derived_dataset_staging,
    create_derived_dataset_staging_area,
    read_partition_or_empty,
    refresh_derived_registry,
)
from src.sources.derived.security_master import build_security_master
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore

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
        updated_at = (now or datetime.now)()
        for _, security in master.iterrows():
            security_id = _clean_string(security.get("security_id"))
            if not security_id:
                continue
            security_df = _materialize_security_daily_bar(store, security, updated_at)
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


def _materialize_security_daily_bar(
    store: ParquetStore,
    security: pd.Series,
    updated_at: datetime,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    baostock_code = _clean_string(security.get("baostock_code"))
    akshare_code = _clean_string(security.get("akshare_code"))

    if baostock_code:
        for dataset_id, adjustment in BAOSTOCK_DAILY_SOURCES.items():
            source = read_partition_or_empty(store, dataset_id, baostock_code)
            frames.append(_map_baostock_daily(source, dataset_id, adjustment, security, updated_at))

    if akshare_code:
        for dataset_id, adjustment in AKSHARE_DAILY_SOURCES.items():
            source = read_partition_or_empty(store, dataset_id, akshare_code)
            frames.append(_map_akshare_daily(source, dataset_id, adjustment, security, updated_at))

    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame()
    combined = pd.concat(non_empty, ignore_index=True)
    combined["_source_rank"] = combined.apply(_source_rank, axis=1)
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
        return pd.DataFrame()
    return pd.DataFrame(
        [
            {
                "date": row.get("date"),
                "security_id": security.get("security_id"),
                "code": security.get("code"),
                "exchange": security.get("exchange"),
                "name": security.get("name"),
                "adjustment": adjustment,
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "prev_close": row.get("prev_close"),
                "volume": row.get("volume"),
                "amount": row.get("amount"),
                "turnover_rate": row.get("turnover_rate"),
                "pct_change": row.get("pct_change"),
                "trade_status": row.get("trade_status"),
                "is_st": row.get("is_st"),
                "is_active": security.get("is_active"),
                "source_dataset": dataset_id,
                "source_endpoint": "query_history_k_data_plus",
                "quality_status": "daily_bar_confirmed",
                "updated_at": updated_at,
            }
            for _, row in df.iterrows()
        ]
    )


def _map_akshare_daily(
    df: pd.DataFrame,
    dataset_id: str,
    adjustment: str,
    security: pd.Series,
    updated_at: datetime,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    return pd.DataFrame(
        [
            {
                "date": row.get("date"),
                "security_id": security.get("security_id"),
                "code": security.get("code"),
                "exchange": security.get("exchange"),
                "name": security.get("name"),
                "adjustment": adjustment,
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "prev_close": None,
                "volume": row.get("volume"),
                "amount": row.get("amount"),
                "turnover_rate": row.get("turnover_rate"),
                "pct_change": row.get("pct_change"),
                "trade_status": None,
                "is_st": None,
                "is_active": security.get("is_active"),
                "source_dataset": dataset_id,
                "source_endpoint": _clean_string(row.get("source_endpoint")) or "stock_zh_a_hist",
                "quality_status": _clean_string(row.get("quality_status")) or "daily_bar_confirmed",
                "updated_at": updated_at,
            }
            for _, row in df.iterrows()
        ]
    )


def _source_rank(row: pd.Series) -> int:
    source_dataset = _clean_string(row.get("source_dataset"))
    if source_dataset.startswith("baostock_"):
        return 0
    quality_status = _clean_string(row.get("quality_status"))
    if quality_status == "daily_bar_confirmed":
        return 1
    if quality_status == "spot_quote_close":
        return 2
    return 3


def _clean_string(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()
