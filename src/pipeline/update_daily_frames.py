"""DataFrame helpers for the daily update pipeline."""

from __future__ import annotations

import pandas as pd

from src.api.market_data import MarketDataProvider
from src.pipeline.adjustments import (
    UNADJUSTED_DAILY_DATASET,
    calculate_adjusted_daily_bar,
    is_adjusted_daily_dataset,
)
from src.pipeline.services import fetch_daily_bars, log_api_fetch
from src.storage.parquet_store import ParquetStore
from src.storage.schema import BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_SCHEMA
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger


def _query_daily_bars(
    provider: MarketDataProvider,
    config: ConfigManager,
    dataset: str,
    stock_code: str,
    start_date: str,
    end_date: str,
    factor_cache: dict[str, pd.DataFrame] | None = None,
    unadjusted_cache: dict[tuple[str, str, str], pd.DataFrame] | None = None,
) -> pd.DataFrame:
    if dataset == UNADJUSTED_DAILY_DATASET:
        return _query_unadjusted_daily_bar(provider, config, stock_code, start_date, end_date, unadjusted_cache)
    if is_adjusted_daily_dataset(dataset):
        unadjusted = _query_unadjusted_daily_bar(
            provider,
            config,
            stock_code,
            start_date,
            end_date,
            unadjusted_cache,
        )
        factors = factor_cache.get(stock_code, pd.DataFrame()) if factor_cache is not None else pd.DataFrame()
        return calculate_adjusted_daily_bar(
            unadjusted,
            factors,
            dataset,
            config.adjust_flag_for_dataset(dataset),
        )
    return fetch_daily_bars(provider, config, dataset, stock_code, start_date, end_date)


def _query_unadjusted_daily_bar(
    provider: MarketDataProvider,
    config: ConfigManager,
    stock_code: str,
    start_date: str,
    end_date: str,
    unadjusted_cache: dict[tuple[str, str, str], pd.DataFrame] | None,
) -> pd.DataFrame:
    key = (stock_code, start_date, end_date)
    if unadjusted_cache is not None and key in unadjusted_cache:
        return unadjusted_cache[key].copy()
    df = fetch_daily_bars(provider, config, UNADJUSTED_DAILY_DATASET, stock_code, start_date, end_date)
    if unadjusted_cache is not None:
        unadjusted_cache[key] = df.copy()
    return df


def _log_daily_frame(
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    df: pd.DataFrame,
) -> None:
    if not is_adjusted_daily_dataset(dataset):
        log_api_fetch(dataset, code, start_date, end_date, df)
        return
    logger.info(
        "Local adjustment completed dataset={} code={} start_date={} end_date={} rows={}",
        dataset,
        code,
        start_date,
        end_date,
        len(df),
    )


def _baostock_cn_stock_adjustment_factor_frames_differ(store: ParquetStore, existing: pd.DataFrame, fresh: pd.DataFrame) -> bool:
    left = store.clean_dataframe_for_schema(existing, BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_SCHEMA)
    right = store.clean_dataframe_for_schema(fresh, BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_SCHEMA)
    if not left.empty:
        left = left.sort_values(["code", "dividend_operate_date"]).reset_index(drop=True)
    if not right.empty:
        right = right.sort_values(["code", "dividend_operate_date"]).reset_index(drop=True)
    return not left.equals(right)


def _needs_baostock_cn_stock_adjustment_factors(daily_targets: list[str]) -> bool:
    return any(is_adjusted_daily_dataset(dataset) for dataset in daily_targets)


def _has_data_in_range(df: pd.DataFrame, start_date: str, end_date: str) -> bool:
    if df.empty:
        return False
    dates = pd.to_datetime(df["date"], errors="coerce")
    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date)
    in_range = (dates >= start_ts) & (dates <= end_ts)
    return in_range.any()
