"""Targeted repair pipeline for replacing a date range."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.api.market_data import create_provider
from src.pipeline.adjustments import (
    ADJUST_FACTOR_DATASET,
    UNADJUSTED_DAILY_DATASET,
    calculate_adjusted_daily_k,
    is_adjusted_daily_dataset,
)
from src.pipeline.common import (
    FULL_HISTORY_START_DATE,
    date_iso,
    expand_daily_datasets,
    replace_daily_range,
    trading_range_bounds,
)
from src.pipeline.services import ensure_calendar_range, fetch_adjust_factor, fetch_daily_k, log_api_fetch
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger


def repair(
    code: str,
    start: str,
    end: str,
    dataset: str,
    root: Path | None = None,
    build_views: bool = True,
    provider: str | None = None,
) -> list[dict[str, str | int]]:
    """Re-fetch and replace a date range for one code and one daily_k dataset."""

    config = ConfigManager(root)
    store = ParquetStore(root=config.root)
    store.ensure_layout()
    start_candidate_date = date_iso(start)
    end_candidate_date = date_iso(end)
    results: list[dict[str, str | int]] = []

    with create_provider(config, provider) as data_provider:
        calendar_df, _ = ensure_calendar_range(
            store,
            data_provider,
            start_candidate_date,
            end_candidate_date,
        )
        start_date, end_date = trading_range_bounds(calendar_df, start_candidate_date, end_candidate_date)

        if dataset == ADJUST_FACTOR_DATASET:
            fresh_factor = fetch_adjust_factor(data_provider, code, FULL_HISTORY_START_DATE, end_date)
            log_api_fetch(ADJUST_FACTOR_DATASET, code, FULL_HISTORY_START_DATE, end_date, fresh_factor)
            path = store.write_adjust_factor(code, fresh_factor)
            logger.info(
                "Repaired {} {} from {} to {} rows={}",
                ADJUST_FACTOR_DATASET,
                code,
                FULL_HISTORY_START_DATE,
                end_date,
                len(fresh_factor),
            )
            results.append(
                {
                    "dataset": ADJUST_FACTOR_DATASET,
                    "code": code,
                    "replacement_rows": len(fresh_factor),
                    "total_rows": len(fresh_factor),
                    "path": str(path),
                }
            )
        else:
            target_datasets = expand_daily_datasets(dataset)
            adjust_factors = pd.DataFrame()
            if any(is_adjusted_daily_dataset(target_dataset) for target_dataset in target_datasets):
                adjust_factors = fetch_adjust_factor(data_provider, code, FULL_HISTORY_START_DATE, end_date)
                log_api_fetch(ADJUST_FACTOR_DATASET, code, FULL_HISTORY_START_DATE, end_date, adjust_factors)
                store.write_adjust_factor(code, adjust_factors)

            unadjusted_cache: dict[tuple[str, str], pd.DataFrame] = {}
            for target_dataset in target_datasets:
                fresh = _repair_daily_frame(
                    data_provider,
                    config,
                    target_dataset,
                    code,
                    start_date,
                    end_date,
                    adjust_factors,
                    unadjusted_cache,
                )
                if is_adjusted_daily_dataset(target_dataset):
                    logger.info(
                        "Local adjustment completed dataset={} code={} start_date={} end_date={} rows={}",
                        target_dataset,
                        code,
                        start_date,
                        end_date,
                        len(fresh),
                    )
                else:
                    log_api_fetch(target_dataset, code, start_date, end_date, fresh)

                existing = store.read_daily_k(target_dataset, code)
                merged = replace_daily_range(store, existing, fresh, start_date, end_date)
                path = store.write_daily_k(target_dataset, code, merged)
                logger.info(
                    "Repaired {} {} from {} to {} replacement_rows={} total_rows={}",
                    target_dataset,
                    code,
                    start_date,
                    end_date,
                    len(fresh),
                    len(merged),
                )
                results.append(
                    {
                        "dataset": target_dataset,
                        "code": code,
                        "replacement_rows": len(fresh),
                        "total_rows": len(merged),
                        "path": str(path),
                    }
                )

    if build_views:
        DuckDBStore(root=config.root).build_views()
    return results


def _repair_daily_frame(
    provider,
    config: ConfigManager,
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    adjust_factors: pd.DataFrame,
    unadjusted_cache: dict[tuple[str, str], pd.DataFrame],
) -> pd.DataFrame:
    if dataset == UNADJUSTED_DAILY_DATASET:
        return _repair_unadjusted(provider, config, code, start_date, end_date, unadjusted_cache)
    if is_adjusted_daily_dataset(dataset):
        unadjusted = _repair_unadjusted(provider, config, code, start_date, end_date, unadjusted_cache)
        return calculate_adjusted_daily_k(
            unadjusted,
            adjust_factors,
            dataset,
            config.adjustflag_for_dataset(dataset),
        )
    return fetch_daily_k(provider, config, dataset, code, start_date, end_date)


def _repair_unadjusted(
    provider,
    config: ConfigManager,
    code: str,
    start_date: str,
    end_date: str,
    unadjusted_cache: dict[tuple[str, str], pd.DataFrame],
) -> pd.DataFrame:
    key = (start_date, end_date)
    if key not in unadjusted_cache:
        unadjusted_cache[key] = fetch_daily_k(
            provider,
            config,
            UNADJUSTED_DAILY_DATASET,
            code,
            start_date,
            end_date,
        )
    return unadjusted_cache[key].copy()
