"""Targeted repair pipeline for replacing a date range."""

from __future__ import annotations

from pathlib import Path

from src.api.market_data import create_provider
from src.pipeline.common import (
    date_iso,
    expand_daily_datasets,
    replace_daily_range,
    trading_range_bounds,
)
from src.pipeline.services import ensure_calendar_range, fetch_daily_k, log_api_fetch
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

        for target_dataset in expand_daily_datasets(dataset):
            fresh = fetch_daily_k(data_provider, config, target_dataset, code, start_date, end_date)
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
