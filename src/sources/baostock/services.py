"""Shared pipeline services for provider fetches and metadata batching."""

from __future__ import annotations

import pandas as pd

from src.pipeline.common import FULL_HISTORY_START_DATE, baostock_cn_trading_calendar_covers_range
from src.sources.common.market_data import DailyBarRequest, MarketDataProvider
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger
from src.utils.run_context import pipeline_log_values


def ensure_baostock_cn_trading_calendar_range(
    store: ParquetStore,
    provider: MarketDataProvider,
    start_date: str,
    end_date: str,
    fetch_start_date: str | None = None,
    fetch_end_date: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame | None]:
    """Ensure local baostock_cn_trading_calendar covers a date range, fetching via provider if needed."""

    baostock_cn_trading_calendar_df = store.read_dataset("baostock_cn_trading_calendar")
    if baostock_cn_trading_calendar_covers_range(baostock_cn_trading_calendar_df, start_date, end_date):
        return baostock_cn_trading_calendar_df, None

    fetched = provider.query_trade_dates(start_date=fetch_start_date, end_date=fetch_end_date)
    log_api_fetch(
        "baostock_cn_trading_calendar",
        "*",
        fetch_start_date or FULL_HISTORY_START_DATE,
        fetch_end_date or "latest",
        fetched,
    )
    store.write_dataset("baostock_cn_trading_calendar", fetched)
    return store.read_dataset("baostock_cn_trading_calendar"), fetched


def fetch_baostock_cn_stock_basic(provider: MarketDataProvider) -> pd.DataFrame:
    df = provider.query_baostock_cn_stock_basic()
    return df


def fetch_baostock_cn_stock_adjustment_factor(
    provider: MarketDataProvider,
    code: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    return provider.query_baostock_cn_stock_adjustment_factor(code=code, start_date=start_date, end_date=end_date)


def fetch_daily_bars(
    provider: MarketDataProvider,
    config: ConfigManager,
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    return provider.query_daily_bars(
        DailyBarRequest(
            dataset=dataset,
            code=code,
            start_date=start_date,
            end_date=end_date,
            fields=config.daily_bar_fields(),
            frequency=str(config.get("datasets.daily_bar.frequency", "d")),
        )
    )


def log_api_fetch(
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    df: pd.DataFrame,
) -> None:
    run_id, pid, thread = pipeline_log_values()
    logger.info(
        "API fetch completed run_id={} pid={} thread={} dataset={} code={} start_date={} end_date={} rows={}",
        run_id,
        pid,
        thread,
        dataset,
        code,
        start_date,
        end_date,
        len(df),
    )
