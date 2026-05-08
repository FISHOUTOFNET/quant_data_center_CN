"""Calendar helpers for the daily update pipeline."""

from __future__ import annotations

import pandas as pd

from src.api.market_data import MarketDataProvider
from src.pipeline.common import (
    FULL_HISTORY_START_DATE,
    PIPELINE_UPDATE_DAILY,
    PipelineCheckpointLookup,
    baostock_cn_trading_calendar_covers_range,
    checkpoint_output_path,
    latest_trading_day_on_or_before,
    should_skip_checkpoint,
    trading_day_lookback_start,
    trading_range_bounds,
    write_checkpoint,
)
from src.pipeline.services import log_api_fetch
from src.pipeline.update_daily_metadata import _add_skipped_run, _add_success_run, _persist_run_status
from src.storage.parquet_store import ParquetStore


def _prepare_partial_baostock_cn_trading_calendar(
    store: ParquetStore,
    provider: MarketDataProvider,
    baostock_cn_trading_calendar_df: pd.DataFrame,
    candidate_end_date: str,
    lookback_days: int,
    baostock_cn_trading_calendar_start_date: str,
) -> tuple[pd.DataFrame, str, str, pd.DataFrame | None]:
    resolved_window = _try_resolve_update_window(
        baostock_cn_trading_calendar_df,
        candidate_end_date,
        lookback_days,
        baostock_cn_trading_calendar_start_date,
    )
    if resolved_window is not None:
        return baostock_cn_trading_calendar_df, resolved_window[0], resolved_window[1], None

    fetched_baostock_cn_trading_calendar_df = provider.query_trade_dates(
        start_date=baostock_cn_trading_calendar_start_date,
        end_date=candidate_end_date,
    )
    log_api_fetch("baostock_cn_trading_calendar", "*", baostock_cn_trading_calendar_start_date, candidate_end_date, fetched_baostock_cn_trading_calendar_df)
    store.write_baostock_cn_trading_calendar(fetched_baostock_cn_trading_calendar_df)
    baostock_cn_trading_calendar_df = store.read_baostock_cn_trading_calendar()
    start_date, end_date = _resolve_update_window(baostock_cn_trading_calendar_df, candidate_end_date, lookback_days)
    return baostock_cn_trading_calendar_df, start_date, end_date, fetched_baostock_cn_trading_calendar_df


def _prepare_full_baostock_cn_trading_calendar(
    store: ParquetStore,
    provider: MarketDataProvider,
    baostock_cn_trading_calendar_df: pd.DataFrame,
    start_candidate_date: str,
    candidate_end_date: str,
) -> tuple[pd.DataFrame, str, str, pd.DataFrame | None]:
    if baostock_cn_trading_calendar_covers_range(baostock_cn_trading_calendar_df, start_candidate_date, candidate_end_date):
        start_date, end_date = trading_range_bounds(baostock_cn_trading_calendar_df, start_candidate_date, candidate_end_date)
        return baostock_cn_trading_calendar_df, start_date, end_date, None

    fetched_baostock_cn_trading_calendar_df = provider.query_trade_dates()
    log_api_fetch("baostock_cn_trading_calendar", "*", FULL_HISTORY_START_DATE, "latest", fetched_baostock_cn_trading_calendar_df)
    store.write_baostock_cn_trading_calendar(fetched_baostock_cn_trading_calendar_df)
    baostock_cn_trading_calendar_df = store.read_baostock_cn_trading_calendar()
    start_date, end_date = trading_range_bounds(baostock_cn_trading_calendar_df, start_candidate_date, candidate_end_date)
    return baostock_cn_trading_calendar_df, start_date, end_date, fetched_baostock_cn_trading_calendar_df


def _write_baostock_cn_trading_calendar_target(
    store: ParquetStore,
    provider: MarketDataProvider,
    mode: str,
    run_records: list[dict[str, object]],
    baostock_cn_trading_calendar_df: pd.DataFrame,
    fetched_baostock_cn_trading_calendar_df: pd.DataFrame | None,
    start_candidate_date: str,
    candidate_end_date: str,
    baostock_cn_trading_calendar_start_date: str,
    lookback_days: int,
    start_date: str,
    end_date: str,
    resume: bool,
    force: bool,
    checkpoint_lookup: PipelineCheckpointLookup | None,
) -> tuple[pd.DataFrame, str, str]:
    baostock_cn_trading_calendar_path = checkpoint_output_path(store, "baostock_cn_trading_calendar", "*", end_date)
    if should_skip_checkpoint(
        store,
        PIPELINE_UPDATE_DAILY,
        "baostock_cn_trading_calendar",
        "*",
        start_date,
        end_date,
        baostock_cn_trading_calendar_path,
        resume,
        force,
        checkpoint_lookup,
    ):
        run_row = _add_skipped_run(run_records, "baostock_cn_trading_calendar", "*", start_date, end_date, "checkpoint")
        _persist_run_status(store, run_row)
        return baostock_cn_trading_calendar_df, start_date, end_date

    if fetched_baostock_cn_trading_calendar_df is None:
        if mode == "partial":
            fetched_baostock_cn_trading_calendar_df = provider.query_trade_dates(
                start_date=baostock_cn_trading_calendar_start_date,
                end_date=candidate_end_date,
            )
            log_api_fetch("baostock_cn_trading_calendar", "*", baostock_cn_trading_calendar_start_date, candidate_end_date, fetched_baostock_cn_trading_calendar_df)
        else:
            fetched_baostock_cn_trading_calendar_df = provider.query_trade_dates()
            log_api_fetch("baostock_cn_trading_calendar", "*", FULL_HISTORY_START_DATE, "latest", fetched_baostock_cn_trading_calendar_df)
        baostock_cn_trading_calendar_path = store.write_baostock_cn_trading_calendar(fetched_baostock_cn_trading_calendar_df)
        baostock_cn_trading_calendar_df = store.read_baostock_cn_trading_calendar()
        if mode == "partial":
            start_date, end_date = _resolve_update_window(baostock_cn_trading_calendar_df, candidate_end_date, lookback_days)
        else:
            start_date, end_date = trading_range_bounds(baostock_cn_trading_calendar_df, start_candidate_date, candidate_end_date)
    else:
        baostock_cn_trading_calendar_path = store.baostock_cn_trading_calendar_path()

    run_row = _add_success_run(run_records, "baostock_cn_trading_calendar", "*", start_date, end_date, len(fetched_baostock_cn_trading_calendar_df))
    _persist_run_status(store, run_row)
    write_checkpoint(
        store,
        PIPELINE_UPDATE_DAILY,
        "baostock_cn_trading_calendar",
        "*",
        start_date,
        end_date,
        "success",
        len(fetched_baostock_cn_trading_calendar_df),
        baostock_cn_trading_calendar_path,
    )
    return baostock_cn_trading_calendar_df, start_date, end_date


def _try_resolve_update_window(
    baostock_cn_trading_calendar_df: pd.DataFrame,
    candidate_end_date: str,
    lookback_days: int,
    baostock_cn_trading_calendar_start_date: str,
) -> tuple[str, str] | None:
    if not baostock_cn_trading_calendar_covers_range(baostock_cn_trading_calendar_df, baostock_cn_trading_calendar_start_date, candidate_end_date):
        return None
    try:
        return _resolve_update_window(baostock_cn_trading_calendar_df, candidate_end_date, lookback_days)
    except ValueError:
        return None


def _resolve_update_window(
    baostock_cn_trading_calendar_df: pd.DataFrame,
    candidate_end_date: str,
    lookback_days: int,
) -> tuple[str, str]:
    end_date = latest_trading_day_on_or_before(baostock_cn_trading_calendar_df, candidate_end_date)
    start_date = trading_day_lookback_start(baostock_cn_trading_calendar_df, end_date, lookback_days)
    return start_date, end_date
