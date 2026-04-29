"""Calendar helpers for the daily update pipeline."""

from __future__ import annotations

import pandas as pd

from src.api.market_data import MarketDataProvider
from src.pipeline.common import (
    FULL_HISTORY_START_DATE,
    PIPELINE_UPDATE_DAILY,
    PipelineCheckpointLookup,
    calendar_covers_range,
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


def _prepare_partial_calendar(
    store: ParquetStore,
    provider: MarketDataProvider,
    calendar_df: pd.DataFrame,
    candidate_end_date: str,
    lookback_days: int,
    calendar_start_date: str,
) -> tuple[pd.DataFrame, str, str, pd.DataFrame | None]:
    resolved_window = _try_resolve_update_window(
        calendar_df,
        candidate_end_date,
        lookback_days,
        calendar_start_date,
    )
    if resolved_window is not None:
        return calendar_df, resolved_window[0], resolved_window[1], None

    fetched_calendar_df = provider.query_trade_dates(
        start_date=calendar_start_date,
        end_date=candidate_end_date,
    )
    log_api_fetch("calendar", "*", calendar_start_date, candidate_end_date, fetched_calendar_df)
    store.write_calendar(fetched_calendar_df)
    calendar_df = store.read_calendar()
    start_date, end_date = _resolve_update_window(calendar_df, candidate_end_date, lookback_days)
    return calendar_df, start_date, end_date, fetched_calendar_df


def _prepare_full_calendar(
    store: ParquetStore,
    provider: MarketDataProvider,
    calendar_df: pd.DataFrame,
    start_candidate_date: str,
    candidate_end_date: str,
) -> tuple[pd.DataFrame, str, str, pd.DataFrame | None]:
    if calendar_covers_range(calendar_df, start_candidate_date, candidate_end_date):
        start_date, end_date = trading_range_bounds(calendar_df, start_candidate_date, candidate_end_date)
        return calendar_df, start_date, end_date, None

    fetched_calendar_df = provider.query_trade_dates()
    log_api_fetch("calendar", "*", FULL_HISTORY_START_DATE, "latest", fetched_calendar_df)
    store.write_calendar(fetched_calendar_df)
    calendar_df = store.read_calendar()
    start_date, end_date = trading_range_bounds(calendar_df, start_candidate_date, candidate_end_date)
    return calendar_df, start_date, end_date, fetched_calendar_df


def _write_calendar_target(
    store: ParquetStore,
    provider: MarketDataProvider,
    mode: str,
    run_records: list[dict[str, object]],
    calendar_df: pd.DataFrame,
    fetched_calendar_df: pd.DataFrame | None,
    start_candidate_date: str,
    candidate_end_date: str,
    calendar_start_date: str,
    lookback_days: int,
    start_date: str,
    end_date: str,
    resume: bool,
    force: bool,
    checkpoint_lookup: PipelineCheckpointLookup | None,
) -> tuple[pd.DataFrame, str, str]:
    calendar_path = checkpoint_output_path(store, "calendar", "*", end_date)
    if should_skip_checkpoint(
        store,
        PIPELINE_UPDATE_DAILY,
        "calendar",
        "*",
        start_date,
        end_date,
        calendar_path,
        resume,
        force,
        checkpoint_lookup,
    ):
        run_row = _add_skipped_run(run_records, "calendar", "*", start_date, end_date, "checkpoint")
        _persist_run_status(store, run_row)
        return calendar_df, start_date, end_date

    if fetched_calendar_df is None:
        if mode == "partial":
            fetched_calendar_df = provider.query_trade_dates(
                start_date=calendar_start_date,
                end_date=candidate_end_date,
            )
            log_api_fetch("calendar", "*", calendar_start_date, candidate_end_date, fetched_calendar_df)
        else:
            fetched_calendar_df = provider.query_trade_dates()
            log_api_fetch("calendar", "*", FULL_HISTORY_START_DATE, "latest", fetched_calendar_df)
        calendar_path = store.write_calendar(fetched_calendar_df)
        calendar_df = store.read_calendar()
        if mode == "partial":
            start_date, end_date = _resolve_update_window(calendar_df, candidate_end_date, lookback_days)
        else:
            start_date, end_date = trading_range_bounds(calendar_df, start_candidate_date, candidate_end_date)
    else:
        calendar_path = store.calendar_path()

    run_row = _add_success_run(run_records, "calendar", "*", start_date, end_date, len(fetched_calendar_df))
    _persist_run_status(store, run_row)
    write_checkpoint(
        store,
        PIPELINE_UPDATE_DAILY,
        "calendar",
        "*",
        start_date,
        end_date,
        "success",
        len(fetched_calendar_df),
        calendar_path,
    )
    return calendar_df, start_date, end_date


def _try_resolve_update_window(
    calendar_df: pd.DataFrame,
    candidate_end_date: str,
    lookback_days: int,
    calendar_start_date: str,
) -> tuple[str, str] | None:
    if not calendar_covers_range(calendar_df, calendar_start_date, candidate_end_date):
        return None
    try:
        return _resolve_update_window(calendar_df, candidate_end_date, lookback_days)
    except ValueError:
        return None


def _resolve_update_window(
    calendar_df: pd.DataFrame,
    candidate_end_date: str,
    lookback_days: int,
) -> tuple[str, str]:
    end_date = latest_trading_day_on_or_before(calendar_df, candidate_end_date)
    start_date = trading_day_lookback_start(calendar_df, end_date, lookback_days)
    return start_date, end_date
