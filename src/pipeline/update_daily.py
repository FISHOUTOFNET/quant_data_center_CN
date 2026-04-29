"""Daily update pipeline with lookback overwrite and metadata state."""

from __future__ import annotations

import traceback
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import pandas as pd

from src.api.market_data import MarketDataProvider, create_provider
from src.pipeline.common import (
    DAILY_K_DATASETS,
    FULL_HISTORY_START_DATE,
    PIPELINE_UPDATE_DAILY,
    PipelineCheckpointLookup,
    calendar_covers_range,
    calendar_fetch_start,
    checkpoint_output_path,
    checkpoint_row,
    daily_frames_differ_on_overlap,
    date_iso,
    default_candidate_date,
    latest_trading_day_on_or_before,
    merge_daily_frames,
    resolve_codes,
    should_skip_checkpoint,
    trading_day_lookback_start,
    write_checkpoint,
)
from src.pipeline.services import (
    PipelineMetadataBatch,
    fetch_daily_k,
    fetch_stock_basic,
    log_api_fetch,
)
from src.pipeline.write_queue import PipelineWriteQueue
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger


def update_daily(
    code: tuple[str, ...] | list[str] | str | None = None,
    universe: str | None = None,
    lookback_days: int | None = None,
    end: str | None = None,
    root: Path | None = None,
    build_views: bool = True,
    resume: bool = True,
    force: bool = False,
    mode: str = "partial",  # partial or full
    provider: str | None = None,
) -> list[dict[str, object]]:
    """Update the three daily_k datasets for the resolved target trading day."""

    config = ConfigManager(root)
    store = ParquetStore(root=config.root)
    store.ensure_layout()

    candidate_end_date = date_iso(end) if end is not None else default_candidate_date(config)
    lookback = int(lookback_days or config.get("pipeline.lookback_days", 30))
    calendar_start_date = calendar_fetch_start(candidate_end_date, lookback)
    run_records: list[dict[str, object]] = []

    with create_provider(config, provider) as data_provider:
        checkpoint_lookup = PipelineCheckpointLookup.from_store(store) if resume and not force else None
        calendar_df = store.read_calendar()
        resolved_window = _try_resolve_update_window(
            calendar_df, candidate_end_date, lookback, calendar_start_date
        )
        calendar_path = checkpoint_output_path(store, "calendar", "*", candidate_end_date)
        if resolved_window is not None and should_skip_checkpoint(
            store,
            PIPELINE_UPDATE_DAILY,
            "calendar",
            "*",
            resolved_window[0],
            resolved_window[1],
            calendar_path,
            resume,
            force,
            checkpoint_lookup,
        ):
            start_date, end_date = resolved_window
            calendar_df = store.read_calendar()
            run_row = _add_skipped_run(run_records, "calendar", "*", start_date, end_date, "checkpoint")
            _persist_run_status(store, run_row)
        else:
            fetched_calendar_df = data_provider.query_trade_dates(
                start_date=calendar_start_date,
                end_date=candidate_end_date,
            )
            log_api_fetch("calendar", "*", calendar_start_date, candidate_end_date, fetched_calendar_df)
            calendar_path = store.write_calendar(fetched_calendar_df)
            calendar_df = store.read_calendar()
            start_date, end_date = _resolve_update_window(calendar_df, candidate_end_date, lookback)
            logger.info(
                "Resolved update target candidate_date={} trading_start={} trading_end={}",
                candidate_end_date,
                start_date,
                end_date,
            )
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

        stock_basic_path = checkpoint_output_path(store, "stock_basic", "*", end_date)
        if should_skip_checkpoint(
            store,
            PIPELINE_UPDATE_DAILY,
            "stock_basic",
            "*",
            start_date,
            end_date,
            stock_basic_path,
            resume,
            force,
            checkpoint_lookup,
        ):
            run_row = _add_skipped_run(run_records, "stock_basic", "*", start_date, end_date, "checkpoint")
            _persist_run_status(store, run_row)
        else:
            basic_df = fetch_stock_basic(data_provider)
            log_api_fetch("stock_basic", "*", start_date, end_date, basic_df)
            stock_basic_path = store.write_stock_basic(basic_df)
            run_row = _add_success_run(run_records, "stock_basic", "*", start_date, end_date, len(basic_df))
            status_row = _status_row("stock_basic", "*", end_date, len(basic_df), "success", "")
            _persist_run_status(store, run_row, status_row)
            write_checkpoint(
                store,
                PIPELINE_UPDATE_DAILY,
                "stock_basic",
                "*",
                start_date,
                end_date,
                "success",
                len(basic_df),
                stock_basic_path,
            )

        codes = resolve_codes(config, store, code, universe, stock_basic_mode="active")

        write_queue = PipelineWriteQueue()
        metadata_batch = PipelineMetadataBatch(
            store,
            int(config.get("pipeline.metadata_flush_size", 200)),
            count_by="run",
        )
        try:
            for dataset in DAILY_K_DATASETS:
                for stock_code in codes:
                    start_time = datetime.now()
                    output_path = checkpoint_output_path(store, dataset, stock_code, end_date)
                    if should_skip_checkpoint(
                        store,
                        PIPELINE_UPDATE_DAILY,
                        dataset,
                        stock_code,
                        start_date,
                        end_date,
                        output_path,
                        resume,
                        force,
                        checkpoint_lookup,
                    ):
                        run_row = _skipped_run_row(dataset, stock_code, start_date, end_date, "checkpoint")
                        write_queue.submit(
                            lambda run_row=run_row: _write_skipped_run(store, run_row, metadata_batch),
                            description=f"skip {dataset} {stock_code}",
                        )
                        continue

                    try:
                        run_start_date = start_date
                        if mode == "partial":
                            existing = store.read_daily_k(dataset, stock_code)
                            refresh_reason = ""

                            if not _has_data_in_range(existing, start_date, end_date):
                                refresh_reason = "empty_lookback"
                                logger.warning(
                                    "Daily lookback {} for {} {} from {} to {}; refetching from {}",
                                    refresh_reason,
                                    dataset,
                                    stock_code,
                                    start_date,
                                    end_date,
                                    FULL_HISTORY_START_DATE,
                                )
                                fresh = _query_daily_k(
                                    data_provider,
                                    config,
                                    dataset,
                                    stock_code,
                                    FULL_HISTORY_START_DATE,
                                    end_date,
                                )
                                log_api_fetch(dataset, stock_code, FULL_HISTORY_START_DATE, end_date, fresh)
                                run_start_date = FULL_HISTORY_START_DATE
                                final_df = fresh
                            else:
                                fresh = _query_daily_k(
                                    data_provider,
                                    config,
                                    dataset,
                                    stock_code,
                                    start_date,
                                    end_date,
                                )
                                log_api_fetch(dataset, stock_code, start_date, end_date, fresh)
                                if daily_frames_differ_on_overlap(store, existing, fresh, start_date, end_date):
                                    refresh_reason = "lookback_mismatch"
                                    logger.warning(
                                        "Daily lookback {} for {} {} from {} to {}; refetching from {}",
                                        refresh_reason,
                                        dataset,
                                        stock_code,
                                        start_date,
                                        end_date,
                                        FULL_HISTORY_START_DATE,
                                    )
                                    fresh = _query_daily_k(
                                        data_provider,
                                        config,
                                        dataset,
                                        stock_code,
                                        FULL_HISTORY_START_DATE,
                                        end_date,
                                    )
                                    log_api_fetch(dataset, stock_code, FULL_HISTORY_START_DATE, end_date, fresh)
                                    run_start_date = FULL_HISTORY_START_DATE
                                    final_df = fresh
                                else:
                                    final_df = merge_daily_frames(store, existing, fresh)
                        else:
                            fresh = _query_daily_k(
                                data_provider,
                                config,
                                dataset,
                                stock_code,
                                start_date,
                                end_date,
                            )
                            log_api_fetch(dataset, stock_code, start_date, end_date, fresh)
                            final_df = fresh
                        write_queue.submit(
                            lambda dataset=dataset,
                            stock_code=stock_code,
                            run_start_date=run_start_date,
                            start_time=start_time,
                            final_df=final_df: _write_daily_success(
                                store,
                                metadata_batch,
                                dataset,
                                stock_code,
                                run_start_date,
                                end_date,
                                start_time,
                                final_df,
                                start_date,
                            ),
                            on_error=lambda error_stack,
                            dataset=dataset,
                            stock_code=stock_code,
                            start_time=start_time,
                            output_path=output_path: _write_daily_failure(
                                store,
                                metadata_batch,
                                dataset,
                                stock_code,
                                start_date,
                                end_date,
                                start_time,
                                output_path,
                                error_stack,
                            ),
                            description=f"update {dataset} {stock_code}",
                        )
                    except Exception:
                        error_stack = traceback.format_exc()
                        logger.exception("Daily update failed for {} {}", dataset, stock_code)
                        write_queue.submit(
                            lambda dataset=dataset,
                            stock_code=stock_code,
                            start_time=start_time,
                            output_path=output_path,
                            error_stack=error_stack: _write_daily_failure(
                                store,
                                metadata_batch,
                                dataset,
                                stock_code,
                                start_date,
                                end_date,
                                start_time,
                                output_path,
                                error_stack,
                            ),
                            description=f"checkpoint failed {dataset} {stock_code}",
                        )
        finally:
            write_queue.submit(lambda: metadata_batch.flush(), description="flush update_daily metadata")
            run_records.extend(write_queue.close())

    if build_views:
        DuckDBStore(root=config.root).build_views()
    return run_records


def _query_daily_k(
    provider: MarketDataProvider,
    config: ConfigManager,
    dataset: str,
    stock_code: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    return fetch_daily_k(provider, config, dataset, stock_code, start_date, end_date)


def _has_data_in_range(df: pd.DataFrame, start_date: str, end_date: str) -> bool:
    if df.empty:
        return False
    dates = pd.to_datetime(df["date"], errors="coerce")
    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date)
    in_range = (dates >= start_ts) & (dates <= end_ts)
    return in_range.any()


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


def _persist_run_status(
    store: ParquetStore,
    run_row: dict[str, object],
    status_row: dict[str, object] | None = None,
) -> None:
    store.append_update_runs(pd.DataFrame([run_row]))
    if status_row is not None:
        store.upsert_update_status(pd.DataFrame([status_row]))


def _write_daily_success(
    store: ParquetStore,
    metadata_batch: PipelineMetadataBatch | None,
    dataset: str,
    code: str,
    run_start_date: str,
    end_date: str,
    start_time: datetime,
    df: pd.DataFrame,
    checkpoint_start_date: str,
) -> dict[str, object]:
    output_path = store.write_daily_k(dataset, code, df)
    row_count = len(df)
    run_row = _run_row(
        dataset,
        code,
        "success",
        run_start_date,
        end_date,
        start_time,
        datetime.now(),
        row_count,
        "",
    )
    status_row = _status_row(dataset, code, end_date, row_count, "success", "")
    checkpoint = checkpoint_row(
        PIPELINE_UPDATE_DAILY,
        dataset,
        code,
        checkpoint_start_date,
        end_date,
        "success",
        row_count,
        output_path,
    )
    if metadata_batch is None:
        _persist_run_status(store, run_row, status_row)
        write_checkpoint(
            store,
            PIPELINE_UPDATE_DAILY,
            dataset,
            code,
            checkpoint_start_date,
            end_date,
            "success",
            row_count,
            output_path,
        )
    else:
        metadata_batch.add(run_row=run_row, status_row=status_row, checkpoint=checkpoint)
    return run_row


def _write_daily_failure(
    store: ParquetStore,
    metadata_batch: PipelineMetadataBatch | None,
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    start_time: datetime,
    output_path: Path,
    error_stack: str,
) -> dict[str, object]:
    run_row = _run_row(
        dataset,
        code,
        "failed",
        start_date,
        end_date,
        start_time,
        datetime.now(),
        0,
        error_stack,
    )
    status_row = _status_row(dataset, code, None, 0, "failed", error_stack)
    checkpoint = checkpoint_row(
        PIPELINE_UPDATE_DAILY,
        dataset,
        code,
        start_date,
        end_date,
        "failed",
        0,
        output_path,
        error_stack,
    )
    try:
        if metadata_batch is None:
            _persist_run_status(store, run_row, status_row)
            write_checkpoint(
                store,
                PIPELINE_UPDATE_DAILY,
                dataset,
                code,
                start_date,
                end_date,
                "failed",
                0,
                output_path,
                error_stack,
            )
        else:
            metadata_batch.add(run_row=run_row, status_row=status_row, checkpoint=checkpoint)
    except Exception:
        logger.exception("Failed to persist daily update failure for {} {}", dataset, code)
    return run_row


def _write_skipped_run(
    store: ParquetStore,
    run_row: dict[str, object],
    metadata_batch: PipelineMetadataBatch | None = None,
) -> dict[str, object]:
    try:
        if metadata_batch is None:
            _persist_run_status(store, run_row)
        else:
            metadata_batch.add(run_row=run_row)
    except Exception:
        logger.exception("Failed to persist skipped daily update for {} {}", run_row["dataset"], run_row["code"])
    return run_row


def _add_success_run(
    records: list[dict[str, object]],
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    row_count: int,
) -> dict[str, object]:
    row = _success_run_row(dataset, code, start_date, end_date, row_count)
    records.append(row)
    return row


def _add_skipped_run(
    records: list[dict[str, object]],
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    reason: str,
) -> dict[str, object]:
    row = _skipped_run_row(dataset, code, start_date, end_date, reason)
    records.append(row)
    return row


def _success_run_row(
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    row_count: int,
) -> dict[str, object]:
    now = datetime.now()
    return _run_row(dataset, code, "success", start_date, end_date, now, now, row_count, "")


def _skipped_run_row(
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    reason: str,
) -> dict[str, object]:
    now = datetime.now()
    return _run_row(dataset, code, f"skipped_{reason}", start_date, end_date, now, now, 0, reason)


def _run_row(
    dataset: str,
    code: str,
    status: str,
    start_date: str,
    end_date: str,
    start_time: datetime,
    end_time: datetime,
    row_count: int,
    error_stack: str,
) -> dict[str, object]:
    row = {
        "task_id": str(uuid4()),
        "dataset": dataset,
        "code": code,
        "status": status,
        "start_date": start_date,
        "end_date": end_date,
        "start_time": start_time,
        "end_time": end_time,
        "row_count": row_count,
        "error_stack": error_stack,
    }
    return row


def _status_row(
    dataset: str,
    code: str,
    last_success_date: str | None,
    row_count: int,
    status: str,
    error_stack: str,
) -> dict[str, object]:
    return {
        "dataset": dataset,
        "code": code,
        "last_success_date": last_success_date,
        "row_count": row_count,
        "status": status,
        "updated_at": datetime.now(),
        "error_stack": error_stack,
    }
