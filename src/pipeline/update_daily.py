"""Daily update pipeline with lookback overwrite and metadata state."""

from __future__ import annotations

import traceback
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.api.market_data import create_provider
from src.pipeline.adjustments import ADJUST_FACTOR_DATASET, UNADJUSTED_DAILY_DATASET
from src.pipeline.common import (
    FULL_HISTORY_START_DATE,
    PipelineCheckpointLookup,
    calendar_fetch_start,
    checkpoint_output_path,
    date_iso,
    default_candidate_date,
    resolve_codes,
)
from src.pipeline.services import PipelineMetadataBatch, fetch_adjust_factor, fetch_daily_k, log_api_fetch
from src.pipeline.update_daily_calendar import (
    _prepare_full_calendar,
    _prepare_partial_calendar,
    _write_calendar_target,
)
from src.pipeline.update_daily_frames import _needs_adjust_factors
from src.pipeline.update_daily_targets import (
    _daily_target_plans,
    _dataset_targets,
    _prefilter_checkpointed_codes,
    _write_stock_basic_target,
)
from src.pipeline.update_daily_types import ApiFetchRequest, BackgroundTaskResult, DailyTargetPlan
from src.pipeline.update_daily_worker import _DailyUpdateBackgroundWorker
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger

__all__ = [
    "ApiFetchRequest",
    "BackgroundTaskResult",
    "DailyTargetPlan",
    "update_daily",
]


def update_daily(
    dataset: str = "all",
    start: str = FULL_HISTORY_START_DATE,
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
    """Update selected datasets for the resolved trading window.

    ``partial`` mode performs the daily lookback refresh. ``full`` mode
    initializes full daily_k history while using the update_daily checkpoint
    namespace.
    """

    if mode not in {"partial", "full"}:
        raise ValueError(f"Unsupported update mode: {mode}")
    include_calendar, include_stock_basic, include_adjust_factor, daily_targets = _dataset_targets(dataset)

    config = ConfigManager(root)
    store = ParquetStore(root=config.root)
    store.ensure_layout()

    start_candidate_date = date_iso(start)
    candidate_end_date = date_iso(end) if end is not None else default_candidate_date(config)
    lookback = int(lookback_days if lookback_days is not None else config.get("pipeline.lookback_days", 30))
    run_records: list[dict[str, object]] = []

    with create_provider(config, provider) as data_provider:
        checkpoint_lookup = PipelineCheckpointLookup.from_store(store) if resume and not force else None
        calendar_df = store.read_calendar()

        if mode == "partial":
            calendar_start_date = calendar_fetch_start(candidate_end_date, lookback)
            calendar_df, start_date, end_date, fetched_calendar_df = _prepare_partial_calendar(
                store,
                data_provider,
                calendar_df,
                candidate_end_date,
                lookback,
                calendar_start_date,
            )
        else:
            calendar_start_date = start_candidate_date
            calendar_df, start_date, end_date, fetched_calendar_df = _prepare_full_calendar(
                store,
                data_provider,
                calendar_df,
                start_candidate_date,
                candidate_end_date,
            )

        if include_calendar:
            calendar_df, start_date, end_date = _write_calendar_target(
                store,
                data_provider,
                mode,
                run_records,
                calendar_df,
                fetched_calendar_df,
                start_candidate_date,
                candidate_end_date,
                calendar_start_date,
                lookback,
                start_date,
                end_date,
                resume,
                force,
                checkpoint_lookup,
            )

        if include_calendar or mode == "partial":
            logger.info(
                "Resolved update target candidate_date={} trading_start={} trading_end={}",
                candidate_end_date,
                start_date,
                end_date,
            )

        if include_stock_basic:
            _write_stock_basic_target(
                store,
                data_provider,
                run_records,
                start_date,
                end_date,
                resume,
                force,
                checkpoint_lookup,
            )

        needs_code_pool = bool(daily_targets or include_adjust_factor)
        if needs_code_pool and not code and not universe and not store.stock_basic_path().exists():
            _write_stock_basic_target(
                store,
                data_provider,
                run_records,
                start_date,
                end_date,
                resume,
                force,
                checkpoint_lookup,
            )

        stock_basic_mode = "all" if mode == "full" else "active"
        codes = (
            resolve_codes(config, store, code, universe, stock_basic_mode=stock_basic_mode)
            if needs_code_pool
            else []
        )

        needs_adjust_factor_api = include_adjust_factor or _needs_adjust_factors(daily_targets)
        codes = _prefilter_checkpointed_codes(
            store,
            codes,
            daily_targets,
            needs_adjust_factor_api,
            mode,
            start_date,
            end_date,
            checkpoint_lookup,
        )
        metadata_batch = PipelineMetadataBatch(
            store,
            int(config.get("pipeline.metadata_flush_size", 200)),
            count_by="run",
        )
        background = _DailyUpdateBackgroundWorker(
            store=store,
            config=config,
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            metadata_batch=metadata_batch,
        )
        pending: deque[Future[BackgroundTaskResult]] = deque()

        def submit_background(action) -> None:
            pending.append(executor.submit(action))

        def dispatch_api_request(request: ApiFetchRequest) -> None:
            if request.kind != "daily_k_full_refetch":
                raise ValueError(f"Unsupported background API request: {request.kind}")
            try:
                fetched = fetch_daily_k(
                    data_provider,
                    config,
                    UNADJUSTED_DAILY_DATASET,
                    request.code,
                    request.start_date,
                    request.end_date,
                )
                log_api_fetch(UNADJUSTED_DAILY_DATASET, request.code, request.start_date, request.end_date, fetched)
                submit_background(
                    lambda request=request, fetched=fetched: background.process_daily_full_refetch(
                        request,
                        fetched,
                        None,
                    )
                )
            except Exception:
                error_stack = traceback.format_exc()
                logger.exception("Daily full refetch API failed for {}", request.code)
                submit_background(
                    lambda request=request, error_stack=error_stack: background.process_daily_full_refetch(
                        request,
                        pd.DataFrame(),
                        error_stack,
                    )
                )

        def drain_completed(block: bool = False) -> None:
            while pending and (block or pending[0].done()):
                result = pending.popleft().result()
                run_records.extend(result.run_records)
                for request in result.api_requests:
                    dispatch_api_request(request)

        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="update-daily-background") as executor:
            for stock_code in codes:
                if needs_adjust_factor_api:
                    factor_start_time = datetime.now()
                    factor_output_path = checkpoint_output_path(store, ADJUST_FACTOR_DATASET, stock_code, end_date)
                    try:
                        factor_df = fetch_adjust_factor(
                            data_provider,
                            stock_code,
                            FULL_HISTORY_START_DATE,
                            end_date,
                        )
                        log_api_fetch(ADJUST_FACTOR_DATASET, stock_code, FULL_HISTORY_START_DATE, end_date, factor_df)
                        submit_background(
                            lambda stock_code=stock_code,
                            factor_df=factor_df,
                            factor_start_time=factor_start_time,
                            factor_output_path=factor_output_path: background.process_adjust_factor_success(
                                stock_code,
                                factor_df,
                                factor_start_time,
                                factor_output_path,
                            )
                        )
                    except Exception:
                        error_stack = traceback.format_exc()
                        logger.exception("Adjust factor API failed for {}", stock_code)
                        submit_background(
                            lambda stock_code=stock_code,
                            factor_start_time=factor_start_time,
                            factor_output_path=factor_output_path,
                            error_stack=error_stack: background.process_adjust_factor_failure(
                                stock_code,
                                factor_start_time,
                                factor_output_path,
                                error_stack,
                            )
                        )

                if daily_targets:
                    plans = _daily_target_plans(
                        store,
                        daily_targets,
                        stock_code,
                        mode,
                        start_date,
                        end_date,
                    )
                    initial_start_date = FULL_HISTORY_START_DATE if mode == "full" else start_date
                    try:
                        daily_df = fetch_daily_k(
                            data_provider,
                            config,
                            UNADJUSTED_DAILY_DATASET,
                            stock_code,
                            initial_start_date,
                            end_date,
                        )
                        log_api_fetch(UNADJUSTED_DAILY_DATASET, stock_code, initial_start_date, end_date, daily_df)
                        submit_background(
                            lambda stock_code=stock_code,
                            plans=plans,
                            initial_start_date=initial_start_date,
                            daily_df=daily_df: background.process_daily_initial(
                                stock_code,
                                plans,
                                initial_start_date,
                                daily_df,
                                None,
                            )
                        )
                    except Exception:
                        error_stack = traceback.format_exc()
                        logger.exception("Daily K API failed for {}", stock_code)
                        submit_background(
                            lambda stock_code=stock_code,
                            plans=plans,
                            initial_start_date=initial_start_date,
                            error_stack=error_stack: background.process_daily_initial(
                                stock_code,
                                plans,
                                initial_start_date,
                                pd.DataFrame(),
                                error_stack,
                            )
                        )

                drain_completed(block=False)

            while pending:
                drain_completed(block=True)
            submit_background(lambda: background.flush_metadata())
            while pending:
                drain_completed(block=True)

    if build_views:
        DuckDBStore(root=config.root).build_views()
    return run_records
