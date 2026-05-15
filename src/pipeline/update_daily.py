"""Daily update pipeline with lookback overwrite and metadata state."""

from __future__ import annotations

import traceback
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.api.market_data import create_provider
from src.pipeline.adjustments import BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET, UNADJUSTED_DAILY_DATASET
from src.pipeline.common import (
    FULL_HISTORY_START_DATE,
    PipelineCheckpointLookup,
    baostock_cn_trading_calendar_covers_range,
    baostock_cn_trading_calendar_fetch_start,
    checkpoint_output_path,
    date_iso,
    default_candidate_date,
    resolve_codes,
)
from src.pipeline.dry_run import apply_limit, blocked_record, dry_run_record
from src.pipeline.finalization import _finalize_write_pipeline
from src.pipeline.services import PipelineMetadataBatch, fetch_baostock_cn_stock_adjustment_factor, fetch_daily_bars, log_api_fetch
from src.pipeline.update_daily_calendar import (
    _prepare_full_baostock_cn_trading_calendar,
    _prepare_partial_baostock_cn_trading_calendar,
    _write_baostock_cn_trading_calendar_target,
)
from src.pipeline.update_daily_frames import _needs_baostock_cn_stock_adjustment_factors
from src.pipeline.update_daily_targets import (
    _daily_target_plans,
    _dataset_targets,
    _prefilter_checkpointed_codes,
    _write_baostock_cn_stock_basic_target,
)
from src.pipeline.update_daily_types import ApiFetchRequest, BackgroundTaskResult, DailyTargetPlan
from src.pipeline.update_daily_worker import _DailyUpdateBackgroundWorker
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
    lookback_days: int | None = None,
    end: str | None = None,
    root: Path | None = None,
    build_views: bool = True,
    resume: bool = True,
    force: bool = False,
    mode: str = "partial",  # partial or full
    provider: str | None = None,
    dry_run: bool = False,
    max_codes: int | None = None,
    max_tasks: int | None = None,
) -> list[dict[str, object]]:
    """Update selected datasets for the resolved trading window.

    ``partial`` mode performs the daily lookback refresh. ``full`` mode
    initializes full daily_bar history while using the update_daily checkpoint
    namespace.
    """

    if mode not in {"partial", "full"}:
        raise ValueError(f"Unsupported update mode: {mode}")
    include_baostock_cn_trading_calendar, include_baostock_cn_stock_basic, include_baostock_cn_stock_adjustment_factor, daily_targets = _dataset_targets(dataset)

    start_candidate_date = date_iso(start)
    config = ConfigManager(root)
    store = ParquetStore(root=config.root)
    metadata_batch: PipelineMetadataBatch | None = None
    success_count = 0
    should_build_views = False

    def flush_metadata() -> None:
        if metadata_batch is not None:
            metadata_batch.flush()

    with _finalize_write_pipeline(
        store=store,
        metadata_flush=flush_metadata,
        build_views=lambda: should_build_views,
        cleanup_tmp_files=lambda: success_count > 0,
    ):
        candidate_end_date = date_iso(end) if end is not None else default_candidate_date(config)
        lookback = int(lookback_days if lookback_days is not None else config.get("pipeline.lookback_days", 30))
        if not dry_run and max_tasks is not None and int(max_tasks) == 0:
            return []
        if not dry_run and max_tasks is not None and dataset != "baostock_cn_trading_calendar":
            required_calendar_start = (
                start_candidate_date
                if mode == "full"
                else baostock_cn_trading_calendar_fetch_start(candidate_end_date, lookback)
            )
            local_calendar = store.read_baostock_cn_trading_calendar()
            if not baostock_cn_trading_calendar_covers_range(
                local_calendar,
                required_calendar_start,
                candidate_end_date,
            ):
                raise RuntimeError(
                    "update-baostock-daily --max-tasks requires local baostock_cn_trading_calendar "
                    "to cover the requested planning window; run a calendar update first or use --dry-run."
                )
        if dry_run:
            dry_start_date = (
                start_candidate_date
                if mode == "full"
                else baostock_cn_trading_calendar_fetch_start(candidate_end_date, lookback)
            )
            records: list[dict[str, object]] = []
            if include_baostock_cn_trading_calendar:
                records.append(
                    dry_run_record(
                        "baostock_cn_trading_calendar",
                        "*",
                        dry_start_date,
                        candidate_end_date,
                        store.baostock_cn_trading_calendar_path(),
                        operation="write_baostock_cn_trading_calendar",
                    )
                )
            if include_baostock_cn_stock_basic:
                records.append(
                    dry_run_record(
                        "baostock_cn_stock_basic",
                        "*",
                        dry_start_date,
                        candidate_end_date,
                        store.baostock_cn_stock_basic_path(),
                        operation="write_baostock_cn_stock_basic",
                    )
                )

            needs_code_pool = bool(daily_targets or include_baostock_cn_stock_adjustment_factor)
            codes: list[str] = []
            if needs_code_pool:
                try:
                    baostock_cn_stock_basic_mode = "all" if mode == "full" else "active"
                    codes = resolve_codes(
                        config,
                        store,
                        code,
                        baostock_cn_stock_basic_mode=baostock_cn_stock_basic_mode,
                    )
                    codes = apply_limit(codes, max_codes, "max_codes")
                except Exception as exc:
                    records.append(
                        blocked_record(
                            dataset,
                            "*",
                            dry_start_date,
                            candidate_end_date,
                            operation="resolve_codes",
                            message=str(exc),
                        )
                    )
                    return apply_limit(records, max_tasks, "max_tasks")

            checkpoint_start = FULL_HISTORY_START_DATE if mode == "full" else dry_start_date
            for stock_code in codes:
                if include_baostock_cn_stock_adjustment_factor:
                    records.append(
                        dry_run_record(
                            BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET,
                            stock_code,
                            FULL_HISTORY_START_DATE,
                            candidate_end_date,
                            checkpoint_output_path(
                                store,
                                BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET,
                                stock_code,
                                candidate_end_date,
                            ),
                            operation="write_baostock_cn_stock_adjustment_factor",
                        )
                    )
                for target_dataset in daily_targets:
                    records.append(
                        dry_run_record(
                            target_dataset,
                            stock_code,
                            checkpoint_start,
                            candidate_end_date,
                            checkpoint_output_path(store, target_dataset, stock_code, candidate_end_date),
                            operation="write_baostock_daily_bars",
                        )
                    )
            return apply_limit(records, max_tasks, "max_tasks")

        store.ensure_layout()
        run_records: list[dict[str, object]] = []
        logger.info(
            "Daily update started dataset={} mode={} force={} resume={} candidate_start={} candidate_end={}",
            dataset,
            mode,
            force,
            resume,
            start_candidate_date,
            candidate_end_date,
        )

        with create_provider(config, provider) as data_provider:
            checkpoint_lookup = PipelineCheckpointLookup.from_store(store) if resume and not force else None
            baostock_cn_trading_calendar_df = store.read_baostock_cn_trading_calendar()

            if mode == "partial":
                baostock_cn_trading_calendar_start_date = baostock_cn_trading_calendar_fetch_start(candidate_end_date, lookback)
                baostock_cn_trading_calendar_df, start_date, end_date, fetched_baostock_cn_trading_calendar_df = _prepare_partial_baostock_cn_trading_calendar(
                    store,
                    data_provider,
                    baostock_cn_trading_calendar_df,
                    candidate_end_date,
                    lookback,
                    baostock_cn_trading_calendar_start_date,
                )
            else:
                baostock_cn_trading_calendar_start_date = start_candidate_date
                baostock_cn_trading_calendar_df, start_date, end_date, fetched_baostock_cn_trading_calendar_df = _prepare_full_baostock_cn_trading_calendar(
                    store,
                    data_provider,
                    baostock_cn_trading_calendar_df,
                    start_candidate_date,
                    candidate_end_date,
                )

            if include_baostock_cn_trading_calendar:
                baostock_cn_trading_calendar_df, start_date, end_date = _write_baostock_cn_trading_calendar_target(
                    store,
                    data_provider,
                    mode,
                    run_records,
                    baostock_cn_trading_calendar_df,
                    fetched_baostock_cn_trading_calendar_df,
                    start_candidate_date,
                    candidate_end_date,
                    baostock_cn_trading_calendar_start_date,
                    lookback,
                    start_date,
                    end_date,
                    resume,
                    force,
                    checkpoint_lookup,
                )

            if include_baostock_cn_trading_calendar or mode == "partial":
                logger.info(
                    "Resolved update target candidate_date={} trading_start={} trading_end={}",
                    candidate_end_date,
                    start_date,
                    end_date,
                )

            if include_baostock_cn_stock_basic:
                _write_baostock_cn_stock_basic_target(
                    store,
                    data_provider,
                    run_records,
                    start_date,
                    end_date,
                    resume,
                    force,
                    checkpoint_lookup,
                )

            needs_code_pool = bool(daily_targets or include_baostock_cn_stock_adjustment_factor)
            if needs_code_pool and not code and not store.baostock_cn_stock_basic_path().exists():
                _write_baostock_cn_stock_basic_target(
                    store,
                    data_provider,
                    run_records,
                    start_date,
                    end_date,
                    resume,
                    force,
                    checkpoint_lookup,
                )

            baostock_cn_stock_basic_mode = "all" if mode == "full" else "active"
            codes = (
                resolve_codes(config, store, code, baostock_cn_stock_basic_mode=baostock_cn_stock_basic_mode)
                if needs_code_pool
                else []
            )
            codes = apply_limit(codes, max_codes, "max_codes")

            needs_baostock_cn_stock_adjustment_factor_api = include_baostock_cn_stock_adjustment_factor or _needs_baostock_cn_stock_adjustment_factors(daily_targets)
            codes = _prefilter_checkpointed_codes(
                store,
                codes,
                daily_targets,
                needs_baostock_cn_stock_adjustment_factor_api,
                mode,
                start_date,
                end_date,
                checkpoint_lookup,
            )
            if max_tasks is not None and codes:
                tasks_per_code = len(daily_targets) + (1 if needs_baostock_cn_stock_adjustment_factor_api else 0)
                if tasks_per_code > 0:
                    max_code_tasks = int(max_tasks) // tasks_per_code
                    codes = codes[:max(max_code_tasks, 0)]
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
            background_workers = max(int(config.get("pipeline.background_workers", 4)), 1)
            background_max_pending = max(
                int(config.get("pipeline.background_max_pending", background_workers * 4)),
                1,
            )
            pending: set[Future[BackgroundTaskResult]] = set()
            future_sequences: dict[Future[BackgroundTaskResult], int] = {}
            completed_results: dict[int, BackgroundTaskResult] = {}
            next_submit_sequence = 0
            next_record_sequence = 0

            def submit_background(action) -> Future[BackgroundTaskResult]:
                nonlocal next_submit_sequence
                while len(pending) >= background_max_pending:
                    drain_completed(block=True)
                future = executor.submit(action)
                future_sequences[future] = next_submit_sequence
                next_submit_sequence += 1
                pending.add(future)
                return future

            def dispatch_api_request(request: ApiFetchRequest) -> None:
                if request.kind != "daily_bar_full_refetch":
                    raise ValueError(f"Unsupported background API request: {request.kind}")
                try:
                    fetched = fetch_daily_bars(
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
                if not pending:
                    return
                if block:
                    done, _ = wait(pending, return_when=FIRST_COMPLETED)
                else:
                    done = {future for future in pending if future.done()}
                for future in sorted(done, key=lambda item: future_sequences[item]):
                    pending.remove(future)
                    collect_completed(future)

            def collect_completed(future: Future[BackgroundTaskResult]) -> None:
                sequence = future_sequences.pop(future)
                result = future.result()
                completed_results[sequence] = result
                for request in result.api_requests:
                    dispatch_api_request(request)
                flush_ordered_run_records()

            def flush_ordered_run_records() -> None:
                nonlocal next_record_sequence
                while next_record_sequence in completed_results:
                    result = completed_results.pop(next_record_sequence)
                    run_records.extend(result.run_records)
                    next_record_sequence += 1

            def process_daily_after_factor(
                factor_future: Future[BackgroundTaskResult] | None,
                stock_code: str,
                plans: list[DailyTargetPlan],
                initial_start_date: str,
                daily_df: pd.DataFrame,
                error_stack: str | None,
            ) -> BackgroundTaskResult:
                if factor_future is not None:
                    factor_future.result()
                return background.process_daily_initial(
                    stock_code,
                    plans,
                    initial_start_date,
                    daily_df,
                    error_stack,
                )

            with ThreadPoolExecutor(
                max_workers=background_workers,
                thread_name_prefix="update-baostock-daily-background",
            ) as executor:
                for stock_code in codes:
                    factor_future: Future[BackgroundTaskResult] | None = None
                    if needs_baostock_cn_stock_adjustment_factor_api:
                        factor_start_time = datetime.now()
                        factor_output_path = checkpoint_output_path(store, BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET, stock_code, end_date)
                        try:
                            factor_df = fetch_baostock_cn_stock_adjustment_factor(
                                data_provider,
                                stock_code,
                                FULL_HISTORY_START_DATE,
                                end_date,
                            )
                            log_api_fetch(BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET, stock_code, FULL_HISTORY_START_DATE, end_date, factor_df)
                            factor_future = submit_background(
                                lambda stock_code=stock_code,
                                factor_df=factor_df,
                                factor_start_time=factor_start_time,
                                factor_output_path=factor_output_path: background.process_baostock_cn_stock_adjustment_factor_success(
                                    stock_code,
                                    factor_df,
                                    factor_start_time,
                                    factor_output_path,
                                )
                            )
                        except Exception:
                            error_stack = traceback.format_exc()
                            logger.exception("Adjust factor API failed for {}", stock_code)
                            factor_future = submit_background(
                                lambda stock_code=stock_code,
                                factor_start_time=factor_start_time,
                                factor_output_path=factor_output_path,
                                error_stack=error_stack: background.process_baostock_cn_stock_adjustment_factor_failure(
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
                            daily_df = fetch_daily_bars(
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
                                daily_df=daily_df,
                                factor_future=factor_future: process_daily_after_factor(
                                    factor_future,
                                    stock_code,
                                    plans,
                                    initial_start_date,
                                    daily_df,
                                    None,
                                )
                            )
                        except Exception:
                            error_stack = traceback.format_exc()
                            logger.exception("Daily bar API failed for {}", stock_code)
                            submit_background(
                                lambda stock_code=stock_code,
                                plans=plans,
                                initial_start_date=initial_start_date,
                                error_stack=error_stack,
                                factor_future=factor_future: process_daily_after_factor(
                                    factor_future,
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

        success_count = sum(1 for row in run_records if row.get("status") == "success")
        failed_count = sum(1 for row in run_records if row.get("status") == "failed")
        skipped_count = sum(1 for row in run_records if str(row.get("status", "")).startswith("skipped"))
        should_build_views = build_views
        logger.info(
            "Daily update completed records={} success={} failed={} skipped={}",
            len(run_records),
            success_count,
            failed_count,
            skipped_count,
        )
        return run_records
