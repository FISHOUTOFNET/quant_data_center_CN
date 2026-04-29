"""Daily update pipeline with lookback overwrite and metadata state."""

from __future__ import annotations

import traceback
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import pandas as pd

from src.api.market_data import MarketDataProvider, create_provider
from src.pipeline.adjustments import (
    ADJUST_FACTOR_DATASET,
    UNADJUSTED_DAILY_DATASET,
    calculate_adjusted_daily_k,
    is_adjusted_daily_dataset,
)
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
    expand_daily_datasets,
    latest_trading_day_on_or_before,
    merge_daily_frames,
    resolve_codes,
    should_skip_checkpoint,
    trading_day_lookback_start,
    trading_range_bounds,
    write_checkpoint,
)
from src.pipeline.services import (
    PipelineMetadataBatch,
    fetch_adjust_factor,
    fetch_daily_k,
    fetch_stock_basic,
    log_api_fetch,
)
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore
from src.storage.schema import ADJUST_FACTOR_SCHEMA
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger


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
        codes = resolve_codes(config, store, code, universe, stock_basic_mode=stock_basic_mode) if needs_code_pool else []

        needs_adjust_factor_api = include_adjust_factor or _needs_adjust_factors(daily_targets)
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
                    if should_skip_checkpoint(
                        store,
                        PIPELINE_UPDATE_DAILY,
                        ADJUST_FACTOR_DATASET,
                        stock_code,
                        FULL_HISTORY_START_DATE,
                        end_date,
                        factor_output_path,
                        resume,
                        force,
                        checkpoint_lookup,
                    ):
                        run_row = _skipped_run_row(
                            ADJUST_FACTOR_DATASET,
                            stock_code,
                            FULL_HISTORY_START_DATE,
                            end_date,
                            "checkpoint",
                        )
                        submit_background(
                            lambda stock_code=stock_code, run_row=run_row: background.process_adjust_factor_skipped(
                                stock_code,
                                run_row,
                            )
                        )
                    else:
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
                        resume,
                        force,
                        checkpoint_lookup,
                    )
                    initial_start_date = FULL_HISTORY_START_DATE if mode == "full" else start_date
                    if _needs_initial_daily_fetch(plans):
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
                    else:
                        submit_background(
                            lambda stock_code=stock_code, plans=plans: background.process_daily_initial(
                                stock_code,
                                plans,
                                None,
                                pd.DataFrame(),
                                None,
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


@dataclass(frozen=True)
class DailyTargetPlan:
    dataset: str
    code: str
    checkpoint_start_date: str
    end_date: str
    output_path: Path
    start_time: datetime
    skipped_by_checkpoint: bool


@dataclass(frozen=True)
class ApiFetchRequest:
    """Background-to-main request for provider calls.

    Provider APIs must stay on the main dispatcher thread. New per-code APIs
    should add request kinds here instead of calling providers from workers.
    """

    kind: str
    code: str
    start_date: str
    end_date: str
    datasets: tuple[str, ...]
    reason: str


@dataclass
class BackgroundTaskResult:
    run_records: list[dict[str, object]] = field(default_factory=list)
    api_requests: list[ApiFetchRequest] = field(default_factory=list)


@dataclass
class _AdjustFactorState:
    frame: pd.DataFrame
    changed: bool
    error_stack: str | None


class _DailyUpdateBackgroundWorker:
    """Single-thread worker for update_daily dataframe and storage work."""

    def __init__(
        self,
        store: ParquetStore,
        config: ConfigManager,
        mode: str,
        start_date: str,
        end_date: str,
        metadata_batch: PipelineMetadataBatch,
    ) -> None:
        self._store = store
        self._config = config
        self._mode = mode
        self._start_date = start_date
        self._end_date = end_date
        self._metadata_batch = metadata_batch
        self._factor_state: dict[str, _AdjustFactorState] = {}
        self._daily_plans_by_code: dict[str, tuple[DailyTargetPlan, ...]] = {}

    def process_adjust_factor_skipped(self, code: str, run_row: dict[str, object]) -> BackgroundTaskResult:
        result = BackgroundTaskResult()
        try:
            _persist_run_status(self._store, run_row)
            factors = self._store.read_adjust_factor(code)
            self._factor_state[code] = _AdjustFactorState(factors, False, None)
            result.run_records.append(run_row)
        except Exception:
            error_stack = traceback.format_exc()
            logger.exception("Adjust factor skip handling failed for {}", code)
            self._factor_state[code] = _AdjustFactorState(pd.DataFrame(), False, error_stack)
            result.run_records.append(
                _run_row(
                    ADJUST_FACTOR_DATASET,
                    code,
                    "failed",
                    FULL_HISTORY_START_DATE,
                    self._end_date,
                    datetime.now(),
                    datetime.now(),
                    0,
                    error_stack,
                )
            )
        return result

    def process_adjust_factor_success(
        self,
        code: str,
        fetched: pd.DataFrame,
        start_time: datetime,
        output_path: Path,
    ) -> BackgroundTaskResult:
        try:
            existing = self._store.read_adjust_factor(code)
            changed = _adjust_factor_frames_differ(self._store, existing, fetched)
            output_path = self._store.write_adjust_factor(code, fetched)
            stored = self._store.read_adjust_factor(code)
            run_row = _run_row(
                ADJUST_FACTOR_DATASET,
                code,
                "success",
                FULL_HISTORY_START_DATE,
                self._end_date,
                start_time,
                datetime.now(),
                len(stored),
                "",
            )
            status_row = _status_row(ADJUST_FACTOR_DATASET, code, self._end_date, len(stored), "success", "")
            _persist_run_status(self._store, run_row, status_row)
            write_checkpoint(
                self._store,
                PIPELINE_UPDATE_DAILY,
                ADJUST_FACTOR_DATASET,
                code,
                FULL_HISTORY_START_DATE,
                self._end_date,
                "success",
                len(stored),
                output_path,
            )
            self._factor_state[code] = _AdjustFactorState(stored, changed, None)
            return BackgroundTaskResult(run_records=[run_row])
        except Exception:
            error_stack = traceback.format_exc()
            logger.exception("Adjust factor update failed for {}", code)
            return self.process_adjust_factor_failure(code, start_time, output_path, error_stack)

    def process_adjust_factor_failure(
        self,
        code: str,
        start_time: datetime,
        output_path: Path,
        error_stack: str,
    ) -> BackgroundTaskResult:
        result = BackgroundTaskResult()
        try:
            existing = self._store.read_adjust_factor(code)
        except Exception:
            existing = pd.DataFrame()
            error_stack = f"{error_stack}\nFailed to read existing adjust factors:\n{traceback.format_exc()}"

        run_row = _run_row(
            ADJUST_FACTOR_DATASET,
            code,
            "failed",
            FULL_HISTORY_START_DATE,
            self._end_date,
            start_time,
            datetime.now(),
            0,
            error_stack,
        )
        status_row = _status_row(ADJUST_FACTOR_DATASET, code, None, 0, "failed", error_stack)
        try:
            _persist_run_status(self._store, run_row, status_row)
            write_checkpoint(
                self._store,
                PIPELINE_UPDATE_DAILY,
                ADJUST_FACTOR_DATASET,
                code,
                FULL_HISTORY_START_DATE,
                self._end_date,
                "failed",
                0,
                output_path,
                error_stack,
            )
        except Exception:
            logger.exception("Failed to persist adjust factor failure for {}", code)

        if existing.empty:
            self._factor_state[code] = _AdjustFactorState(existing, False, error_stack)
        else:
            logger.warning("Using existing local adjust factors for {} after fetch failure", code)
            self._factor_state[code] = _AdjustFactorState(existing, False, None)
        result.run_records.append(run_row)
        return result

    def process_daily_initial(
        self,
        code: str,
        plans: list[DailyTargetPlan],
        fetch_start_date: str | None,
        unadjusted: pd.DataFrame,
        api_error_stack: str | None,
    ) -> BackgroundTaskResult:
        self._daily_plans_by_code[code] = tuple(plans)
        try:
            return self._process_daily_initial(code, plans, fetch_start_date, unadjusted, api_error_stack)
        except Exception:
            error_stack = traceback.format_exc()
            logger.exception("Daily background processing failed for {}", code)
            return BackgroundTaskResult(run_records=self._write_daily_failures(plans, error_stack))

    def process_daily_full_refetch(
        self,
        request: ApiFetchRequest,
        unadjusted: pd.DataFrame,
        api_error_stack: str | None,
    ) -> BackgroundTaskResult:
        plans = [
            plan
            for plan in self._daily_plans_by_code.get(request.code, ())
            if plan.dataset in set(request.datasets)
        ]
        try:
            if api_error_stack is not None:
                return BackgroundTaskResult(run_records=self._write_daily_failures(plans, api_error_stack))
            return BackgroundTaskResult(
                run_records=self._write_daily_frames(
                    plans,
                    unadjusted,
                    FULL_HISTORY_START_DATE,
                    request.start_date,
                )
            )
        except Exception:
            error_stack = traceback.format_exc()
            logger.exception("Daily full refetch processing failed for {}", request.code)
            return BackgroundTaskResult(run_records=self._write_daily_failures(plans, error_stack))

    def flush_metadata(self) -> BackgroundTaskResult:
        try:
            self._metadata_batch.flush()
            return BackgroundTaskResult()
        except Exception:
            error_stack = traceback.format_exc()
            logger.exception("Pipeline metadata flush failed")
            return BackgroundTaskResult(run_records=[{"status": "failed", "error_stack": error_stack}])

    def _process_daily_initial(
        self,
        code: str,
        plans: list[DailyTargetPlan],
        fetch_start_date: str | None,
        unadjusted: pd.DataFrame,
        api_error_stack: str | None,
    ) -> BackgroundTaskResult:
        result = BackgroundTaskResult()
        factor_state = self._factor_state.get(code, _AdjustFactorState(pd.DataFrame(), False, None))
        active_plans: list[DailyTargetPlan] = []

        for plan in plans:
            if plan.skipped_by_checkpoint:
                if is_adjusted_daily_dataset(plan.dataset) and factor_state.error_stack:
                    result.run_records.append(self._write_daily_failure(plan, factor_state.error_stack))
                elif is_adjusted_daily_dataset(plan.dataset) and factor_state.changed:
                    active_plans.append(plan)
                else:
                    run_row = _skipped_run_row(
                        plan.dataset,
                        plan.code,
                        plan.checkpoint_start_date,
                        plan.end_date,
                        "checkpoint",
                    )
                    result.run_records.append(_write_skipped_run(self._store, run_row, self._metadata_batch))
            elif is_adjusted_daily_dataset(plan.dataset) and factor_state.error_stack:
                result.run_records.append(self._write_daily_failure(plan, factor_state.error_stack))
            else:
                active_plans.append(plan)

        if not active_plans:
            return result

        if self._mode == "full":
            if fetch_start_date is None:
                for plan in active_plans:
                    self._log_full_refetch(plan, "adjust_factor_changed")
                result.api_requests.append(
                    ApiFetchRequest(
                        kind="daily_k_full_refetch",
                        code=code,
                        start_date=FULL_HISTORY_START_DATE,
                        end_date=self._end_date,
                        datasets=tuple(dict.fromkeys(plan.dataset for plan in active_plans)),
                        reason="adjust_factor_changed",
                    )
                )
                return result
            if api_error_stack is not None:
                result.run_records.extend(self._write_daily_failures(active_plans, api_error_stack))
            else:
                result.run_records.extend(
                    self._write_daily_frames(
                        active_plans,
                        unadjusted,
                        FULL_HISTORY_START_DATE,
                        fetch_start_date or FULL_HISTORY_START_DATE,
                    )
                )
            return result

        full_plans: list[DailyTargetPlan] = []
        full_reasons: dict[str, str] = {}
        remaining_plans: list[DailyTargetPlan] = []

        for plan in active_plans:
            if is_adjusted_daily_dataset(plan.dataset) and factor_state.changed:
                self._log_full_refetch(plan, "adjust_factor_changed")
                full_plans.append(plan)
                full_reasons[plan.dataset] = "adjust_factor_changed"
            else:
                remaining_plans.append(plan)

        if api_error_stack is not None:
            result.run_records.extend(self._write_daily_failures(remaining_plans, api_error_stack))
        else:
            for plan in remaining_plans:
                fresh = self._daily_frame_from_unadjusted(plan.dataset, plan.code, unadjusted, factor_state.frame)
                _log_daily_frame(plan.dataset, plan.code, fetch_start_date or self._start_date, self._end_date, fresh)
                existing = self._store.read_daily_k(plan.dataset, plan.code)
                if not _has_data_in_range(existing, self._start_date, self._end_date):
                    self._log_full_refetch(plan, "empty_lookback")
                    full_plans.append(plan)
                    full_reasons[plan.dataset] = "empty_lookback"
                elif daily_frames_differ_on_overlap(self._store, existing, fresh, self._start_date, self._end_date):
                    self._log_full_refetch(plan, "lookback_mismatch")
                    full_plans.append(plan)
                    full_reasons[plan.dataset] = "lookback_mismatch"
                else:
                    final_df = merge_daily_frames(self._store, existing, fresh)
                    result.run_records.append(
                        self._write_daily_success(plan, plan.checkpoint_start_date, final_df)
                    )

        if full_plans:
            datasets = tuple(dict.fromkeys(plan.dataset for plan in full_plans))
            reason = ",".join(dict.fromkeys(full_reasons[dataset] for dataset in datasets))
            result.api_requests.append(
                ApiFetchRequest(
                    kind="daily_k_full_refetch",
                    code=code,
                    start_date=FULL_HISTORY_START_DATE,
                    end_date=self._end_date,
                    datasets=datasets,
                    reason=reason,
                )
            )
        return result

    def _write_daily_frames(
        self,
        plans: list[DailyTargetPlan],
        unadjusted: pd.DataFrame,
        run_start_date: str,
        fetch_start_date: str,
    ) -> list[dict[str, object]]:
        records: list[dict[str, object]] = []
        factor_state = self._factor_state.get(
            plans[0].code if plans else "",
            _AdjustFactorState(pd.DataFrame(), False, None),
        )
        for plan in plans:
            try:
                fresh = self._daily_frame_from_unadjusted(plan.dataset, plan.code, unadjusted, factor_state.frame)
                _log_daily_frame(plan.dataset, plan.code, fetch_start_date, self._end_date, fresh)
                records.append(self._write_daily_success(plan, run_start_date, fresh))
            except Exception:
                error_stack = traceback.format_exc()
                logger.exception("Daily write failed for {} {}", plan.dataset, plan.code)
                records.append(self._write_daily_failure(plan, error_stack))
        return records

    def _write_daily_success(
        self,
        plan: DailyTargetPlan,
        run_start_date: str,
        df: pd.DataFrame,
    ) -> dict[str, object]:
        return _write_daily_success(
            self._store,
            self._metadata_batch,
            plan.dataset,
            plan.code,
            run_start_date,
            plan.end_date,
            plan.start_time,
            df,
            plan.checkpoint_start_date,
        )

    def _write_daily_failure(self, plan: DailyTargetPlan, error_stack: str) -> dict[str, object]:
        return _write_daily_failure(
            self._store,
            self._metadata_batch,
            plan.dataset,
            plan.code,
            plan.checkpoint_start_date,
            plan.end_date,
            plan.start_time,
            plan.output_path,
            error_stack,
        )

    def _write_daily_failures(
        self,
        plans: list[DailyTargetPlan],
        error_stack: str,
    ) -> list[dict[str, object]]:
        return [self._write_daily_failure(plan, error_stack) for plan in plans]

    def _daily_frame_from_unadjusted(
        self,
        dataset: str,
        code: str,
        unadjusted: pd.DataFrame,
        factors: pd.DataFrame,
    ) -> pd.DataFrame:
        if dataset == UNADJUSTED_DAILY_DATASET:
            return unadjusted.copy()
        if is_adjusted_daily_dataset(dataset):
            return calculate_adjusted_daily_k(
                unadjusted,
                factors,
                dataset,
                self._config.adjustflag_for_dataset(dataset),
            )
        raise ValueError(f"Unsupported async daily dataset: {dataset}")

    def _log_full_refetch(self, plan: DailyTargetPlan, reason: str) -> None:
        if reason == "adjust_factor_changed":
            logger.warning(
                "Daily lookback {} for {} {}; recomputing from {}",
                reason,
                plan.dataset,
                plan.code,
                FULL_HISTORY_START_DATE,
            )
            return
        logger.warning(
            "Daily lookback {} for {} {} from {} to {}; refetching from {}",
            reason,
            plan.dataset,
            plan.code,
            self._start_date,
            self._end_date,
            FULL_HISTORY_START_DATE,
        )


def _daily_target_plans(
    store: ParquetStore,
    daily_targets: list[str],
    code: str,
    mode: str,
    start_date: str,
    end_date: str,
    resume: bool,
    force: bool,
    checkpoint_lookup: PipelineCheckpointLookup | None,
) -> list[DailyTargetPlan]:
    plans: list[DailyTargetPlan] = []
    checkpoint_start_date = FULL_HISTORY_START_DATE if mode == "full" else start_date
    for dataset in daily_targets:
        output_path = checkpoint_output_path(store, dataset, code, end_date)
        plans.append(
            DailyTargetPlan(
                dataset=dataset,
                code=code,
                checkpoint_start_date=checkpoint_start_date,
                end_date=end_date,
                output_path=output_path,
                start_time=datetime.now(),
                skipped_by_checkpoint=should_skip_checkpoint(
                    store,
                    PIPELINE_UPDATE_DAILY,
                    dataset,
                    code,
                    checkpoint_start_date,
                    end_date,
                    output_path,
                    resume,
                    force,
                    checkpoint_lookup,
                ),
            )
        )
    return plans


def _needs_initial_daily_fetch(plans: list[DailyTargetPlan]) -> bool:
    return any(not plan.skipped_by_checkpoint for plan in plans)


def _dataset_targets(dataset: str) -> tuple[bool, bool, bool, list[str]]:
    if dataset == "all":
        return True, True, True, list(DAILY_K_DATASETS)
    if dataset == "calendar":
        return True, False, False, []
    if dataset == "stock_basic":
        return False, True, False, []
    if dataset == ADJUST_FACTOR_DATASET:
        return False, False, True, []
    if dataset in {"daily_k_all", "daily_k"} or dataset in DAILY_K_DATASETS:
        return False, False, False, expand_daily_datasets(dataset)
    raise ValueError(f"Unsupported update dataset: {dataset}")


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


def _write_stock_basic_target(
    store: ParquetStore,
    provider: MarketDataProvider,
    run_records: list[dict[str, object]],
    start_date: str,
    end_date: str,
    resume: bool,
    force: bool,
    checkpoint_lookup: PipelineCheckpointLookup | None,
) -> None:
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
        return

    basic_df = fetch_stock_basic(provider)
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


def _write_adjust_factor_target(
    store: ParquetStore,
    provider: MarketDataProvider,
    code: str,
    end_date: str,
    resume: bool,
    force: bool,
    checkpoint_lookup: PipelineCheckpointLookup | None,
) -> tuple[pd.DataFrame, bool, str | None, dict[str, object] | None]:
    output_path = checkpoint_output_path(store, ADJUST_FACTOR_DATASET, code, end_date)
    if should_skip_checkpoint(
        store,
        PIPELINE_UPDATE_DAILY,
        ADJUST_FACTOR_DATASET,
        code,
        FULL_HISTORY_START_DATE,
        end_date,
        output_path,
        resume,
        force,
        checkpoint_lookup,
    ):
        run_row = _skipped_run_row(ADJUST_FACTOR_DATASET, code, FULL_HISTORY_START_DATE, end_date, "checkpoint")
        _persist_run_status(store, run_row)
        return store.read_adjust_factor(code), False, None, run_row

    start_time = datetime.now()
    try:
        existing = store.read_adjust_factor(code)
        fetched = fetch_adjust_factor(provider, code, FULL_HISTORY_START_DATE, end_date)
        log_api_fetch(ADJUST_FACTOR_DATASET, code, FULL_HISTORY_START_DATE, end_date, fetched)
        changed = _adjust_factor_frames_differ(store, existing, fetched)
        output_path = store.write_adjust_factor(code, fetched)
        stored = store.read_adjust_factor(code)
        run_row = _run_row(
            ADJUST_FACTOR_DATASET,
            code,
            "success",
            FULL_HISTORY_START_DATE,
            end_date,
            start_time,
            datetime.now(),
            len(stored),
            "",
        )
        status_row = _status_row(ADJUST_FACTOR_DATASET, code, end_date, len(stored), "success", "")
        _persist_run_status(store, run_row, status_row)
        write_checkpoint(
            store,
            PIPELINE_UPDATE_DAILY,
            ADJUST_FACTOR_DATASET,
            code,
            FULL_HISTORY_START_DATE,
            end_date,
            "success",
            len(stored),
            output_path,
        )
        return stored, changed, None, run_row
    except Exception:
        error_stack = traceback.format_exc()
        logger.exception("Adjust factor update failed for {}", code)
        existing = store.read_adjust_factor(code)
        run_row = _run_row(
            ADJUST_FACTOR_DATASET,
            code,
            "failed",
            FULL_HISTORY_START_DATE,
            end_date,
            start_time,
            datetime.now(),
            0,
            error_stack,
        )
        status_row = _status_row(ADJUST_FACTOR_DATASET, code, None, 0, "failed", error_stack)
        try:
            _persist_run_status(store, run_row, status_row)
            write_checkpoint(
                store,
                PIPELINE_UPDATE_DAILY,
                ADJUST_FACTOR_DATASET,
                code,
                FULL_HISTORY_START_DATE,
                end_date,
                "failed",
                0,
                output_path,
                error_stack,
            )
        except Exception:
            logger.exception("Failed to persist adjust factor failure for {}", code)
        if existing.empty:
            return existing, False, error_stack, run_row
        logger.warning("Using existing local adjust factors for {} after fetch failure", code)
        return existing, False, None, run_row


def _query_daily_k(
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
        return _query_unadjusted_daily_k(provider, config, stock_code, start_date, end_date, unadjusted_cache)
    if is_adjusted_daily_dataset(dataset):
        unadjusted = _query_unadjusted_daily_k(
            provider,
            config,
            stock_code,
            start_date,
            end_date,
            unadjusted_cache,
        )
        factors = factor_cache.get(stock_code, pd.DataFrame()) if factor_cache is not None else pd.DataFrame()
        return calculate_adjusted_daily_k(
            unadjusted,
            factors,
            dataset,
            config.adjustflag_for_dataset(dataset),
        )
    return fetch_daily_k(provider, config, dataset, stock_code, start_date, end_date)


def _query_unadjusted_daily_k(
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
    df = fetch_daily_k(provider, config, UNADJUSTED_DAILY_DATASET, stock_code, start_date, end_date)
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


def _adjust_factor_frames_differ(store: ParquetStore, existing: pd.DataFrame, fresh: pd.DataFrame) -> bool:
    left = store.clean_dataframe_for_schema(existing, ADJUST_FACTOR_SCHEMA)
    right = store.clean_dataframe_for_schema(fresh, ADJUST_FACTOR_SCHEMA)
    if not left.empty:
        left = left.sort_values(["code", "dividOperateDate"]).reset_index(drop=True)
    if not right.empty:
        right = right.sort_values(["code", "dividOperateDate"]).reset_index(drop=True)
    return not left.equals(right)


def _needs_adjust_factors(daily_targets: list[str]) -> bool:
    return any(is_adjusted_daily_dataset(dataset) for dataset in daily_targets)


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
