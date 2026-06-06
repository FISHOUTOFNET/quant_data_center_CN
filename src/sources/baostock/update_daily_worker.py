"""Background worker for the daily update pipeline."""

from __future__ import annotations

import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Lock

import pandas as pd

from src.pipeline.common import (
    FULL_HISTORY_START_DATE,
    PIPELINE_UPDATE_DAILY,
    daily_frames_differ_on_overlap,
    merge_daily_frames,
)
from src.pipeline.lifecycle import LifecycleTaskRef, PipelineMetadataBatch, failure_rows, success_rows
from src.sources.akshare.pipeline.capital_structure_pending import enqueue_capital_structure_pending
from src.sources.baostock.adjustments import (
    BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET,
    UNADJUSTED_DAILY_DATASET,
    calculate_adjusted_daily_bar,
    is_adjusted_daily_dataset,
)
from src.sources.baostock.update_daily_frames import (
    _baostock_cn_stock_adjustment_factor_frames_differ,
    _has_data_in_range,
    _log_daily_frame,
)
from src.sources.baostock.update_daily_metadata import (
    _write_daily_failure,
    _write_daily_success,
)
from src.sources.baostock.update_daily_types import ApiFetchRequest, BackgroundTaskResult, DailyTargetPlan
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger


@dataclass
class _AdjustFactorState:
    frame: pd.DataFrame
    changed: bool
    error_stack: str | None


class _DailyUpdateBackgroundWorker:
    """Thread-safe worker for update_daily dataframe and storage work."""

    def __init__(
        self,
        store: ParquetStore,
        config: ConfigManager,
        mode: str,
        start_date: str,
        end_date: str,
        metadata_batch: PipelineMetadataBatch,
        retain_factor_state: bool = True,
    ) -> None:
        self._store = store
        self._config = config
        self._mode = mode
        self._start_date = start_date
        self._end_date = end_date
        self._metadata_batch = metadata_batch
        self._retain_factor_state = retain_factor_state
        self._factor_state: dict[str, _AdjustFactorState] = {}
        self._daily_plans_by_code: dict[str, tuple[DailyTargetPlan, ...]] = {}
        self._state_lock = Lock()

    def process_baostock_cn_stock_adjustment_factor_success(
        self,
        code: str,
        fetched: pd.DataFrame,
        start_time: datetime,
        output_path: Path,
    ) -> BackgroundTaskResult:
        try:
            partition = {"code": code}
            existing = self._store.read_dataset(BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET, partition)
            stored = self._store.prepare_dataset_frame(BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET, fetched, partition)
            changed = _baostock_cn_stock_adjustment_factor_frames_differ(self._store, existing, stored)
            output_path = self._store.write_dataset(
                BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET, stored, partition
            ).primary_path
            rows = success_rows(
                LifecycleTaskRef(
                    PIPELINE_UPDATE_DAILY,
                    BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET,
                    code,
                    FULL_HISTORY_START_DATE,
                    self._end_date,
                    output_path,
                ),
                started_at=start_time,
                ended_at=datetime.now(),
                row_count=len(stored),
            )
            self._metadata_batch.add(run_row=rows.run_row, status_row=rows.status_row, checkpoint=rows.checkpoint_row)
            if self._retain_factor_state:
                self._set_factor_state(code, _AdjustFactorState(stored, changed, None))
            if changed:
                try:
                    enqueue_capital_structure_pending(
                        self._store.root,
                        code=code,
                        trigger_dataset=BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET,
                        trigger_reason="adjustment_factor_changed",
                    )
                except Exception as exc:
                    logger.warning("Failed to enqueue capital structure refresh for {}: {}", code, exc)
            return BackgroundTaskResult(run_records=[rows.run_row])
        except Exception:
            error_stack = traceback.format_exc()
            logger.exception("Adjust factor update failed for {}", code)
            return self.process_baostock_cn_stock_adjustment_factor_failure(code, start_time, output_path, error_stack)

    def process_baostock_cn_stock_adjustment_factor_failure(
        self,
        code: str,
        start_time: datetime,
        output_path: Path,
        error_stack: str,
    ) -> BackgroundTaskResult:
        result = BackgroundTaskResult()
        try:
            existing = self._store.read_dataset(BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET, {"code": code})
        except Exception:
            existing = pd.DataFrame()
            error_stack = f"{error_stack}\nFailed to read existing adjust factors:\n{traceback.format_exc()}"

        rows = failure_rows(
            LifecycleTaskRef(
                PIPELINE_UPDATE_DAILY,
                BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET,
                code,
                FULL_HISTORY_START_DATE,
                self._end_date,
                output_path,
            ),
            started_at=start_time,
            ended_at=datetime.now(),
            error_stack=error_stack,
        )
        try:
            self._metadata_batch.add(run_row=rows.run_row, status_row=rows.status_row, checkpoint=rows.checkpoint_row)
        except Exception:
            logger.exception("Failed to persist adjust factor failure for {}", code)

        if self._retain_factor_state:
            if existing.empty:
                self._set_factor_state(code, _AdjustFactorState(existing, False, error_stack))
            else:
                logger.warning("Using existing local adjust factors for {} after fetch failure", code)
                self._set_factor_state(code, _AdjustFactorState(existing, False, None))
        result.run_records.append(rows.run_row)
        return result

    def use_existing_baostock_cn_stock_adjustment_factor(self, code: str) -> None:
        if not self._retain_factor_state:
            return
        existing = self._store.read_dataset(BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET, {"code": code})
        self._set_factor_state(code, _AdjustFactorState(existing, False, None))

    def process_daily_initial(
        self,
        code: str,
        plans: list[DailyTargetPlan],
        fetch_start_date: str | None,
        unadjusted: pd.DataFrame,
        api_error_stack: str | None,
    ) -> BackgroundTaskResult:
        self._set_daily_plans(code, plans)
        try:
            result = self._process_daily_initial(code, plans, fetch_start_date, unadjusted, api_error_stack)
        except Exception:
            error_stack = traceback.format_exc()
            logger.exception("Daily background processing failed for {}", code)
            result = BackgroundTaskResult(run_records=self._write_daily_failures(plans, error_stack))
        if not result.api_requests:
            self._release_code_state(code)
        return result

    def process_daily_full_refetch(
        self,
        request: ApiFetchRequest,
        unadjusted: pd.DataFrame,
        api_error_stack: str | None,
    ) -> BackgroundTaskResult:
        plans = [plan for plan in self._daily_plans_for_code(request.code) if plan.dataset in set(request.datasets)]
        try:
            if api_error_stack is not None:
                result = BackgroundTaskResult(run_records=self._write_daily_failures(plans, api_error_stack))
            else:
                result = BackgroundTaskResult(
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
            result = BackgroundTaskResult(run_records=self._write_daily_failures(plans, error_stack))
        self._release_code_state(request.code)
        return result

    def flush_metadata(self) -> BackgroundTaskResult:
        try:
            self._metadata_batch.flush()
            return BackgroundTaskResult()
        except Exception:
            error_stack = traceback.format_exc()
            logger.exception("Pipeline metadata flush failed")
            return BackgroundTaskResult(run_records=[{"status": "failed", "error_stack": error_stack}])

    def _set_factor_state(self, code: str, state: _AdjustFactorState) -> None:
        with self._state_lock:
            self._factor_state[code] = state

    def _factor_state_for_code(self, code: str) -> _AdjustFactorState:
        with self._state_lock:
            return self._factor_state.get(code, _AdjustFactorState(pd.DataFrame(), False, None))

    def _set_daily_plans(self, code: str, plans: list[DailyTargetPlan]) -> None:
        with self._state_lock:
            self._daily_plans_by_code[code] = tuple(plans)

    def _daily_plans_for_code(self, code: str) -> tuple[DailyTargetPlan, ...]:
        with self._state_lock:
            return self._daily_plans_by_code.get(code, ())

    def _release_code_state(self, code: str) -> None:
        with self._state_lock:
            self._factor_state.pop(code, None)
            self._daily_plans_by_code.pop(code, None)

    def _process_daily_initial(
        self,
        code: str,
        plans: list[DailyTargetPlan],
        fetch_start_date: str | None,
        unadjusted: pd.DataFrame,
        api_error_stack: str | None,
    ) -> BackgroundTaskResult:
        result = BackgroundTaskResult()
        factor_state = self._factor_state_for_code(code)
        active_plans: list[DailyTargetPlan] = []

        for plan in plans:
            if is_adjusted_daily_dataset(plan.dataset) and factor_state.error_stack:
                result.run_records.append(self._write_daily_failure(plan, factor_state.error_stack))
            else:
                active_plans.append(plan)

        if not active_plans:
            return result

        if self._mode == "full":
            if fetch_start_date is None:
                for plan in active_plans:
                    self._log_full_refetch(plan, "baostock_cn_stock_adjustment_factor_changed")
                result.api_requests.append(
                    ApiFetchRequest(
                        kind="daily_bar_full_refetch",
                        code=code,
                        start_date=FULL_HISTORY_START_DATE,
                        end_date=self._end_date,
                        datasets=tuple(dict.fromkeys(plan.dataset for plan in active_plans)),
                        reason="baostock_cn_stock_adjustment_factor_changed",
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

        if api_error_stack is not None:
            result.run_records.extend(self._write_daily_failures(active_plans, api_error_stack))
            return result

        unadjusted_base, refetch_reason = self._partial_unadjusted_base(code, unadjusted)
        if refetch_reason is not None:
            datasets = tuple(dict.fromkeys(plan.dataset for plan in active_plans))
            for plan in active_plans:
                self._log_full_refetch(plan, refetch_reason)
            result.api_requests.append(
                ApiFetchRequest(
                    kind="daily_bar_full_refetch",
                    code=code,
                    start_date=FULL_HISTORY_START_DATE,
                    end_date=self._end_date,
                    datasets=datasets,
                    reason=refetch_reason,
                )
            )
            return result

        if factor_state.changed and any(is_adjusted_daily_dataset(plan.dataset) for plan in active_plans):
            logger.info(
                "Adjust factors changed for {}; recalculating adjusted daily datasets from unadjusted history",
                code,
            )

        for plan in active_plans:
            fresh = self._daily_frame_from_unadjusted(plan.dataset, plan.code, unadjusted_base, factor_state.frame)
            log_start_date = (
                FULL_HISTORY_START_DATE
                if is_adjusted_daily_dataset(plan.dataset)
                else (fetch_start_date or self._start_date)
            )
            _log_daily_frame(plan.dataset, plan.code, log_start_date, self._end_date, fresh)
            run_start_date = (
                FULL_HISTORY_START_DATE if is_adjusted_daily_dataset(plan.dataset) else (plan.checkpoint_start_date)
            )
            result.run_records.append(self._write_daily_success(plan, run_start_date, fresh))
        return result

    def _partial_unadjusted_base(
        self,
        code: str,
        fresh_unadjusted: pd.DataFrame,
    ) -> tuple[pd.DataFrame, str | None]:
        existing = self._store.read_dataset(UNADJUSTED_DAILY_DATASET, {"code": code})
        if not _has_data_in_range(existing, self._start_date, self._end_date):
            return pd.DataFrame(), "unadjusted_empty_lookback"
        if daily_frames_differ_on_overlap(self._store, existing, fresh_unadjusted, self._start_date, self._end_date):
            return pd.DataFrame(), "unadjusted_lookback_mismatch"
        return merge_daily_frames(self._store, existing, fresh_unadjusted), None

    def _write_daily_frames(
        self,
        plans: list[DailyTargetPlan],
        unadjusted: pd.DataFrame,
        run_start_date: str,
        fetch_start_date: str,
    ) -> list[dict[str, object]]:
        records: list[dict[str, object]] = []
        factor_state = self._factor_state_for_code(plans[0].code if plans else "")
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
            return calculate_adjusted_daily_bar(
                unadjusted,
                factors,
                dataset,
                self._config.adjust_flag_for_dataset(dataset),
            )
        raise ValueError(f"Unsupported async daily dataset: {dataset}")

    def _log_full_refetch(self, plan: DailyTargetPlan, reason: str) -> None:
        logger.warning(
            "Daily lookback {} for {} {} from {} to {}; refetching from {}",
            reason,
            plan.dataset,
            plan.code,
            self._start_date,
            self._end_date,
            FULL_HISTORY_START_DATE,
        )
