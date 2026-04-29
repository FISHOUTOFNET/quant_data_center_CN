"""Background worker for the daily update pipeline."""

from __future__ import annotations

import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.pipeline.adjustments import (
    ADJUST_FACTOR_DATASET,
    UNADJUSTED_DAILY_DATASET,
    calculate_adjusted_daily_k,
    is_adjusted_daily_dataset,
)
from src.pipeline.common import (
    FULL_HISTORY_START_DATE,
    PIPELINE_UPDATE_DAILY,
    daily_frames_differ_on_overlap,
    merge_daily_frames,
    write_checkpoint,
)
from src.pipeline.services import PipelineMetadataBatch
from src.pipeline.update_daily_frames import (
    _adjust_factor_frames_differ,
    _has_data_in_range,
    _log_daily_frame,
)
from src.pipeline.update_daily_metadata import (
    _persist_run_status,
    _run_row,
    _status_row,
    _write_daily_failure,
    _write_daily_success,
)
from src.pipeline.update_daily_types import ApiFetchRequest, BackgroundTaskResult, DailyTargetPlan
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger


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
            if is_adjusted_daily_dataset(plan.dataset) and factor_state.error_stack:
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
