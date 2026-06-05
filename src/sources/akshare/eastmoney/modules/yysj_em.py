"""AkShare Eastmoney report disclosure schedule update module."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from src.sources.akshare.cninfo.adapters.report_disclosure import report_period_end_date
from src.sources.akshare.pipeline.execution import AkShareExecutionContext, AkShareUpdateRequest, ConcurrencyPolicy, FetchResult
from src.sources.akshare.pipeline.common import error_stack
from src.pipeline.common import should_skip_checkpoint
from src.pipeline.lifecycle import LifecycleTaskRef
from src.storage.dataset_catalog import AKSHARE_YYSJ_EM_DATASET
from src.utils.logging import logger

PIPELINE_UPDATE_AKSHARE_YYSJ_EM = "update_akshare_yysj_em"
DEFAULT_SYMBOLS = ("沪深A股", "京市A股")
REPORT_PERIOD_SUFFIXES = ("一季", "半年报", "三季", "年报")
YYSJ_EM_MIN_PERIOD_END_DATE = date(2008, 12, 31)


@dataclass(frozen=True)
class YysjEmTask:
    dataset: str
    symbol: str
    period: str
    period_end_date: str
    output_path: Path
    skipped: bool = False


class YysjEmModule:
    target = "yysj_em"

    def plan(self, request: AkShareUpdateRequest, context: AkShareExecutionContext) -> list[YysjEmTask]:
        today = _request_today(request)
        symbols = _resolve_symbols(request.market)
        periods = _resolve_periods(
            request,
            today,
            context.config.get("datasets.akshare_cn_stock_yysj_em.full_start_year", 2008),
        )
        tasks: list[YysjEmTask] = []
        for period in periods:
            period_end = report_period_end_date(period).isoformat()
            output_path = context.store.dataset_path(
                AKSHARE_YYSJ_EM_DATASET.name,
                {"report_period": period},
            )
            for symbol in symbols:
                skipped = should_skip_checkpoint(
                    context.store,
                    PIPELINE_UPDATE_AKSHARE_YYSJ_EM,
                    AKSHARE_YYSJ_EM_DATASET.name,
                    symbol,
                    period_end,
                    period_end,
                    output_path,
                    request.resume,
                    request.force,
                    context.checkpoint_lookup,
                )
                tasks.append(
                    YysjEmTask(
                        dataset=AKSHARE_YYSJ_EM_DATASET.name,
                        symbol=symbol,
                        period=period,
                        period_end_date=period_end,
                        output_path=output_path,
                        skipped=skipped,
                    )
                )
        if request.max_tasks is not None:
            tasks = tasks[: max(int(request.max_tasks), 0)]
        return tasks

    def prefilter(self, tasks: list[YysjEmTask], context: AkShareExecutionContext) -> list[YysjEmTask]:
        return list(tasks)

    def fetch(self, task: YysjEmTask, context: AkShareExecutionContext) -> FetchResult:
        now = datetime.now()
        if task.skipped:
            return FetchResult(task=task, started_at=now, ended_at=now, skipped=True)
        try:
            response = context.client.fetch_yysj_em(symbol=task.symbol, period=task.period)
            return FetchResult(task=task, started_at=now, ended_at=datetime.now(), response=response)
        except Exception as exc:
            return FetchResult(
                task=task,
                started_at=now,
                ended_at=datetime.now(),
                error=exc,
                error_stack=error_stack(exc),
            )

    def record_result(self, result: FetchResult, context: AkShareExecutionContext) -> list[dict[str, object]]:
        task = result.task
        if result.skipped:
            return []
        if result.error is not None:
            rows = context.lifecycle.record_failure(
                _task_ref(task),
                started_at=result.started_at,
                ended_at=result.ended_at,
                error_stack=result.error_stack,
            )
            return [rows.run_row]
        try:
            if result.response is None:
                raise RuntimeError("stock_yysj_em returned no response")
            output_path = context.store.write_dataset(
                task.dataset,
                result.response.data,
                {"report_period": task.period},
                mode="merge",
            ).primary_path
            rows = context.lifecycle.record_success(
                _task_ref(task),
                started_at=result.started_at,
                ended_at=datetime.now(),
                row_count=len(result.response.data),
                output_path=output_path,
                last_success_date=task.period_end_date,
            )
            return [rows.run_row]
        except Exception as exc:
            rows = context.lifecycle.record_failure(
                _task_ref(task),
                started_at=result.started_at,
                ended_at=datetime.now(),
                error_stack=error_stack(exc),
            )
            return [rows.run_row]

    def record_skip(
        self,
        task: YysjEmTask,
        context: AkShareExecutionContext,
        status: str = "skipped_checkpoint",
        reason: str = "checkpoint",
    ) -> list[dict[str, object]]:
        now = datetime.now()
        rows = context.lifecycle.record_skipped(
            _task_ref(task),
            status=status,
            started_at=now,
            ended_at=now,
            reason=reason,
        )
        return [rows.run_row]

    def progress_row(self, task: YysjEmTask, rows: list[dict[str, object]]) -> dict[str, object]:
        if rows:
            return rows[-1]
        return {"dataset": task.dataset, "code": task.symbol, "status": "skipped_checkpoint", "row_count": 0}

    def concurrency(self, request: AkShareUpdateRequest, context: AkShareExecutionContext) -> ConcurrencyPolicy:
        return ConcurrencyPolicy(workers=1)

    def log_started(self, request: AkShareUpdateRequest, planned: int, processing: int, workers: int) -> None:
        logger.info(
            "AkShare yysj_em update started symbols={} mode={} force={} planned_tasks={} processing_tasks={}",
            ",".join(_resolve_symbols(request.market)),
            request.mode,
            request.force,
            planned,
            processing,
        )

    def log_progress(self, progress: Any, task: YysjEmTask, row: dict[str, object]) -> None:
        logger.info(
            "AkShare yysj_em progress {}/{} symbol={} period={} dataset={} status={} rows={}",
            progress.processed,
            progress.total,
            task.symbol,
            task.period,
            row.get("dataset", task.dataset),
            row.get("status", "unknown"),
            row.get("row_count", 0),
        )

    def log_completed(self, progress: Any) -> None:
        logger.info(
            "AkShare yysj_em update completed processed={} success={} failed={} skipped={}",
            progress.processed,
            progress.success,
            progress.failed,
            progress.skipped,
        )


def _resolve_symbols(symbol: str | None) -> tuple[str, ...]:
    if symbol is None or str(symbol).strip() == "":
        return DEFAULT_SYMBOLS
    return (str(symbol).strip(),)


def _resolve_periods(request: AkShareUpdateRequest, today: date, full_start_year: object) -> list[str]:
    explicit_periods = _normalize_explicit_periods(request.period)
    if explicit_periods:
        return explicit_periods
    if request.mode == "full":
        start_year = int(full_start_year)
        return _periods_between(start_year, today)
    if request.mode != "partial":
        raise ValueError(f"Unsupported AkShare yysj_em update mode: {request.mode}")
    return _periods_between(today.year - 2, today)[-4:]


def _normalize_explicit_periods(period: tuple[str, ...] | list[str] | str | None) -> list[str]:
    if period is None:
        return []
    values = [period] if isinstance(period, str) else list(period)
    periods: list[str] = []
    for item in values:
        value = str(item).strip()
        if not value:
            continue
        report_period_end_date(value)
        periods.append(value)
    return list(dict.fromkeys(periods))


def _periods_between(start_year: int, today: date) -> list[str]:
    periods: list[str] = []
    for year in range(start_year, today.year + 1):
        for suffix in REPORT_PERIOD_SUFFIXES:
            period = f"{year}{suffix}"
            period_end = report_period_end_date(period)
            if YYSJ_EM_MIN_PERIOD_END_DATE <= period_end <= today:
                periods.append(period)
    return periods


def _request_today(request: AkShareUpdateRequest) -> date:
    now = (request.now or datetime.now)()
    if isinstance(now, datetime):
        return now.date()
    return now


def _task_ref(task: YysjEmTask) -> LifecycleTaskRef:
    return LifecycleTaskRef(
        PIPELINE_UPDATE_AKSHARE_YYSJ_EM,
        task.dataset,
        task.symbol,
        task.period_end_date,
        task.period_end_date,
        task.output_path,
    )
