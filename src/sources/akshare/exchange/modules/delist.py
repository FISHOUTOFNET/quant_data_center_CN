"""AkShare delist update module."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from src.pipeline.common import should_skip_checkpoint
from src.pipeline.lifecycle import LifecycleTaskRef
from src.sources.akshare.core.normalization import date_iso
from src.sources.akshare.pipeline.common import PIPELINE_UPDATE_AKSHARE_DELIST, error_stack
from src.sources.akshare.pipeline.execution import (
    AkShareExecutionContext,
    AkShareUpdateRequest,
    ConcurrencyPolicy,
    FetchResult,
)
from src.storage.dataset_catalog import AKSHARE_DELIST_SH_DATASET, AKSHARE_DELIST_SZ_DATASET
from src.utils.logging import logger

EXCHANGE_CONFIG = {
    "sh": {
        "dataset": AKSHARE_DELIST_SH_DATASET,
        "fetch_method": "fetch_akshare_cn_stock_delist_sh",
        "default_symbol": "全部",
    },
    "sz": {
        "dataset": AKSHARE_DELIST_SZ_DATASET,
        "fetch_method": "fetch_akshare_cn_stock_delist_sz",
        "default_symbol": "终止上市公司",
    },
}


@dataclass(frozen=True)
class DelistTask:
    exchange: str
    dataset: str
    symbol: str
    snapshot_date: str
    output_path: Path
    fetch_method: str
    skipped: bool = False


class DelistModule:
    target = "delist"

    def plan(self, request: AkShareUpdateRequest, context: AkShareExecutionContext) -> list[DelistTask]:
        snapshot_date = date_iso(request.end, datetime.now().date().isoformat())
        tasks: list[DelistTask] = []
        for exchange, config in EXCHANGE_CONFIG.items():
            dataset = config["dataset"].name
            symbol = request.market if request.market else str(config.get("default_symbol", "全部"))
            output_path = context.store.dataset_path(dataset, {"snapshot_date": snapshot_date})
            skipped = should_skip_checkpoint(
                context.store,
                PIPELINE_UPDATE_AKSHARE_DELIST,
                dataset,
                symbol,
                snapshot_date,
                snapshot_date,
                output_path,
                request.resume,
                request.force,
                context.checkpoint_lookup,
            )
            tasks.append(
                DelistTask(
                    exchange=exchange,
                    dataset=dataset,
                    symbol=symbol,
                    snapshot_date=snapshot_date,
                    output_path=output_path,
                    fetch_method=str(config["fetch_method"]),
                    skipped=skipped,
                )
            )
        return tasks

    def prefilter(self, tasks: list[DelistTask], context: AkShareExecutionContext) -> list[DelistTask]:
        return list(tasks)

    def fetch(self, task: DelistTask, context: AkShareExecutionContext) -> FetchResult:
        now = datetime.now()
        if task.skipped:
            return FetchResult(task=task, started_at=now, ended_at=now, skipped=True)
        try:
            fetch_method = getattr(context.client, task.fetch_method)
            response = fetch_method(symbol=task.symbol, snapshot_date=task.snapshot_date)
            return FetchResult(task=task, started_at=now, ended_at=datetime.now(), response=response)
        except Exception as exc:
            return FetchResult(
                task=task, started_at=now, ended_at=datetime.now(), error=exc, error_stack=error_stack(exc)
            )

    def record_result(self, result: FetchResult, context: AkShareExecutionContext) -> list[dict[str, object]]:
        task = result.task
        if result.skipped:
            return []
        if result.error is not None:
            rows = context.lifecycle.record_failure(
                _task_ref(task.dataset, task.symbol, task.snapshot_date, task.output_path),
                started_at=result.started_at,
                ended_at=result.ended_at,
                error_stack=result.error_stack,
            )
            return [rows.run_row]
        try:
            assert result.response is not None
            output_path = context.store.write_dataset(
                task.dataset, result.response.data, {"snapshot_date": task.snapshot_date}
            ).primary_path
            rows = context.lifecycle.record_success(
                _task_ref(task.dataset, task.symbol, task.snapshot_date, output_path),
                started_at=result.started_at,
                ended_at=datetime.now(),
                row_count=len(result.response.data),
                output_path=output_path,
            )
            return [rows.run_row]
        except Exception as exc:
            rows = context.lifecycle.record_failure(
                _task_ref(task.dataset, task.symbol, task.snapshot_date, task.output_path),
                started_at=result.started_at,
                ended_at=datetime.now(),
                error_stack=error_stack(exc),
            )
            return [rows.run_row]

    def record_skip(self, task: DelistTask, context: AkShareExecutionContext) -> list[dict[str, object]]:
        return []

    def progress_row(self, task: DelistTask, rows: list[dict[str, object]]) -> dict[str, object]:
        if rows:
            return rows[-1]
        return {"dataset": task.dataset, "code": task.symbol, "status": "skipped_checkpoint", "row_count": 0}

    def concurrency(self, request: AkShareUpdateRequest, context: AkShareExecutionContext) -> ConcurrencyPolicy:
        return ConcurrencyPolicy(workers=1)

    def log_started(self, request: AkShareUpdateRequest, planned: int, processing: int, workers: int) -> None:
        logger.info(
            "AkShare delist update started market={} snapshot_date={} force={} planned_tasks={} processing_tasks={}",
            request.market or "",
            request.end or "",
            request.force,
            planned,
            processing,
        )

    def log_progress(self, progress: Any, task: DelistTask, row: dict[str, object]) -> None:
        logger.info(
            "AkShare delist progress {}/{} exchange={} code={} dataset={} status={} rows={}",
            progress.processed,
            progress.total,
            task.exchange,
            row.get("code", task.symbol),
            row.get("dataset", task.dataset),
            row.get("status", "unknown"),
            row.get("row_count", 0),
        )

    def log_completed(self, progress: Any) -> None:
        logger.info(
            "AkShare delist update completed processed={} success={} failed={} skipped={}",
            progress.processed,
            progress.success,
            progress.failed,
            progress.skipped,
        )


def _task_ref(dataset: str, code: str, snapshot_date: str, output_path: Path) -> LifecycleTaskRef:
    return LifecycleTaskRef(
        PIPELINE_UPDATE_AKSHARE_DELIST,
        dataset,
        code,
        snapshot_date,
        snapshot_date,
        output_path,
    )
