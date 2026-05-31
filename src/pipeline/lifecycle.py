"""Shared pipeline metadata lifecycle helpers."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from src.pipeline.common import checkpoint_row
from src.storage.data_registry import DataRegistry
from src.storage.parquet_store import ParquetStore
from src.utils.logging import logger
from src.utils.run_context import pipeline_log_values


@dataclass(frozen=True)
class LifecycleTaskRef:
    pipeline: str
    dataset: str
    code: str
    start_date: str
    end_date: str
    output_path: str | Path
    checkpoint_start_date: str | None = None

    @property
    def effective_checkpoint_start_date(self) -> str:
        return self.checkpoint_start_date or self.start_date


@dataclass(frozen=True)
class LifecycleRows:
    run_row: dict[str, object]
    status_row: dict[str, object] | None = None
    checkpoint_row: dict[str, object] | None = None


class PipelineMetadataBatch:
    """Batch metadata writes while data files are written immediately."""

    def __init__(self, store: ParquetStore, flush_size: int, count_by: str) -> None:
        if count_by not in {"run", "checkpoint"}:
            raise ValueError(f"Unsupported metadata batch counter: {count_by}")
        self._store = store
        self._flush_size = max(int(flush_size), 1)
        self._count_by = count_by
        self._run_rows: list[dict[str, object]] = []
        self._status_rows: list[dict[str, object]] = []
        self._checkpoint_rows: list[dict[str, object]] = []
        self._lock = RLock()
        self._flush_write_lock = RLock()

    def add(
        self,
        run_row: dict[str, object] | None = None,
        status_row: dict[str, object] | None = None,
        checkpoint: dict[str, object] | None = None,
    ) -> None:
        should_flush = False
        with self._lock:
            if run_row is not None:
                self._run_rows.append(run_row)
            if status_row is not None:
                self._status_rows.append(status_row)
            if checkpoint is not None:
                self._checkpoint_rows.append(checkpoint)
            should_flush = self._pending_count >= self._flush_size
        if should_flush:
            self.flush()

    def flush(self) -> None:
        with self._flush_write_lock:
            with self._lock:
                if self._pending_count == 0:
                    return
                run_rows = self._run_rows
                status_rows = _dedupe_rows(self._status_rows, ("dataset", "code"))
                checkpoint_rows = _dedupe_rows(
                    self._checkpoint_rows,
                    ("pipeline", "dataset", "code", "start_date", "end_date"),
                )
                self._run_rows = []
                self._status_rows = []
                self._checkpoint_rows = []
            run_id, pid, thread = pipeline_log_values()
            start = time.perf_counter()
            logger.info(
                "Pipeline metadata flush started run_id={} pid={} thread={} run_rows={} status_rows={} "
                "checkpoint_rows={}",
                run_id,
                pid,
                thread,
                len(run_rows),
                len(status_rows),
                len(checkpoint_rows),
            )
            try:
                self._store.persist_update_metadata(run_rows, status_rows, checkpoint_rows)
                elapsed = time.perf_counter() - start
                logger.info(
                    "Pipeline metadata flush completed run_id={} pid={} thread={} elapsed={:.3f}s run_rows={} "
                    "status_rows={} checkpoint_rows={}",
                    run_id,
                    pid,
                    thread,
                    elapsed,
                    len(run_rows),
                    len(status_rows),
                    len(checkpoint_rows),
                )
            except Exception:
                with self._lock:
                    self._run_rows = [*run_rows, *self._run_rows]
                    self._status_rows = [*status_rows, *self._status_rows]
                    self._checkpoint_rows = [*checkpoint_rows, *self._checkpoint_rows]
                raise

    @property
    def _pending_count(self) -> int:
        if self._count_by == "run":
            return len(self._run_rows)
        return len(self._checkpoint_rows)


class PipelineLifecycle:
    """Create, batch, flush, and publish metadata rows for update pipelines."""

    def __init__(self, store: ParquetStore, flush_size: int, count_by: str = "run") -> None:
        self._store = store
        self._batch = PipelineMetadataBatch(store, flush_size=flush_size, count_by=count_by)

    def record_success(
        self,
        task: LifecycleTaskRef,
        *,
        started_at: datetime,
        ended_at: datetime,
        row_count: int,
        output_path: str | Path | None = None,
        last_success_date: str | None = None,
    ) -> LifecycleRows:
        rows = success_rows(
            task,
            started_at=started_at,
            ended_at=ended_at,
            row_count=row_count,
            output_path=output_path,
            last_success_date=last_success_date,
        )
        self.add(rows)
        return rows

    def record_failure(
        self,
        task: LifecycleTaskRef,
        *,
        started_at: datetime,
        ended_at: datetime,
        error_stack: str,
        output_path: str | Path | None = None,
    ) -> LifecycleRows:
        rows = failure_rows(
            task,
            started_at=started_at,
            ended_at=ended_at,
            error_stack=error_stack,
            output_path=output_path,
        )
        self.add(rows)
        return rows

    def record_skipped(
        self,
        task: LifecycleTaskRef,
        *,
        status: str,
        started_at: datetime,
        ended_at: datetime,
        reason: str = "",
    ) -> LifecycleRows:
        rows = skipped_rows(
            task,
            status=status,
            started_at=started_at,
            ended_at=ended_at,
            reason=reason,
        )
        self.add(rows)
        return rows

    def add(self, rows: LifecycleRows) -> None:
        self._batch.add(
            run_row=rows.run_row,
            status_row=rows.status_row,
            checkpoint=rows.checkpoint_row,
        )

    def flush(self) -> None:
        self._batch.flush()

    def finish(self) -> None:
        self.flush()
        refresh_dirty_registry(self._store)


def run_row(
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
    return {
        "task_id": str(uuid.uuid4()),
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


def success_rows(
    task: LifecycleTaskRef,
    *,
    started_at: datetime,
    ended_at: datetime,
    row_count: int,
    output_path: str | Path | None = None,
    last_success_date: str | None = None,
) -> LifecycleRows:
    return LifecycleRows(
        run_row=run_row(
            task.dataset,
            task.code,
            "success",
            task.start_date,
            task.end_date,
            started_at,
            ended_at,
            row_count,
            "",
        ),
        status_row=status_row(
            task.dataset,
            task.code,
            last_success_date or task.end_date,
            row_count,
            "success",
            "",
        ),
        checkpoint_row=checkpoint_row(
            task.pipeline,
            task.dataset,
            task.code,
            task.effective_checkpoint_start_date,
            task.end_date,
            "success",
            row_count,
            output_path or task.output_path,
        ),
    )


def failure_rows(
    task: LifecycleTaskRef,
    *,
    started_at: datetime,
    ended_at: datetime,
    error_stack: str,
    output_path: str | Path | None = None,
) -> LifecycleRows:
    return LifecycleRows(
        run_row=run_row(
            task.dataset,
            task.code,
            "failed",
            task.start_date,
            task.end_date,
            started_at,
            ended_at,
            0,
            error_stack,
        ),
        status_row=status_row(task.dataset, task.code, None, 0, "failed", error_stack),
        checkpoint_row=checkpoint_row(
            task.pipeline,
            task.dataset,
            task.code,
            task.effective_checkpoint_start_date,
            task.end_date,
            "failed",
            0,
            output_path or task.output_path,
            error_stack,
        ),
    )


def skipped_rows(
    task: LifecycleTaskRef,
    *,
    status: str,
    started_at: datetime,
    ended_at: datetime,
    reason: str = "",
) -> LifecycleRows:
    return LifecycleRows(
        run_row=run_row(
            task.dataset,
            task.code,
            status,
            task.start_date,
            task.end_date,
            started_at,
            ended_at,
            0,
            reason,
        ),
        status_row=status_row(task.dataset, task.code, None, 0, status, reason),
        checkpoint_row=checkpoint_row(
            task.pipeline,
            task.dataset,
            task.code,
            task.effective_checkpoint_start_date,
            task.end_date,
            status,
            0,
            task.output_path,
            reason,
        ),
    )


def status_row(
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


def _dedupe_rows(rows: list[dict[str, object]], key_fields: tuple[str, ...]) -> list[dict[str, object]]:
    if len(rows) < 2:
        return rows
    by_key: dict[tuple[object, ...], dict[str, object]] = {}
    for row in rows:
        by_key[tuple(row.get(field) for field in key_fields)] = row
    return list(by_key.values())


def refresh_dirty_registry(store: ParquetStore) -> None:
    dataset_ids = set(store.dirty_datasets())
    if not dataset_ids:
        return

    ordered_dataset_ids = sorted(dataset_ids)
    try:
        registry = DataRegistry(root=store.root)
        registry.write_catalog()
        registry.refresh_inventory(
            ordered_dataset_ids,
            status_rows=store.read_dataset_update_status(),
        )
    except Exception as exc:
        logger.warning("Failed to refresh registry after run for datasets={}: {}", ordered_dataset_ids, exc)
