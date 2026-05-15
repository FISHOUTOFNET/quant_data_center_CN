"""Derived Baostock valuation percentile pipeline."""

from __future__ import annotations

import os
import traceback
import time
from contextlib import contextmanager
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from src.analytics.valuation_percentile import compute_valuation_percentiles
from src.pipeline.adjustments import UNADJUSTED_DAILY_DATASET
from src.pipeline.common import PipelineCheckpointLookup, checkpoint_row, date_iso, should_skip_checkpoint
from src.pipeline.dry_run import apply_limit, dry_run_record
from src.pipeline.update_daily_metadata import _run_row, _status_row
from src.storage.dataset_catalog import BAOSTOCK_CN_STOCK_VALUATION_PERCENTILE_DATASET
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger


PIPELINE_UPDATE_BAOSTOCK_VALUATION_PERCENTILE = "update_baostock_valuation_percentile"
VALUATION_SOURCE_COLUMNS = ("date", "code", "pe_ttm", "pb_mrq", "ps_ttm", "pcf_ncf_ttm")


@contextmanager
def _valuation_update_lock(root: Path):
    lock_dir = root / "data" / "locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{PIPELINE_UPDATE_BAOSTOCK_VALUATION_PERCENTILE}.lock"
    lock_file = lock_path.open("a+b")
    acquired = False
    try:
        try:
            _ensure_lock_byte(lock_file)
            _lock_file(lock_file)
            acquired = True
        except OSError as exc:
            message = f"Baostock valuation percentile update is already running for root {root}"
            logger.error(message)
            raise RuntimeError(message) from exc
        lock_file.seek(0)
        lock_file.truncate()
        lock_owner = f"pid={os.getpid()} started_at={datetime.now().isoformat(timespec='seconds')}\n"
        lock_file.write(lock_owner.encode("utf-8"))
        lock_file.flush()
        yield
    finally:
        if acquired:
            try:
                _unlock_file(lock_file)
            except OSError as exc:
                logger.warning("Failed to release valuation percentile update lock {}: {}", lock_path, exc)
        lock_file.close()


def _ensure_lock_byte(lock_file) -> None:
    lock_file.seek(0)
    if lock_file.read(1):
        lock_file.seek(0)
        return
    lock_file.seek(0)
    lock_file.write(b"0")
    lock_file.flush()
    lock_file.seek(0)


def _lock_file(lock_file) -> None:
    lock_file.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        import fcntl

        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock_file(lock_file) -> None:
    lock_file.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        import fcntl

        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


@dataclass(frozen=True)
class _ValuationPercentileTask:
    root: Path
    code: str
    mode: str
    replace_start: str | None
    checkpoint_start: str
    source_end: str
    output_path: Path


@dataclass(frozen=True)
class _ValuationPercentileResult:
    task: _ValuationPercentileTask
    started_at: datetime
    ended_at: datetime
    computed: pd.DataFrame | None = None
    error_stack: str = ""
    compute_seconds: float = 0.0


def update_baostock_valuation_percentile(
    mode: str = "partial",
    code: tuple[str, ...] | list[str] | str | None = None,
    start: str | None = None,
    root: Path | None = None,
    build_views: bool = True,
    resume: bool = True,
    force: bool = False,
    workers: int | None = None,
    dry_run: bool = False,
    max_codes: int | None = None,
    max_tasks: int | None = None,
) -> list[dict[str, object]]:
    """Compute local Baostock valuation percentiles from unadjusted daily bars."""

    config = ConfigManager(root)
    if not dry_run and max_tasks is not None and int(max_tasks) == 0:
        return []
    if dry_run:
        return _update_baostock_valuation_percentile_unlocked(
            mode=mode,
            code=code,
            start=start,
            root=config.root,
            build_views=build_views,
            resume=resume,
            force=force,
            workers=workers,
            dry_run=True,
            max_codes=max_codes,
            max_tasks=max_tasks,
        )
    with _valuation_update_lock(config.root):
        return _update_baostock_valuation_percentile_unlocked(
            mode=mode,
            code=code,
            start=start,
            root=config.root,
            build_views=build_views,
            resume=resume,
            force=force,
            workers=workers,
            dry_run=False,
            max_codes=max_codes,
            max_tasks=max_tasks,
        )


def _update_baostock_valuation_percentile_unlocked(
    mode: str = "partial",
    code: tuple[str, ...] | list[str] | str | None = None,
    start: str | None = None,
    root: Path | None = None,
    build_views: bool = True,
    resume: bool = True,
    force: bool = False,
    workers: int | None = None,
    dry_run: bool = False,
    max_codes: int | None = None,
    max_tasks: int | None = None,
) -> list[dict[str, object]]:
    if mode not in {"partial", "full"}:
        raise ValueError(f"Unsupported valuation percentile mode: {mode}")

    config = ConfigManager(root)
    store = ParquetStore(root=config.root)
    if not dry_run:
        store.ensure_layout()
    dataset = BAOSTOCK_CN_STOCK_VALUATION_PERCENTILE_DATASET.name
    run_records: list[dict[str, object]] = []
    status_rows: list[dict[str, object]] = []
    checkpoint_rows: list[dict[str, object]] = []
    success_count = 0
    timing_totals = {
        "compute_seconds": 0.0,
        "parquet_write_seconds": 0.0,
        "metadata_persist_seconds": 0.0,
    }

    codes = apply_limit(_resolve_source_codes(store, code), max_codes, "max_codes")
    resolved_workers = _resolve_workers(config, workers)
    logger.info(
        "Baostock valuation percentile update started mode={} force={} resume={} workers={} codes={}",
        mode,
        force,
        resume,
        resolved_workers,
        len(codes),
    )

    tasks: list[_ValuationPercentileTask] = []
    latest_prefilter_enabled = resume and not force and start is None
    latest_prefilter_total = 0
    latest_prefilter_skipped = 0
    checkpoint_lookup: PipelineCheckpointLookup | None = None

    def get_checkpoint_lookup() -> PipelineCheckpointLookup | None:
        nonlocal checkpoint_lookup
        if not resume or force:
            return None
        if checkpoint_lookup is None:
            checkpoint_lookup = _filtered_checkpoint_lookup(store)
        return checkpoint_lookup

    for stock_code in codes:
        output_path = store.baostock_cn_stock_valuation_percentile_path(stock_code)
        source_bounds = _source_date_bounds(store, stock_code)
        if source_bounds is None:
            continue
        source_start, source_end = source_bounds
        if latest_prefilter_enabled:
            latest_prefilter_total += 1
            if _target_covers_source_latest(store, stock_code, source_end):
                latest_prefilter_skipped += 1
                continue

        replace_start = _replace_start(store, stock_code, mode, start)
        checkpoint_start = replace_start or source_start

        if not dry_run and _should_skip(
            store,
            dataset,
            stock_code,
            checkpoint_start,
            source_end,
            output_path,
            resume,
            force,
            get_checkpoint_lookup(),
        ):
            continue

        tasks.append(
            _ValuationPercentileTask(
                root=config.root,
                code=stock_code,
                mode=mode,
                replace_start=replace_start,
                checkpoint_start=checkpoint_start,
                source_end=source_end,
                output_path=output_path,
            )
        )

    tasks = apply_limit(tasks, max_tasks, "max_tasks")

    if dry_run:
        return [
            dry_run_record(
                dataset,
                task.code,
                task.checkpoint_start,
                task.source_end,
                task.output_path,
                operation="write_baostock_cn_stock_valuation_percentile",
                mode=task.mode,
            )
            for task in tasks
        ]

    if latest_prefilter_skipped:
        skipped_ratio = latest_prefilter_skipped / latest_prefilter_total * 100 if latest_prefilter_total else 0.0
        logger.info(
            "Baostock valuation percentile prefilter skipped {}/{} codes ({:.1f}%); processing {} codes",
            latest_prefilter_skipped,
            latest_prefilter_total,
            skipped_ratio,
            len(tasks),
        )

    progress_total = len(tasks)
    progress_processed = 0
    progress_success = 0
    progress_failed = 0

    def log_progress(row: dict[str, object]) -> None:
        nonlocal progress_processed, progress_success, progress_failed
        progress_processed += 1
        status = str(row.get("status", "unknown"))
        if status == "success":
            progress_success += 1
        elif status == "failed":
            progress_failed += 1
        logger.info(
            "Baostock valuation percentile progress {}/{} code={} status={} rows={}",
            progress_processed,
            progress_total,
            row.get("code", ""),
            status,
            row.get("row_count", 0),
        )

    def record_result(result: _ValuationPercentileResult) -> None:
        nonlocal success_count
        run_record_count = len(run_records)
        if _record_valuation_result(
            store,
            result,
            dataset,
            run_records,
            status_rows,
            checkpoint_rows,
            timing_totals,
        ):
            success_count += 1
        progress_row = (
            run_records[-1]
            if len(run_records) > run_record_count
            else {
                "dataset": dataset,
                "code": result.task.code,
                "status": "skipped_empty",
                "row_count": 0,
            }
        )
        log_progress(progress_row)

    if resolved_workers == 1 or len(tasks) <= 1:
        for task in tasks:
            record_result(_compute_valuation_percentile_task(task))
    else:
        with ProcessPoolExecutor(max_workers=resolved_workers) as executor:
            futures = {executor.submit(_compute_valuation_percentile_task, task): task for task in tasks}
            for future in as_completed(futures):
                task = futures[future]
                try:
                    result = future.result()
                except Exception:
                    result = _ValuationPercentileResult(
                        task=task,
                        started_at=datetime.now(),
                        ended_at=datetime.now(),
                        error_stack=traceback.format_exc(),
                    )
                record_result(result)

    if run_records or status_rows or checkpoint_rows:
        metadata_started = time.perf_counter()
        store.persist_update_metadata(run_records, status_rows, checkpoint_rows)
        timing_totals["metadata_persist_seconds"] += time.perf_counter() - metadata_started
    store.close()

    if build_views:
        DuckDBStore(root=config.root).build_views(cleanup_tmp_files=success_count > 0)
    logger.info(
        (
            "Baostock valuation percentile timing summary compute={:.3f}s "
            "parquet_write={:.3f}s metadata_persist={:.3f}s"
        ),
        timing_totals["compute_seconds"],
        timing_totals["parquet_write_seconds"],
        timing_totals["metadata_persist_seconds"],
    )
    logger.info(
        "Baostock valuation percentile update completed records={} success={} failed={}",
        len(run_records),
        success_count,
        progress_failed,
    )
    return run_records


def _compute_valuation_percentile_task(task: _ValuationPercentileTask) -> _ValuationPercentileResult:
    started_at = datetime.now()
    compute_started = time.perf_counter()
    store = ParquetStore(root=task.root)
    try:
        source = store.read_baostock_daily_bars(
            UNADJUSTED_DAILY_DATASET,
            task.code,
            columns=VALUATION_SOURCE_COLUMNS,
        )
        if source.empty:
            raise ValueError(f"Source daily bars are missing for {task.code}")
        computed = compute_valuation_percentiles(source, start=task.replace_start)
        return _ValuationPercentileResult(
            task=task,
            started_at=started_at,
            ended_at=datetime.now(),
            computed=computed,
            compute_seconds=time.perf_counter() - compute_started,
        )
    except Exception:
        return _ValuationPercentileResult(
            task=task,
            started_at=started_at,
            ended_at=datetime.now(),
            error_stack=traceback.format_exc(),
            compute_seconds=time.perf_counter() - compute_started,
        )
    finally:
        store.close()


def _record_valuation_result(
    store: ParquetStore,
    result: _ValuationPercentileResult,
    dataset: str,
    run_records: list[dict[str, object]],
    status_rows: list[dict[str, object]],
    checkpoint_rows: list[dict[str, object]],
    timing_totals: dict[str, float],
) -> bool:
    task = result.task
    timing_totals["compute_seconds"] += result.compute_seconds
    if result.error_stack or result.computed is None:
        _append_failed_metadata(
            dataset,
            task,
            result.started_at,
            result.ended_at,
            result.error_stack,
            run_records,
            status_rows,
            checkpoint_rows,
        )
        return False

    try:
        if task.mode == "partial" and task.replace_start is not None:
            existing = store.read_baostock_cn_stock_valuation_percentile(task.code)
            final = _replace_from_start(existing, result.computed, task.replace_start)
        else:
            final = result.computed

        if final.empty:
            return False

        write_timing: dict[str, float] = {}
        path = store.write_baostock_cn_stock_valuation_percentile(
            task.code,
            final,
            refresh_registry_inventory=False,
            timing=write_timing,
        )
        timing_totals["parquet_write_seconds"] += write_timing.get("parquet_write_seconds", 0.0)
        row_count = len(final)
        run_row = _run_row(
            dataset,
            task.code,
            "success",
            task.checkpoint_start,
            task.source_end,
            result.started_at,
            datetime.now(),
            row_count,
            "",
        )
        status_row = _status_row(dataset, task.code, task.source_end, row_count, "success", "")
        checkpoint = checkpoint_row(
            PIPELINE_UPDATE_BAOSTOCK_VALUATION_PERCENTILE,
            dataset,
            task.code,
            task.checkpoint_start,
            task.source_end,
            "success",
            row_count,
            path,
        )
        run_records.append(run_row)
        status_rows.append(status_row)
        checkpoint_rows.append(checkpoint)
        return True
    except Exception:
        _append_failed_metadata(
            dataset,
            task,
            result.started_at,
            datetime.now(),
            traceback.format_exc(),
            run_records,
            status_rows,
            checkpoint_rows,
        )
        return False


def _append_failed_metadata(
    dataset: str,
    task: _ValuationPercentileTask,
    started_at: datetime,
    ended_at: datetime,
    error_stack: str,
    run_records: list[dict[str, object]],
    status_rows: list[dict[str, object]],
    checkpoint_rows: list[dict[str, object]],
) -> None:
    logger.error("Baostock valuation percentile failed for {}", task.code)
    run_row = _run_row(
        dataset,
        task.code,
        "failed",
        task.checkpoint_start,
        task.source_end,
        started_at,
        ended_at,
        0,
        error_stack,
    )
    status_row = _status_row(dataset, task.code, None, 0, "failed", error_stack)
    checkpoint = checkpoint_row(
        PIPELINE_UPDATE_BAOSTOCK_VALUATION_PERCENTILE,
        dataset,
        task.code,
        task.checkpoint_start,
        task.source_end,
        "failed",
        0,
        task.output_path,
        error_stack,
    )
    run_records.append(run_row)
    status_rows.append(status_row)
    checkpoint_rows.append(checkpoint)


def _resolve_source_codes(store: ParquetStore, code: tuple[str, ...] | list[str] | str | None) -> list[str]:
    if isinstance(code, str):
        return [code]
    if code:
        return [str(item) for item in code]
    dataset_dir = store.parquet_dir / UNADJUSTED_DAILY_DATASET
    if not dataset_dir.exists():
        return []
    prefix = "code="
    return sorted(
        item.name[len(prefix):]
        for item in dataset_dir.iterdir()
        if item.is_dir() and item.name.startswith(prefix) and (item / "data.parquet").exists()
    )


def _source_date_bounds(store: ParquetStore, code: str) -> tuple[str, str] | None:
    path = store.baostock_daily_bar_path(UNADJUSTED_DAILY_DATASET, code)
    if not path.exists():
        logger.warning("Skip valuation percentile for {} because source daily bars are missing", code)
        return None
    source_bounds = _parquet_date_bounds(path)
    if source_bounds is None:
        source_bounds = _date_column_bounds(store, path)
    if source_bounds is None:
        logger.warning("Skip valuation percentile for {} because source daily bar dates are invalid", code)
        return None
    return source_bounds


def _target_covers_source_latest(store: ParquetStore, code: str, source_end: str) -> bool:
    target_bounds = _target_date_bounds(store, code)
    if target_bounds is None:
        return False
    return target_bounds[1] >= source_end


def _target_date_bounds(store: ParquetStore, code: str) -> tuple[str, str] | None:
    path = store.baostock_cn_stock_valuation_percentile_path(code)
    if not path.exists():
        return None
    bounds = _parquet_date_bounds(path)
    if bounds is not None:
        return bounds
    try:
        return _date_column_bounds(store, path)
    except Exception as exc:
        logger.warning("Cannot inspect valuation percentile date bounds for {}: {}", code, exc)
        return None


def _parquet_date_bounds(path: Path) -> tuple[str, str] | None:
    try:
        metadata = pq.read_metadata(path)
    except Exception as exc:
        logger.warning("Cannot inspect parquet metadata date bounds for {}: {}", path, exc)
        return None
    column_index = _metadata_column_index(metadata, "date")
    if column_index is None:
        return None
    min_date: str | None = None
    max_date: str | None = None
    for row_group_index in range(metadata.num_row_groups):
        stats = metadata.row_group(row_group_index).column(column_index).statistics
        if stats is None or stats.min is None or stats.max is None:
            return None
        stats_min = _date_bound_value(stats.min)
        stats_max = _date_bound_value(stats.max)
        if stats_min is None or stats_max is None:
            return None
        min_date = stats_min if min_date is None else min(min_date, stats_min)
        max_date = stats_max if max_date is None else max(max_date, stats_max)
    if min_date is None or max_date is None:
        return None
    return min_date, max_date


def _metadata_column_index(metadata: pq.FileMetaData, column_name: str) -> int | None:
    for index in range(metadata.num_columns):
        if metadata.schema.column(index).name == column_name:
            return index
    return None


def _date_bound_value(value: object) -> str | None:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date().isoformat()


def _date_column_bounds(store: ParquetStore, path: Path) -> tuple[str, str] | None:
    dates = pd.to_datetime(store._safe_read_parquet(path, columns=["date"])["date"], errors="coerce").dropna()
    if dates.empty:
        return None
    return dates.min().date().isoformat(), dates.max().date().isoformat()


def _resolve_workers(config: ConfigManager, workers: int | None) -> int:
    raw_workers = workers if workers is not None else config.get("pipeline.baostock_valuation_percentile_workers", 4)
    try:
        return max(int(raw_workers), 1)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid Baostock valuation percentile workers value: {raw_workers!r}") from exc


def _filtered_checkpoint_lookup(store: ParquetStore) -> PipelineCheckpointLookup:
    checkpoints = store.read_pipeline_checkpoints()
    if checkpoints.empty:
        return PipelineCheckpointLookup(checkpoints)
    filtered = checkpoints.loc[
        (checkpoints["pipeline"].astype("string") == PIPELINE_UPDATE_BAOSTOCK_VALUATION_PERCENTILE)
        & (
            checkpoints["dataset"].astype("string")
            == BAOSTOCK_CN_STOCK_VALUATION_PERCENTILE_DATASET.name
        )
    ]
    return PipelineCheckpointLookup(filtered)


def _replace_start(store: ParquetStore, code: str, mode: str, start: str | None) -> str | None:
    if mode == "full":
        return None
    if start is not None:
        return date_iso(start)
    target_bounds = _target_date_bounds(store, code)
    if target_bounds is None:
        return None
    return (pd.Timestamp(target_bounds[1]).date() + timedelta(days=1)).isoformat()


def _replace_from_start(existing: pd.DataFrame, fresh: pd.DataFrame, start: str) -> pd.DataFrame:
    if existing.empty:
        return fresh.reset_index(drop=True)
    dates = pd.to_datetime(existing["date"], errors="coerce")
    before = existing.loc[dates < pd.Timestamp(start)].copy()
    combined = pd.concat([before, fresh], ignore_index=True)
    if combined.empty:
        return combined
    combined["_date_key"] = pd.to_datetime(combined["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return (
        combined.drop_duplicates(["code", "_date_key"], keep="last")
        .drop(columns=["_date_key"])
        .sort_values(["code", "date"])
        .reset_index(drop=True)
    )


def _should_skip(
    store: ParquetStore,
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    output_path: Path,
    resume: bool,
    force: bool,
    checkpoint_lookup: PipelineCheckpointLookup | None = None,
) -> bool:
    return should_skip_checkpoint(
        store,
        PIPELINE_UPDATE_BAOSTOCK_VALUATION_PERCENTILE,
        dataset,
        code,
        start_date,
        end_date,
        output_path,
        resume,
        force,
        checkpoint_lookup=checkpoint_lookup,
    )
