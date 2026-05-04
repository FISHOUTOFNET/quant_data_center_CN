"""Manual AkShare stock_zh_a_hist full and incremental pipeline."""

from __future__ import annotations

import traceback
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.api.akshare_client import AkShareClient, AkShareResponse
from src.pipeline.akshare_common import (
    PIPELINE_UPDATE_AKSHARE_HIST,
    append_failed_manifest,
    append_response_manifest,
    error_stack,
    error_type,
    failed_metadata,
    persist_metadata,
    success_metadata,
    write_raw_response,
)
from src.pipeline.akshare_universe import resolve_akshare_universe_codes
from src.pipeline.common import default_candidate_date, should_skip_checkpoint
from src.storage.dataset_catalog import stock_zh_a_hist_adjusts, stock_zh_a_hist_dataset_name
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger


@dataclass(frozen=True)
class AkShareHistTask:
    code: str
    adjust: str
    dataset: str
    start_date: str
    end_date: str
    output_path: Path


@dataclass(frozen=True)
class _HistFetchResult:
    task: AkShareHistTask
    started_at: datetime
    ended_at: datetime
    response: AkShareResponse | None = None
    error: Exception | None = None
    error_stack: str = ""


def update_akshare_hist(
    mode: str,
    adjust: str = "all",
    code: tuple[str, ...] | list[str] | str | None = None,
    start: str | date | None = None,
    end: str | date | None = None,
    max_tasks: int | None = None,
    workers: int | None = None,
    root: Path | None = None,
    resume: bool = True,
    force: bool = False,
    build_views: bool = True,
    client: Any | None = None,
    client_factory: Callable[[ConfigManager, pd.DataFrame], Any] | None = None,
) -> list[dict[str, object]]:
    """Run full initialization or manual incremental repair for stock_zh_a_hist."""

    mode = str(mode).strip().lower()
    config = ConfigManager(root)
    store = ParquetStore(root=config.root)
    store.ensure_layout()
    tasks = plan_akshare_hist_tasks(
        config=config,
        store=store,
        mode=mode,
        adjust=adjust,
        code=code,
        start=start,
        end=end,
        max_tasks=max_tasks,
    )
    stock_basic_df = store.read_stock_basic()
    ak_client = client or (
        client_factory(config, stock_basic_df)
        if client_factory is not None
        else AkShareClient(config=config, stock_basic_df=stock_basic_df)
    )
    selected_tasks = [
        task
        for task in tasks
        if not should_skip_checkpoint(
            store,
            PIPELINE_UPDATE_AKSHARE_HIST,
            task.dataset,
            task.code,
            task.start_date,
            task.end_date,
            task.output_path,
            resume,
            force,
        )
    ]
    metadata: list[tuple[dict[str, object], dict[str, object], dict[str, object]]] = []
    records: list[dict[str, object]] = []

    resolved_workers = _resolve_workers(config, workers)
    if resolved_workers == 1:
        for task in selected_tasks:
            result = _fetch_hist_task(ak_client, task)
            row = _record_hist_result(store, result, mode, ak_client, metadata)
            if row is not None:
                records.append(row)
    else:
        with ThreadPoolExecutor(max_workers=resolved_workers, thread_name_prefix="update-akshare-hist") as executor:
            futures: dict[Future[_HistFetchResult], AkShareHistTask] = {
                executor.submit(_fetch_hist_task, ak_client, task): task for task in selected_tasks
            }
            for future in as_completed(futures):
                try:
                    result = future.result()
                except Exception as exc:
                    task = futures[future]
                    result = _HistFetchResult(
                        task=task,
                        started_at=datetime.now(),
                        ended_at=datetime.now(),
                        error=exc,
                        error_stack=traceback.format_exc(),
                    )
                row = _record_hist_result(store, result, mode, ak_client, metadata)
                if row is not None:
                    records.append(row)

    persisted_records = persist_metadata(store, metadata)
    store.close()
    if build_views:
        DuckDBStore(root=config.root).build_views()
    return persisted_records if persisted_records else records


def plan_akshare_hist_tasks(
    config: ConfigManager,
    store: ParquetStore,
    mode: str,
    adjust: str = "all",
    code: tuple[str, ...] | list[str] | str | None = None,
    start: str | date | None = None,
    end: str | date | None = None,
    max_tasks: int | None = None,
) -> list[AkShareHistTask]:
    normalized_mode = str(mode).strip().lower()
    if normalized_mode not in {"full", "incremental"}:
        raise ValueError(f"Unsupported stock_zh_a_hist mode: {mode}")
    end_date = _date_iso(end, default_candidate_date(config))
    if normalized_mode == "full":
        start_date = _date_iso(start, str(config.get("datasets.stock_zh_a_hist.full_start", "1990-01-01")))
    else:
        if start is None:
            raise ValueError("stock_zh_a_hist incremental mode requires --start")
        start_date = _date_iso(start, end_date)
    if start_date > end_date:
        raise ValueError(f"stock_zh_a_hist start date {start_date} is after end date {end_date}")

    codes = _resolve_hist_codes(store, code, include_delisted=normalized_mode == "full")
    adjusts = _resolve_adjusts(adjust)
    tasks = [
        AkShareHistTask(
            code=stock_code,
            adjust=hist_adjust,
            dataset=stock_zh_a_hist_dataset_name(hist_adjust),
            start_date=start_date,
            end_date=end_date,
            output_path=store.stock_zh_a_hist_path(hist_adjust, stock_code),
        )
        for hist_adjust in adjusts
        for stock_code in codes
    ]
    if max_tasks is not None:
        tasks = tasks[: max(int(max_tasks), 0)]
    return tasks


def _fetch_hist_task(client: Any, task: AkShareHistTask) -> _HistFetchResult:
    started_at = datetime.now()
    try:
        response = client.fetch_stock_zh_a_hist(
            symbol=task.code,
            start_date=task.start_date,
            end_date=task.end_date,
            adjust=task.adjust,
        )
        return _HistFetchResult(task=task, started_at=started_at, ended_at=datetime.now(), response=response)
    except Exception as exc:
        return _HistFetchResult(
            task=task,
            started_at=started_at,
            ended_at=datetime.now(),
            error=exc,
            error_stack=error_stack(exc),
        )


def _record_hist_result(
    store: ParquetStore,
    result: _HistFetchResult,
    mode: str,
    client: Any,
    metadata: list[tuple[dict[str, object], dict[str, object], dict[str, object]]],
) -> dict[str, object] | None:
    task = result.task
    if result.error is not None:
        append_failed_manifest(
            store,
            PIPELINE_UPDATE_AKSHARE_HIST,
            task.dataset,
            "stock_zh_a_hist",
            task.code,
            {
                "symbol": task.code,
                "start_date": task.start_date,
                "end_date": task.end_date,
                "adjust": task.adjust,
            },
            client,
            error_type(result.error),
            str(result.error),
            result.started_at,
            result.ended_at,
        )
        metadata.append(
            failed_metadata(
                PIPELINE_UPDATE_AKSHARE_HIST,
                task.dataset,
                task.code,
                task.start_date,
                task.end_date,
                result.started_at,
                result.ended_at,
                result.error_stack,
                task.output_path,
            )
        )
        logger.error("AkShare hist task failed code={} adjust={}: {}", task.code, task.adjust, result.error)
        return None

    response = result.response
    if response is None:
        return _record_hist_result(
            store,
            _HistFetchResult(
                task=task,
                started_at=result.started_at,
                ended_at=datetime.now(),
                error=RuntimeError("stock_zh_a_hist returned no response"),
                error_stack="stock_zh_a_hist returned no response",
            ),
            mode,
            client,
            metadata,
        )

    try:
        raw_path = write_raw_response(store.root, response, result.started_at)
        if mode == "full":
            output_path = store.write_stock_zh_a_hist(task.adjust, task.code, response.data)
        else:
            output_path = store.upsert_stock_zh_a_hist(task.adjust, task.code, response.data)
        ended_at = datetime.now()
        append_response_manifest(
            store,
            PIPELINE_UPDATE_AKSHARE_HIST,
            task.dataset,
            task.code,
            response,
            raw_path,
            "success",
            "",
            "",
            result.started_at,
            ended_at,
        )
        row_metadata = success_metadata(
            PIPELINE_UPDATE_AKSHARE_HIST,
            task.dataset,
            task.code,
            task.start_date,
            task.end_date,
            result.started_at,
            ended_at,
            len(response.data),
            output_path,
        )
        metadata.append(row_metadata)
        return row_metadata[0]
    except Exception as exc:
        ended_at = datetime.now()
        stack = error_stack(exc)
        append_failed_manifest(
            store,
            PIPELINE_UPDATE_AKSHARE_HIST,
            task.dataset,
            "stock_zh_a_hist",
            task.code,
            response.params,
            client,
            error_type(exc),
            str(exc),
            result.started_at,
            ended_at,
        )
        metadata.append(
            failed_metadata(
                PIPELINE_UPDATE_AKSHARE_HIST,
                task.dataset,
                task.code,
                task.start_date,
                task.end_date,
                result.started_at,
                ended_at,
                stack,
                task.output_path,
            )
        )
        logger.exception("AkShare hist write failed code={} adjust={}", task.code, task.adjust)
        return None


def _resolve_hist_codes(
    store: ParquetStore,
    code: tuple[str, ...] | list[str] | str | None,
    include_delisted: bool,
) -> list[str]:
    return resolve_akshare_universe_codes(
        store,
        code=code,
        include_delisted=include_delisted,
        context="stock_zh_a_hist",
    )


def _resolve_adjusts(adjust: str) -> list[str]:
    normalized = str(adjust).strip().lower()
    if normalized == "all":
        return list(stock_zh_a_hist_adjusts())
    if normalized in stock_zh_a_hist_adjusts():
        return [normalized]
    raise ValueError(f"Unsupported stock_zh_a_hist adjust: {adjust}")


def _resolve_workers(config: ConfigManager, workers: int | None) -> int:
    raw_workers = workers if workers is not None else config.get("api.akshare.workers", 3)
    try:
        return max(int(raw_workers), 1)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid AkShare hist workers value: {raw_workers!r}") from exc


def _date_iso(value: str | date | None, default: str) -> str:
    if value is None:
        return default
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return pd.to_datetime(value, errors="raise").date().isoformat()
