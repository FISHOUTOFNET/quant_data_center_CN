"""Initial historical load pipeline."""

from __future__ import annotations

import traceback
from pathlib import Path

import pandas as pd

from src.api.market_data import create_provider
from src.pipeline.common import (
    DAILY_K_DATASETS,
    FULL_HISTORY_START_DATE,
    PIPELINE_INIT_HISTORY,
    PipelineCheckpointLookup,
    checkpoint_output_path,
    checkpoint_row,
    date_iso,
    default_candidate_date,
    expand_daily_datasets,
    resolve_codes,
    should_skip_checkpoint,
    trading_range_bounds,
    write_checkpoint,
)
from src.pipeline.services import (
    PipelineMetadataBatch,
    ensure_calendar_range,
    fetch_daily_k,
    fetch_stock_basic,
    log_api_fetch,
    path_result,
)
from src.pipeline.write_queue import PipelineWriteQueue
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger


def init_history(
    dataset: str = "all",
    start: str = FULL_HISTORY_START_DATE,
    end: str | None = None,
    code: tuple[str, ...] | list[str] | str | None = None,
    universe: str | None = None,
    root: Path | None = None,
    build_views: bool = True,
    resume: bool = True,
    force: bool = False,
    provider: str | None = None,
) -> list[dict[str, object]]:
    """Initialize a dataset for one code or a configured universe."""

    config = ConfigManager(root)
    store = ParquetStore(root=config.root)
    store.ensure_layout()

    start_candidate_date = date_iso(start)
    end_candidate_date = date_iso(end) if end is not None else default_candidate_date(config)
    results: list[dict[str, object]] = []

    daily_targets = _daily_targets(dataset)
    with create_provider(config, provider) as data_provider:
        checkpoint_lookup = PipelineCheckpointLookup.from_store(store) if resume and not force else None
        calendar_df, ensured_calendar_df = ensure_calendar_range(
            store,
            data_provider,
            start_candidate_date,
            end_candidate_date,
        )
        start_date, end_date = trading_range_bounds(calendar_df, start_candidate_date, end_candidate_date)

        if dataset in {"all", "calendar"}:
            path = checkpoint_output_path(store, "calendar", "*", end_date)
            if should_skip_checkpoint(
                store,
                PIPELINE_INIT_HISTORY,
                "calendar",
                "*",
                start_date,
                end_date,
                path,
                resume,
                force,
                checkpoint_lookup,
            ):
                results.append(path_result("calendar", "*", 0, path, "skipped"))
            elif ensured_calendar_df is not None and not force:
                calendar_rows = ensured_calendar_df
                path = store.calendar_path()
                calendar_df = store.read_calendar()
                start_date, end_date = trading_range_bounds(calendar_df, start_candidate_date, end_candidate_date)
                write_checkpoint(
                    store,
                    PIPELINE_INIT_HISTORY,
                    "calendar",
                    "*",
                    start_date,
                    end_date,
                    "success",
                    len(calendar_rows),
                    path,
                )
                results.append(path_result("calendar", "*", len(calendar_rows), path, "success"))
            else:
                calendar_rows = data_provider.query_trade_dates()
                log_api_fetch("calendar", "*", FULL_HISTORY_START_DATE, "latest", calendar_rows)
                path = store.write_calendar(calendar_rows)
                calendar_df = store.read_calendar()
                start_date, end_date = trading_range_bounds(calendar_df, start_candidate_date, end_candidate_date)
                write_checkpoint(
                    store,
                    PIPELINE_INIT_HISTORY,
                    "calendar",
                    "*",
                    start_date,
                    end_date,
                    "success",
                    len(calendar_rows),
                    path,
                )
                results.append(path_result("calendar", "*", len(calendar_rows), path, "success"))

        if dataset in {"all", "stock_basic"}:
            path = checkpoint_output_path(store, "stock_basic", "*", end_date)
            if should_skip_checkpoint(
                store,
                PIPELINE_INIT_HISTORY,
                "stock_basic",
                "*",
                start_date,
                end_date,
                path,
                resume,
                force,
                checkpoint_lookup,
            ):
                results.append(path_result("stock_basic", "*", 0, path, "skipped"))
            else:
                basic_df = fetch_stock_basic(data_provider)
                log_api_fetch("stock_basic", "*", start_date, end_date, basic_df)
                path = store.write_stock_basic(basic_df)
                write_checkpoint(
                    store,
                    PIPELINE_INIT_HISTORY,
                    "stock_basic",
                    "*",
                    start_date,
                    end_date,
                    "success",
                    len(basic_df),
                    path,
                )
                results.append(path_result("stock_basic", "*", len(basic_df), path, "success"))

        if daily_targets and not code and not universe and not store.stock_basic_path().exists():
            basic_df = fetch_stock_basic(data_provider)
            log_api_fetch("stock_basic", "*", start_date, end_date, basic_df)
            path = store.write_stock_basic(basic_df)
            write_checkpoint(
                store,
                PIPELINE_INIT_HISTORY,
                "stock_basic",
                "*",
                start_date,
                end_date,
                "success",
                len(basic_df),
                path,
            )
            results.append(path_result("stock_basic", "*", len(basic_df), path, "success"))

        codes = (
            resolve_codes(config, store, code, universe, stock_basic_mode="all")
            if daily_targets
            else []
        )

        if daily_targets:
            write_queue = PipelineWriteQueue()
            checkpoint_batch = PipelineMetadataBatch(
                store,
                int(config.get("pipeline.metadata_flush_size", 200)),
                count_by="checkpoint",
            )
            try:
                for target_dataset in daily_targets:
                    for stock_code in codes:
                        path = checkpoint_output_path(store, target_dataset, stock_code, end_date)
                        if should_skip_checkpoint(
                            store,
                            PIPELINE_INIT_HISTORY,
                            target_dataset,
                            stock_code,
                            FULL_HISTORY_START_DATE,
                            end_date,
                            path,
                            resume,
                            force,
                            checkpoint_lookup,
                        ):
                            logger.info("Skipped initialized {} {} by checkpoint", target_dataset, stock_code)
                            skipped_result = {
                                "dataset": target_dataset,
                                "code": stock_code,
                                "rows": 0,
                                "path": str(path),
                                "status": "skipped",
                            }
                            write_queue.submit(
                                lambda skipped_result=skipped_result: skipped_result,
                                description=f"skip {target_dataset} {stock_code}",
                            )
                            continue

                        try:
                            df = fetch_daily_k(
                                data_provider,
                                config,
                                target_dataset,
                                stock_code,
                                FULL_HISTORY_START_DATE,
                                end_date,
                            )
                            log_api_fetch(target_dataset, stock_code, FULL_HISTORY_START_DATE, end_date, df)
                            write_queue.submit(
                                lambda target_dataset=target_dataset, stock_code=stock_code, df=df: _write_daily_success(
                                    store,
                                    checkpoint_batch,
                                    target_dataset,
                                    stock_code,
                                    FULL_HISTORY_START_DATE,
                                    end_date,
                                    df,
                                ),
                                on_error=lambda error_stack,
                                target_dataset=target_dataset,
                                stock_code=stock_code,
                                path=path: _write_daily_failure(
                                    store,
                                    checkpoint_batch,
                                    target_dataset,
                                    stock_code,
                                    FULL_HISTORY_START_DATE,
                                    end_date,
                                    path,
                                    error_stack,
                                ),
                                description=f"initialize {target_dataset} {stock_code}",
                            )
                        except Exception:
                            error_stack = traceback.format_exc()
                            logger.exception("Initial history load failed for {} {}", target_dataset, stock_code)
                            write_queue.submit(
                                lambda target_dataset=target_dataset,
                                stock_code=stock_code,
                                path=path,
                                error_stack=error_stack: _write_daily_failure(
                                    store,
                                    checkpoint_batch,
                                    target_dataset,
                                    stock_code,
                                    FULL_HISTORY_START_DATE,
                                    end_date,
                                    path,
                                    error_stack,
                                ),
                                description=f"checkpoint failed {target_dataset} {stock_code}",
                            )
            finally:
                write_queue.submit(lambda: checkpoint_batch.flush(), description="flush init_history checkpoints")
                results.extend(write_queue.close())

    if build_views:
        DuckDBStore(root=config.root).build_views()
    return results


def _write_daily_success(
    store: ParquetStore,
    checkpoint_batch: PipelineMetadataBatch | None,
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    df: pd.DataFrame,
) -> dict[str, object]:
    path = store.write_daily_k(dataset, code, df)
    checkpoint = checkpoint_row(
        PIPELINE_INIT_HISTORY,
        dataset,
        code,
        start_date,
        end_date,
        "success",
        len(df),
        path,
    )
    if checkpoint_batch is None:
        write_checkpoint(
            store,
            PIPELINE_INIT_HISTORY,
            dataset,
            code,
            start_date,
            end_date,
            "success",
            len(df),
            path,
        )
    else:
        checkpoint_batch.add(checkpoint=checkpoint)
    logger.info(
        "Initialized {} {} from {} to {} rows={}",
        dataset,
        code,
        start_date,
        end_date,
        len(df),
    )
    return {"dataset": dataset, "code": code, "rows": len(df), "path": str(path), "status": "success"}


def _write_daily_failure(
    store: ParquetStore,
    checkpoint_batch: PipelineMetadataBatch | None,
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    path: Path,
    error_stack: str,
) -> dict[str, object]:
    try:
        checkpoint = checkpoint_row(
            PIPELINE_INIT_HISTORY,
            dataset,
            code,
            start_date,
            end_date,
            "failed",
            0,
            path,
            error_stack,
        )
        if checkpoint_batch is None:
            write_checkpoint(
                store,
                PIPELINE_INIT_HISTORY,
                dataset,
                code,
                start_date,
                end_date,
                "failed",
                0,
                path,
                error_stack,
            )
        else:
            checkpoint_batch.add(checkpoint=checkpoint)
    except Exception:
        logger.exception("Failed to write failed checkpoint for {} {}", dataset, code)
    return {
        "dataset": dataset,
        "code": code,
        "rows": 0,
        "path": str(path),
        "status": "failed",
        "error_stack": error_stack,
    }


def _daily_targets(dataset: str) -> list[str]:
    if dataset in {"all", "daily_k_all", "daily_k"}:
        return list(DAILY_K_DATASETS)
    if dataset in DAILY_K_DATASETS:
        return expand_daily_datasets(dataset)
    if dataset in {"stock_basic", "calendar"}:
        return []
    raise ValueError(f"Unsupported init dataset: {dataset}")
