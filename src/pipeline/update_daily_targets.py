"""Target and checkpoint helpers for the daily update pipeline."""

from __future__ import annotations

from datetime import datetime

from src.api.market_data import MarketDataProvider
from src.pipeline.adjustments import BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET
from src.pipeline.common import (
    DAILY_BAR_DATASETS,
    FULL_HISTORY_START_DATE,
    PIPELINE_UPDATE_DAILY,
    PipelineCheckpointLookup,
    checkpoint_output_path,
    expand_daily_datasets,
    should_skip_checkpoint,
    write_checkpoint,
)
from src.pipeline.services import fetch_baostock_cn_stock_basic, log_api_fetch
from src.pipeline.update_daily_metadata import (
    _add_skipped_run,
    _add_success_run,
    _persist_run_status,
    _status_row,
)
from src.pipeline.update_daily_types import DailyTargetPlan
from src.storage.parquet_store import ParquetStore
from src.utils.logging import logger


def _daily_target_plans(
    store: ParquetStore,
    daily_targets: list[str],
    code: str,
    mode: str,
    start_date: str,
    end_date: str,
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
            )
        )
    return plans


def _prefilter_checkpointed_codes(
    store: ParquetStore,
    codes: list[str],
    daily_targets: list[str],
    needs_baostock_cn_stock_adjustment_factor_api: bool,
    mode: str,
    start_date: str,
    end_date: str,
    checkpoint_lookup: PipelineCheckpointLookup | None,
) -> list[str]:
    if checkpoint_lookup is None or not codes:
        return list(codes)

    remaining_codes = [
        stock_code
        for stock_code in codes
        if not _code_checkpoints_complete(
            store,
            checkpoint_lookup,
            stock_code,
            daily_targets,
            needs_baostock_cn_stock_adjustment_factor_api,
            mode,
            start_date,
            end_date,
        )
    ]
    skipped_count = len(codes) - len(remaining_codes)
    if skipped_count:
        skipped_ratio = skipped_count / len(codes) * 100
        logger.info(
            "Checkpoint prefilter skipped {}/{} codes ({:.1f}%); processing {} codes",
            skipped_count,
            len(codes),
            skipped_ratio,
            len(remaining_codes),
        )
    return remaining_codes


def _code_checkpoints_complete(
    store: ParquetStore,
    checkpoint_lookup: PipelineCheckpointLookup,
    code: str,
    daily_targets: list[str],
    needs_baostock_cn_stock_adjustment_factor_api: bool,
    mode: str,
    start_date: str,
    end_date: str,
) -> bool:
    if needs_baostock_cn_stock_adjustment_factor_api and not _checkpoint_lookup_succeeded(
        store,
        checkpoint_lookup,
        BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET,
        code,
        FULL_HISTORY_START_DATE,
        end_date,
    ):
        return False

    checkpoint_start_date = FULL_HISTORY_START_DATE if mode == "full" else start_date
    for dataset in daily_targets:
        if not _checkpoint_lookup_succeeded(
            store,
            checkpoint_lookup,
            dataset,
            code,
            checkpoint_start_date,
            end_date,
        ):
            return False
    return bool(needs_baostock_cn_stock_adjustment_factor_api or daily_targets)


def _checkpoint_lookup_succeeded(
    store: ParquetStore,
    checkpoint_lookup: PipelineCheckpointLookup,
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
) -> bool:
    output_path = checkpoint_output_path(store, dataset, code, end_date)
    return checkpoint_lookup.pipeline_checkpoint_succeeded(
        PIPELINE_UPDATE_DAILY,
        dataset,
        code,
        start_date,
        end_date,
        output_path,
    )


def _dataset_targets(dataset: str) -> tuple[bool, bool, bool, list[str]]:
    if dataset == "all":
        return True, True, True, list(DAILY_BAR_DATASETS)
    if dataset == "baostock_cn_trading_calendar":
        return True, False, False, []
    if dataset == "baostock_cn_stock_basic":
        return False, True, False, []
    if dataset == BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET:
        return False, False, True, []
    if dataset in DAILY_BAR_DATASETS:
        return False, False, False, expand_daily_datasets(dataset)
    raise ValueError(f"Unsupported update dataset: {dataset}")


def _write_baostock_cn_stock_basic_target(
    store: ParquetStore,
    provider: MarketDataProvider,
    run_records: list[dict[str, object]],
    start_date: str,
    end_date: str,
    resume: bool,
    force: bool,
    checkpoint_lookup: PipelineCheckpointLookup | None,
) -> None:
    baostock_cn_stock_basic_path = checkpoint_output_path(store, "baostock_cn_stock_basic", "*", end_date)
    if should_skip_checkpoint(
        store,
        PIPELINE_UPDATE_DAILY,
        "baostock_cn_stock_basic",
        "*",
        start_date,
        end_date,
        baostock_cn_stock_basic_path,
        resume,
        force,
        checkpoint_lookup,
    ):
        run_row = _add_skipped_run(run_records, "baostock_cn_stock_basic", "*", start_date, end_date, "checkpoint")
        _persist_run_status(store, run_row)
        return

    basic_df = fetch_baostock_cn_stock_basic(provider)
    log_api_fetch("baostock_cn_stock_basic", "*", start_date, end_date, basic_df)
    baostock_cn_stock_basic_path = store.write_baostock_cn_stock_basic(basic_df)
    run_row = _add_success_run(run_records, "baostock_cn_stock_basic", "*", start_date, end_date, len(basic_df))
    status_row = _status_row("baostock_cn_stock_basic", "*", end_date, len(basic_df), "success", "")
    _persist_run_status(store, run_row, status_row)
    write_checkpoint(
        store,
        PIPELINE_UPDATE_DAILY,
        "baostock_cn_stock_basic",
        "*",
        start_date,
        end_date,
        "success",
        len(basic_df),
        baostock_cn_stock_basic_path,
    )
