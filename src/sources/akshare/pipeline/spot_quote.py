"""AkShare spot quote update module with Sina fallback."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from src.pipeline.common import (
    default_candidate_date,
    is_trading_day,
    latest_trading_day_on_or_before,
    should_skip_checkpoint,
)
from src.pipeline.lifecycle import LifecycleTaskRef
from src.sources.akshare.core.normalization import date_iso
from src.sources.akshare.pipeline.common import PIPELINE_UPDATE_AKSHARE_SPOT, error_stack
from src.sources.akshare.pipeline.execution_types import (
    AkShareExecutionContext,
    AkShareUpdateRequest,
    ConcurrencyPolicy,
    FetchResult,
)
from src.sources.akshare.pipeline.universe import normalize_akshare_code_list
from src.storage.dataset_catalog import (
    AKSHARE_SPOT_QUOTE_EASTMONEY_DATASET,
    AKSHARE_SPOT_QUOTE_SINA_DATASET,
    akshare_daily_bar_dataset_id,
)
from src.storage.parquet_store import ParquetStore
from src.storage.schema import field_names, schema_for_dataset
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger


@dataclass(frozen=True)
class SpotQuoteTask:
    dataset: str
    trade_date: str
    output_path: Path
    update_daily_bar_from_spot: bool
    skipped: bool = False


class SpotQuoteModule:
    target = "spot_quote"

    def plan(self, request: AkShareUpdateRequest, context: AkShareExecutionContext) -> list[SpotQuoteTask]:
        _ensure_spot_quote_close_window(context.config, request.now, context.store)
        trade_date = _resolve_trade_date(context.config, request.end)
        dataset = AKSHARE_SPOT_QUOTE_EASTMONEY_DATASET.name
        output_path = context.store.dataset_path(dataset, {"trade_date": trade_date})
        skipped = should_skip_checkpoint(
            context.store,
            PIPELINE_UPDATE_AKSHARE_SPOT,
            dataset,
            "*",
            trade_date,
            trade_date,
            output_path,
            request.resume,
            request.force,
            context.checkpoint_lookup,
        )
        return [
            SpotQuoteTask(
                dataset=dataset,
                trade_date=trade_date,
                output_path=output_path,
                update_daily_bar_from_spot=bool(
                    context.config.get("datasets.akshare_cn_stock_spot_quote.update_daily_bar_from_spot", True)
                ),
                skipped=skipped,
            )
        ]

    def prefilter(self, tasks: list[SpotQuoteTask], context: AkShareExecutionContext) -> list[SpotQuoteTask]:
        return list(tasks)

    def fetch(self, task: SpotQuoteTask, context: AkShareExecutionContext) -> FetchResult:
        now = datetime.now()
        if task.skipped:
            return FetchResult(task=task, started_at=now, ended_at=now, skipped=True)
        try:
            response = context.client.fetch_spot_quote_eastmoney(trade_date=task.trade_date)
            return FetchResult(task=task, started_at=now, ended_at=datetime.now(), response=response)
        except Exception as exc:
            return FetchResult(
                task=task, started_at=now, ended_at=datetime.now(), error=exc, error_stack=error_stack(exc)
            )

    def record_result(self, result: FetchResult, context: AkShareExecutionContext) -> list[dict[str, object]]:
        task = result.task
        if result.skipped:
            return []
        records: list[dict[str, object]] = []
        if result.error is not None:
            logger.info("AkShare spot fallback started trade_date={} reason={}", task.trade_date, str(result.error))
            fallback_records = _run_sina_fallback(
                context.store,
                context.client,
                task.trade_date,
                str(result.error),
                context,
                task.update_daily_bar_from_spot,
            )
            records.extend(fallback_records)
            fallback_succeeded = any(str(row.get("status")) == "success" for row in fallback_records)
            rows = (
                context.lifecycle.record_skipped(
                    _task_ref(task.dataset, "*", task.trade_date, task.trade_date, task.output_path),
                    status="skipped_fallback",
                    started_at=result.started_at,
                    ended_at=result.ended_at,
                    reason=result.error_stack or str(result.error),
                )
                if fallback_succeeded
                else context.lifecycle.record_failure(
                    _task_ref(task.dataset, "*", task.trade_date, task.trade_date, task.output_path),
                    started_at=result.started_at,
                    ended_at=result.ended_at,
                    error_stack=result.error_stack,
                )
            )
            records.append(rows.run_row)
            return records

        try:
            assert result.response is not None
            output_path = context.store.write_dataset(
                task.dataset, result.response.data, {"trade_date": task.trade_date}
            ).primary_path
            daily_bar_rows = _drop_delisted_daily_bar_rows(
                context.store, spot_em_to_daily_bar_unadjusted(result.response.data)
            )
            daily_bar_output_path = (
                _write_spot_daily_bar_rows(context.store, daily_bar_rows)
                if task.update_daily_bar_from_spot
                else context.store.parquet_dir / akshare_daily_bar_dataset_id("unadjusted")
            )
            ended_at = datetime.now()
            rows = context.lifecycle.record_success(
                _task_ref(task.dataset, "*", task.trade_date, task.trade_date, output_path),
                started_at=result.started_at,
                ended_at=ended_at,
                row_count=len(result.response.data),
                output_path=output_path,
            )
            records.append(rows.run_row)
            if task.update_daily_bar_from_spot:
                daily_bar_dataset = akshare_daily_bar_dataset_id("unadjusted")
                daily_bar_rows_result = context.lifecycle.record_success(
                    _task_ref(daily_bar_dataset, "*", task.trade_date, task.trade_date, daily_bar_output_path),
                    started_at=result.started_at,
                    ended_at=ended_at,
                    row_count=len(daily_bar_rows),
                    output_path=daily_bar_output_path,
                )
                records.append(daily_bar_rows_result.run_row)
        except Exception as exc:
            rows = context.lifecycle.record_failure(
                _task_ref(task.dataset, "*", task.trade_date, task.trade_date, task.output_path),
                started_at=result.started_at,
                ended_at=datetime.now(),
                error_stack=error_stack(exc),
            )
            records.append(rows.run_row)
        return records

    def record_skip(
        self,
        task: SpotQuoteTask,
        context: AkShareExecutionContext,
        status: str = "skipped_checkpoint",
        reason: str = "checkpoint",
    ) -> list[dict[str, object]]:
        del task, context, status, reason
        return []

    def progress_row(self, task: SpotQuoteTask, rows: list[dict[str, object]]) -> dict[str, object]:
        if rows:
            return rows[0]
        return {"dataset": task.dataset, "code": "*", "status": "skipped_checkpoint", "row_count": 0}

    def concurrency(self, request: AkShareUpdateRequest, context: AkShareExecutionContext) -> ConcurrencyPolicy:
        return ConcurrencyPolicy(workers=1)

    def log_started(self, request: AkShareUpdateRequest, planned: int, processing: int, workers: int) -> None:
        logger.info(
            "AkShare spot update started trade_date={} force={} resume={}",
            request.end or "",
            request.force,
            request.resume,
        )

    def log_progress(self, progress: Any, task: SpotQuoteTask, row: dict[str, object]) -> None:
        logger.info(
            "AkShare spot progress {}/{} code={} dataset={} status={} rows={}",
            progress.processed,
            progress.total,
            row.get("code", "*"),
            row.get("dataset", task.dataset),
            row.get("status", "unknown"),
            row.get("row_count", 0),
        )

    def log_completed(self, progress: Any) -> None:
        logger.info(
            "AkShare spot update completed processed={} success={} failed={} skipped={}",
            progress.processed,
            progress.success,
            progress.failed,
            progress.skipped,
        )


def spot_em_to_daily_bar_unadjusted(df: pd.DataFrame) -> pd.DataFrame:
    schema = schema_for_dataset(akshare_daily_bar_dataset_id("unadjusted"))
    if df.empty:
        return pd.DataFrame(columns=field_names(schema))
    daily_bar = pd.DataFrame(
        {
            "date": df["trade_date"],
            "code": df["code"],
            "source_symbol": df["source_symbol"],
            "open": df["open"],
            "high": df["high"],
            "low": df["low"],
            "close": df["last_price"],
            "volume": pd.to_numeric(df["volume"], errors="coerce").round().astype("Int64"),
            "amount": df["amount"],
            "amplitude": df["amplitude"],
            "pct_change": df["pct_change"],
            "price_change": df["price_change"],
            "turnover_rate": df["turnover_rate"],
            "adjustment": "unadjusted",
            "source_endpoint": "stock_zh_a_spot_em",
            "quality_status": "spot_quote_close",
            "fetched_at": df["fetched_at"],
        }
    )
    return daily_bar[field_names(schema)].reset_index(drop=True)


def spot_sina_to_daily_bar_unadjusted(df: pd.DataFrame) -> pd.DataFrame:
    schema = schema_for_dataset(akshare_daily_bar_dataset_id("unadjusted"))
    if df.empty:
        return pd.DataFrame(columns=field_names(schema))
    daily_bar = pd.DataFrame(
        {
            "date": df["trade_date"],
            "code": df["code"],
            "source_symbol": df["source_symbol"],
            "open": df["open"],
            "high": df["high"],
            "low": df["low"],
            "close": df["last_price"],
            "volume": pd.to_numeric(df["volume"], errors="coerce").round().astype("Int64"),
            "amount": df["amount"],
            "amplitude": pd.NA,
            "pct_change": df["pct_change"],
            "price_change": df["price_change"],
            "turnover_rate": pd.NA,
            "adjustment": "unadjusted",
            "source_endpoint": "stock_zh_a_spot",
            "quality_status": "spot_quote_close",
            "fetched_at": df["fetched_at"],
        }
    )
    return daily_bar[field_names(schema)].reset_index(drop=True)


def _write_spot_daily_bar_rows(store: ParquetStore, daily_bar_rows: pd.DataFrame) -> Path:
    dataset = akshare_daily_bar_dataset_id("unadjusted")
    dataset_dir = store.parquet_dir / dataset
    logger.info("AkShare spot daily-bar upsert started dataset={} rows={}", dataset, len(daily_bar_rows))
    if daily_bar_rows.empty:
        dataset_dir.mkdir(parents=True, exist_ok=True)
        logger.info("AkShare spot daily-bar upsert completed dataset={} rows={} path={}", dataset, 0, dataset_dir)
        return dataset_dir
    result = store.write_dataset(dataset, daily_bar_rows, mode="upsert", skip_existing=True)
    if result.skipped_partitions > 0:
        logger.info(
            "Spot daily-bar batch append completed updated={} skipped={}",
            result.updated_partitions,
            result.skipped_partitions,
        )
    logger.info(
        "AkShare spot daily-bar upsert completed dataset={} rows={} path={}",
        dataset,
        len(daily_bar_rows),
        dataset_dir,
    )
    return dataset_dir


def _run_sina_fallback(
    store: ParquetStore,
    client: Any,
    trade_date: str,
    fallback_reason: str,
    context: AkShareExecutionContext,
    update_daily_bar_from_spot: bool,
) -> list[dict[str, object]]:
    dataset = AKSHARE_SPOT_QUOTE_SINA_DATASET.name
    output_path = store.dataset_path(dataset, {"trade_date": trade_date})
    records: list[dict[str, object]] = []
    started_at = datetime.now()
    try:
        response = client.fetch_spot_quote_sina(trade_date=trade_date, fallback_reason=fallback_reason)
        output_path = store.write_dataset(dataset, response.data, {"trade_date": trade_date}).primary_path
        logger.info(
            "AkShare spot fallback parquet write completed dataset={} rows={} path={}",
            dataset,
            len(response.data),
            output_path,
        )
        daily_bar_rows = spot_sina_to_daily_bar_unadjusted(response.data)
        daily_bar_output_path = (
            _write_spot_daily_bar_rows(store, daily_bar_rows)
            if update_daily_bar_from_spot
            else store.parquet_dir / akshare_daily_bar_dataset_id("unadjusted")
        )
        ended_at = datetime.now()
        rows = context.lifecycle.record_success(
            _task_ref(dataset, "*", trade_date, trade_date, output_path),
            started_at=started_at,
            ended_at=ended_at,
            row_count=len(response.data),
            output_path=output_path,
        )
        records.append(rows.run_row)
        if update_daily_bar_from_spot:
            daily_bar_dataset = akshare_daily_bar_dataset_id("unadjusted")
            daily_bar_rows_result = context.lifecycle.record_success(
                _task_ref(daily_bar_dataset, "*", trade_date, trade_date, daily_bar_output_path),
                started_at=started_at,
                ended_at=ended_at,
                row_count=len(daily_bar_rows),
                output_path=daily_bar_output_path,
            )
            records.append(daily_bar_rows_result.run_row)
        logger.info("AkShare spot fallback lifecycle recorded trade_date={} records={}", trade_date, len(records))
    except Exception as exc:
        rows = context.lifecycle.record_failure(
            _task_ref(dataset, "*", trade_date, trade_date, output_path),
            started_at=started_at,
            ended_at=datetime.now(),
            error_stack=error_stack(exc),
        )
        records.append(rows.run_row)
    return records


def _task_ref(dataset: str, code: str, start_date: str, end_date: str, output_path: Path) -> LifecycleTaskRef:
    return LifecycleTaskRef(PIPELINE_UPDATE_AKSHARE_SPOT, dataset, code, start_date, end_date, output_path)


def _resolve_trade_date(config: ConfigManager, end: str | date | None) -> str:
    candidate = date_iso(end, default_candidate_date(config)) if end is not None else default_candidate_date(config)
    store = ParquetStore(root=config.root)
    try:
        calendar = store.read_dataset("baostock_cn_trading_calendar")
    except Exception as exc:
        raise RuntimeError(
            "Cannot resolve trade date without baostock_cn_trading_calendar data. "
            "Please run baostock_cn_trading_calendar update first."
        ) from exc
    finally:
        store.close()
    if is_trading_day(calendar, candidate):
        return candidate
    if end is not None and calendar.empty:
        logger.warning("Spot trade_date uses explicit end={} because baostock_cn_trading_calendar is empty", candidate)
        return candidate
    try:
        resolved = latest_trading_day_on_or_before(calendar, candidate)
        logger.info("Spot trade_date resolved from {} to {} (non-trading day)", candidate, resolved)
        return resolved
    except ValueError as exc:
        raise RuntimeError(
            f"No trading day found on or before {candidate}. Please check baostock_cn_trading_calendar data."
        ) from exc


def _drop_delisted_daily_bar_rows(store: ParquetStore, daily_bar_rows: pd.DataFrame) -> pd.DataFrame:
    if daily_bar_rows.empty:
        return daily_bar_rows
    delist_df = store.read_latest_dataset("akshare_cn_stock_delist_sh")
    if daily_bar_rows.empty or delist_df.empty or "code" not in delist_df.columns:
        return daily_bar_rows
    delisted = set(normalize_akshare_code_list(delist_df["code"].dropna().astype(str).tolist()))
    if not delisted:
        return daily_bar_rows
    codes = daily_bar_rows["code"].astype("string").map(lambda value: str(value).strip())
    return daily_bar_rows.loc[~codes.isin(delisted)].reset_index(drop=True)


def _ensure_spot_quote_close_window(
    config: ConfigManager,
    now: Callable[[], datetime] | None,
    store: ParquetStore,
) -> None:
    timezone_name = str(config.get("project.timezone", "Asia/Shanghai"))
    local_now = (now or datetime.now)()
    if local_now.tzinfo is None:
        local_now = local_now.replace(tzinfo=ZoneInfo(timezone_name))
    else:
        local_now = local_now.astimezone(ZoneInfo(timezone_name))
    if local_now.time() < time(18, 0):
        if local_now.time() < time(8, 0):
            return
        try:
            calendar = store.read_dataset("baostock_cn_trading_calendar")
        except Exception as exc:
            raise RuntimeError(
                "stock_zh_a_spot_em/stock_zh_a_spot cannot verify trading day "
                "without baostock_cn_trading_calendar data. Please run baostock_cn_trading_calendar update first."
            ) from exc
        if not is_trading_day(calendar, local_now.date()):
            return
        raise RuntimeError(
            "stock_zh_a_spot_em/stock_zh_a_spot can only write daily bars after 18:00 "
            "and before 08:00 Asia/Shanghai on trading days"
        )
