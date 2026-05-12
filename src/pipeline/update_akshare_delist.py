"""Manual AkShare A-share delisted stock updates."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.api.akshare_client import AkShareClient
from src.pipeline.akshare_common import (
    PIPELINE_UPDATE_AKSHARE_DELIST,
    error_stack,
    failed_metadata,
    persist_metadata,
    success_metadata,
)
from src.pipeline.common import should_skip_checkpoint
from src.pipeline.dry_run import apply_limit, dry_run_record
from src.storage.dataset_catalog import AKSHARE_DELIST_SH_DATASET, AKSHARE_DELIST_SZ_DATASET
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger

EXCHANGE_CONFIG = {
    "sh": {
        "dataset": AKSHARE_DELIST_SH_DATASET,
        "fetch_method": "fetch_akshare_cn_stock_delist_sh",
        "write_method": "write_akshare_cn_stock_delist_sh",
        "path_method": "akshare_cn_stock_delist_sh_path",
        "default_symbol": "全部",
    },
    "sz": {
        "dataset": AKSHARE_DELIST_SZ_DATASET,
        "fetch_method": "fetch_akshare_cn_stock_delist_sz",
        "write_method": "write_akshare_cn_stock_delist_sz",
        "path_method": "akshare_cn_stock_delist_sz_path",
        "default_symbol": "终止上市公司",
    },
}


def update_akshare_delist(
    market: str | None = None,
    snapshot_date: str | date | None = None,
    root: Path | None = None,
    resume: bool = True,
    force: bool = False,
    build_views: bool = True,
    exchanges: list[str] | None = None,
    max_tasks: int | None = None,
    client: Any | None = None,
    client_factory: Callable[[ConfigManager], Any] | None = None,
    dry_run: bool = False,
) -> list[dict[str, object]]:
    """Fetch delist snapshots for both SH and SZ exchanges."""

    config = ConfigManager(root)
    store = ParquetStore(root=config.root)
    resolved_snapshot_date = _date_iso(snapshot_date, datetime.now().date().isoformat())

    resolved_exchanges = exchanges if exchanges is not None else list(EXCHANGE_CONFIG.keys())
    valid_exchanges = apply_limit(
        [exchange for exchange in resolved_exchanges if exchange in EXCHANGE_CONFIG],
        max_tasks,
        "max_tasks",
    )
    if not valid_exchanges and not dry_run:
        return []

    if dry_run:
        records = []
        for exchange in valid_exchanges:
            exchange_config = EXCHANGE_CONFIG[exchange]
            resolved_symbol = market if market else exchange_config.get("default_symbol", "全部")
            output_path = getattr(store, exchange_config["path_method"])(resolved_snapshot_date)
            records.append(
                dry_run_record(
                    exchange_config["dataset"].name,
                    resolved_symbol,
                    resolved_snapshot_date,
                    resolved_snapshot_date,
                    output_path,
                    operation=str(exchange_config["write_method"]),
                    exchange=exchange,
                )
            )
        return records

    store.ensure_layout()

    ak_client = client or (
        client_factory(config)
        if client_factory is not None
        else AkShareClient(config=config)
    )

    all_metadata = []
    progress_processed = 0
    progress_success = 0
    progress_failed = 0
    progress_skipped = 0
    logger.info(
        "AkShare delist update started market={} snapshot_date={} force={} planned_tasks={} processing_tasks={}",
        market or "",
        resolved_snapshot_date,
        force,
        len(resolved_exchanges),
        len(valid_exchanges),
    )

    for exchange in valid_exchanges:
        exchange_metadata = _fetch_exchange_delist(
            store=store,
            ak_client=ak_client,
            exchange=exchange,
            market=market,
            snapshot_date=resolved_snapshot_date,
            resume=resume,
            force=force,
        )
        all_metadata.extend(exchange_metadata)
        exchange_config = EXCHANGE_CONFIG[exchange]
        resolved_symbol = market if market else exchange_config.get("default_symbol", "全部")
        if exchange_metadata:
            row = exchange_metadata[-1][0]
        else:
            row = {
                "dataset": exchange_config["dataset"].name,
                "code": resolved_symbol,
                "status": "skipped_checkpoint",
                "row_count": 0,
            }
        progress_processed += 1
        status = str(row.get("status", "unknown"))
        if status == "success":
            progress_success += 1
        elif status == "failed":
            progress_failed += 1
        elif status.startswith("skipped"):
            progress_skipped += 1
        logger.info(
            "AkShare delist progress {}/{} exchange={} code={} dataset={} status={} rows={}",
            progress_processed,
            len(valid_exchanges),
            exchange,
            row.get("code", resolved_symbol),
            row.get("dataset", exchange_config["dataset"].name),
            status,
            row.get("row_count", 0),
        )

    records = persist_metadata(store, all_metadata)
    store.close()
    if build_views:
        DuckDBStore(root=config.root).build_views(cleanup_tmp_files=progress_success > 0)
    logger.info(
        "AkShare delist update completed processed={} success={} failed={} skipped={}",
        progress_processed,
        progress_success,
        progress_failed,
        progress_skipped,
    )
    return records


def _fetch_exchange_delist(
    store: ParquetStore,
    ak_client: Any,
    exchange: str,
    market: str | None,
    snapshot_date: str,
    resume: bool,
    force: bool,
) -> list[tuple[dict[str, object], dict[str, object], dict[str, object]]]:
    """Fetch delist data for a single exchange."""

    config = EXCHANGE_CONFIG[exchange]
    dataset = config["dataset"].name
    output_path = getattr(store, config["path_method"])(snapshot_date)
    resolved_symbol = market if market else config.get("default_symbol", "全部")

    if should_skip_checkpoint(
        store,
        PIPELINE_UPDATE_AKSHARE_DELIST,
        dataset,
        resolved_symbol,
        snapshot_date,
        snapshot_date,
        output_path,
        resume,
        force,
    ):
        return []

    metadata = []
    started_at = datetime.now()
    try:
        fetch_method = getattr(ak_client, config["fetch_method"])
        response = fetch_method(symbol=resolved_symbol, snapshot_date=snapshot_date)
        write_method = getattr(store, config["write_method"])
        output_path = write_method(snapshot_date, response.data)
        ended_at = datetime.now()
        metadata.append(
            success_metadata(
                PIPELINE_UPDATE_AKSHARE_DELIST,
                dataset,
                resolved_symbol,
                snapshot_date,
                snapshot_date,
                started_at,
                ended_at,
                len(response.data),
                output_path,
            )
        )
    except Exception as exc:
        ended_at = datetime.now()
        stack = error_stack(exc)
        metadata.append(
            failed_metadata(
                PIPELINE_UPDATE_AKSHARE_DELIST,
                dataset,
                resolved_symbol,
                snapshot_date,
                snapshot_date,
                started_at,
                ended_at,
                stack,
                output_path,
            )
        )

    return metadata


def _date_iso(value: str | date | None, default: str) -> str:
    if value is None:
        return default
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return pd.to_datetime(value, errors="raise").date().isoformat()
