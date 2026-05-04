"""Manual AkShare A-share universe updates."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.api.akshare_client import AkShareClient
from src.pipeline.akshare_common import (
    PIPELINE_UPDATE_AKSHARE_UNIVERSE,
    append_failed_manifest,
    append_response_manifest,
    error_stack,
    error_type,
    failed_metadata,
    persist_metadata,
    success_metadata,
    write_raw_response,
)
from src.pipeline.common import should_skip_checkpoint
from src.storage.dataset_catalog import STOCK_INFO_SH_DELIST_DATASET
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager


def update_akshare_universe(
    market: str = "全部",
    snapshot_date: str | date | None = None,
    root: Path | None = None,
    resume: bool = True,
    force: bool = False,
    build_views: bool = True,
    client: Any | None = None,
    client_factory: Callable[[ConfigManager, pd.DataFrame], Any] | None = None,
) -> list[dict[str, object]]:
    """Fetch the manual SH delist snapshot used by the AkShare A-share pool."""

    config = ConfigManager(root)
    store = ParquetStore(root=config.root)
    store.ensure_layout()
    resolved_snapshot_date = _date_iso(snapshot_date, datetime.now().date().isoformat())
    dataset = STOCK_INFO_SH_DELIST_DATASET.name
    output_path = store.stock_info_sh_delist_path(resolved_snapshot_date)
    if should_skip_checkpoint(
        store,
        PIPELINE_UPDATE_AKSHARE_UNIVERSE,
        dataset,
        market,
        resolved_snapshot_date,
        resolved_snapshot_date,
        output_path,
        resume,
        force,
    ):
        store.close()
        return []

    stock_basic_df = store.read_stock_basic()
    ak_client = client or (
        client_factory(config, stock_basic_df)
        if client_factory is not None
        else AkShareClient(config=config, stock_basic_df=stock_basic_df)
    )

    metadata = []
    started_at = datetime.now()
    try:
        response = ak_client.fetch_stock_info_sh_delist(symbol=market, snapshot_date=resolved_snapshot_date)
        raw_path = write_raw_response(store.root, response, started_at)
        output_path = store.write_stock_info_sh_delist(resolved_snapshot_date, response.data)
        ended_at = datetime.now()
        append_response_manifest(
            store,
            PIPELINE_UPDATE_AKSHARE_UNIVERSE,
            dataset,
            market,
            response,
            raw_path,
            "success",
            "",
            "",
            started_at,
            ended_at,
        )
        metadata.append(
            success_metadata(
                PIPELINE_UPDATE_AKSHARE_UNIVERSE,
                dataset,
                market,
                resolved_snapshot_date,
                resolved_snapshot_date,
                started_at,
                ended_at,
                len(response.data),
                output_path,
            )
        )
    except Exception as exc:
        ended_at = datetime.now()
        stack = error_stack(exc)
        append_failed_manifest(
            store,
            PIPELINE_UPDATE_AKSHARE_UNIVERSE,
            dataset,
            "stock_info_sh_delist",
            market,
            {"symbol": market, "snapshot_date": resolved_snapshot_date},
            ak_client,
            error_type(exc),
            str(exc),
            started_at,
            ended_at,
        )
        metadata.append(
            failed_metadata(
                PIPELINE_UPDATE_AKSHARE_UNIVERSE,
                dataset,
                market,
                resolved_snapshot_date,
                resolved_snapshot_date,
                started_at,
                ended_at,
                stack,
                output_path,
            )
        )

    records = persist_metadata(store, metadata)
    store.close()
    if build_views:
        DuckDBStore(root=config.root).build_views()
    return records


def _date_iso(value: str | date | None, default: str) -> str:
    if value is None:
        return default
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return pd.to_datetime(value, errors="raise").date().isoformat()
