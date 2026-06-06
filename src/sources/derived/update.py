"""Unified entry point for derived dataset builds."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from src.sources.derived.common import refresh_derived_registry
from src.sources.derived.security_master import build_security_master
from src.sources.derived.stock_daily_bar import build_cn_stock_daily_bar
from src.sources.derived.stock_valuation import build_cn_stock_valuation
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore

TARGET_ORDER = ("security_master", "daily_bar", "valuation")
DATASET_BY_TARGET = {
    "security_master": "cn_security_master",
    "daily_bar": "cn_stock_daily_bar",
    "valuation": "cn_stock_valuation",
}


def build_derived_datasets(
    *,
    root: Path | None = None,
    targets: tuple[str, ...] = ("all",),
    build_views: bool = True,
    refresh_registry: bool = True,
    now: Callable[[], datetime] | None = None,
) -> list[dict[str, object]]:
    store = ParquetStore(root=root)
    store.ensure_layout()
    expanded = _expand_targets(targets)
    if any(target in expanded for target in ("daily_bar", "valuation")) and "security_master" not in expanded:
        master = store.read_dataset("cn_security_master")
        if not store.dataset_exists("cn_security_master") or master.empty:
            expanded = ("security_master", *expanded)

    results: list[dict[str, object]] = []
    for target in expanded:
        builder = {
            "security_master": build_security_master,
            "daily_bar": build_cn_stock_daily_bar,
            "valuation": build_cn_stock_valuation,
        }[target]
        results.append(builder(root=store.root, build_views=False, refresh_registry=False, now=now))

    if refresh_registry:
        refresh_derived_registry(store, [str(result["dataset"]) for result in results])
    if build_views:
        DuckDBStore(root=store.root).build_views()
    return results


def _expand_targets(targets: tuple[str, ...]) -> tuple[str, ...]:
    requested: list[str] = []
    for target in targets:
        if target == "all":
            requested.extend(TARGET_ORDER)
        elif target in TARGET_ORDER:
            requested.append(target)
        else:
            raise ValueError(f"Unsupported derived target: {target}")

    deduped = []
    for target in TARGET_ORDER:
        if target in requested and target not in deduped:
            deduped.append(target)
    return tuple(deduped)
