"""Unified entry point for derived dataset builds."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from src.sources.derived.common import build_derived_file_lock, refresh_derived_registry
from src.sources.derived.security_master import build_security_master
from src.sources.derived.stock_daily_bar import (
    AKSHARE_DAILY_SOURCES,
    BAOSTOCK_DAILY_SOURCES,
    build_cn_stock_daily_bar,
)
from src.sources.derived.stock_valuation import (
    AKSHARE_VALUATION_DATASET,
    BAOSTOCK_PERCENTILE_DATASET,
    build_cn_stock_valuation,
)
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore
from src.utils.logging import logger

TARGET_ORDER = ("security_master", "daily_bar", "valuation")
DATASET_BY_TARGET = {
    "security_master": "cn_security_master",
    "daily_bar": "cn_stock_daily_bar",
    "valuation": "cn_stock_valuation",
}
BuildMode = str


@dataclass(frozen=True)
class IncrementalPlan:
    security_ids_by_target: dict[str, tuple[str, ...]]
    full_targets: tuple[str, ...]
    reasons: dict[str, str]


def build_derived_datasets(
    *,
    root: Path | None = None,
    targets: tuple[str, ...] = ("all",),
    mode: BuildMode = "incremental",
    security_ids: tuple[str, ...] | None = None,
    changed_since: datetime | None = None,
    build_views: bool = True,
    refresh_registry: bool = True,
    now: Callable[[], datetime] | None = None,
) -> list[dict[str, object]]:
    if mode not in {"full", "incremental"}:
        raise ValueError(f"Unsupported derived build mode: {mode}")
    normalized_security_ids = _normalize_security_ids(security_ids)
    if mode == "full" and normalized_security_ids:
        raise ValueError("--security-id is only supported with --mode incremental")

    store = ParquetStore(root=root)
    expanded = _expand_targets(targets)
    if any(target in expanded for target in ("daily_bar", "valuation")) and "security_master" not in expanded:
        expanded = ("security_master", *expanded)

    with build_derived_file_lock(store.root, expanded):
        store.ensure_layout()
        results: list[dict[str, object]] = []
        incremental_plan: IncrementalPlan | None = None
        for target in expanded:
            if mode == "incremental" and target != "security_master" and incremental_plan is None:
                incremental_plan = _resolve_incremental_plan(
                    store,
                    expanded,
                    explicit_security_ids=normalized_security_ids,
                    changed_since=changed_since,
                )
            builder = {
                "security_master": build_security_master,
                "daily_bar": build_cn_stock_daily_bar,
                "valuation": build_cn_stock_valuation,
            }[target]
            target_security_ids = _target_security_ids(target, mode, incremental_plan or IncrementalPlan({}, (), {}))
            results.append(
                builder(
                    root=store.root,
                    security_ids=target_security_ids,
                    changed_since=changed_since,
                    build_views=False,
                    refresh_registry=False,
                    now=now,
                )
            )

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


def _target_security_ids(
    target: str,
    mode: BuildMode,
    incremental_plan: IncrementalPlan,
) -> tuple[str, ...] | None:
    if target == "security_master" or mode == "full" or target in incremental_plan.full_targets:
        return None
    return incremental_plan.security_ids_by_target.get(target, ())


def _resolve_incremental_plan(
    store: ParquetStore,
    targets: tuple[str, ...],
    *,
    explicit_security_ids: tuple[str, ...],
    changed_since: datetime | None,
) -> IncrementalPlan:
    target_ids = tuple(target for target in targets if target in {"daily_bar", "valuation"})
    if not target_ids:
        return IncrementalPlan({}, (), {})
    if explicit_security_ids:
        logger.info("Derived incremental build using explicit security_ids={}", explicit_security_ids)
        return IncrementalPlan(dict.fromkeys(target_ids, explicit_security_ids), (), {})

    master = store.read_dataset("cn_security_master")
    if master.empty or "security_id" not in master.columns:
        reasons = dict.fromkeys(target_ids, "cn_security_master is missing or empty")
        _log_incremental_fallback(reasons)
        return IncrementalPlan({}, target_ids, reasons)

    full_targets: list[str] = []
    reasons: dict[str, str] = {}
    security_ids_by_target: dict[str, tuple[str, ...]] = {}
    for target in target_ids:
        dataset_id = DATASET_BY_TARGET[target]
        if not (store.parquet_dir / dataset_id).exists():
            full_targets.append(target)
            reasons[target] = f"{dataset_id} has not been built yet"
            continue
        security_ids = _changed_security_ids_for_target(store, master, target, changed_since)
        security_ids_by_target[target] = security_ids
        logger.info(
            "Derived incremental build target={} changed_security_count={} security_ids={}",
            target,
            len(security_ids),
            security_ids[:50],
        )
    if reasons:
        _log_incremental_fallback(reasons)
    return IncrementalPlan(security_ids_by_target, tuple(full_targets), reasons)


def _changed_security_ids_for_target(
    store: ParquetStore,
    master,
    target: str,
    changed_since: datetime | None,
) -> tuple[str, ...]:
    changed: set[str] = set()
    for _, security in master.iterrows():
        security_id = _clean_string(security.get("security_id"))
        if not security_id:
            continue
        source_paths = _source_paths_for_security(store, security, target)
        if not source_paths:
            continue
        target_path = store.dataset_path(DATASET_BY_TARGET[target], {"security_id": security_id})
        target_mtime = _path_mtime(target_path)
        if target_mtime is None:
            changed.add(security_id)
            continue
        for source_path in source_paths:
            source_mtime = _path_mtime(source_path)
            if source_mtime is None:
                continue
            if changed_since is not None and source_mtime >= changed_since:
                changed.add(security_id)
                break
            if changed_since is None and source_mtime > target_mtime:
                changed.add(security_id)
                break
    return tuple(sorted(changed))


def _source_paths_for_security(store: ParquetStore, security, target: str) -> tuple[Path, ...]:
    paths: list[Path] = []
    baostock_code = _clean_string(security.get("baostock_code"))
    akshare_code = _clean_string(security.get("akshare_code"))
    if target == "daily_bar":
        if baostock_code:
            paths.extend(
                store.dataset_path(dataset_id, {"code": baostock_code})
                for dataset_id in BAOSTOCK_DAILY_SOURCES
                if store.dataset_exists(dataset_id, {"code": baostock_code})
            )
        if akshare_code:
            paths.extend(
                store.dataset_path(dataset_id, {"code": akshare_code})
                for dataset_id in AKSHARE_DAILY_SOURCES
                if store.dataset_exists(dataset_id, {"code": akshare_code})
            )
    elif target == "valuation":
        if akshare_code and store.dataset_exists(AKSHARE_VALUATION_DATASET, {"code": akshare_code}):
            paths.append(store.dataset_path(AKSHARE_VALUATION_DATASET, {"code": akshare_code}))
        if baostock_code and store.dataset_exists(BAOSTOCK_PERCENTILE_DATASET, {"code": baostock_code}):
            paths.append(store.dataset_path(BAOSTOCK_PERCENTILE_DATASET, {"code": baostock_code}))
    return tuple(paths)


def _path_mtime(path: Path) -> datetime | None:
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime)


def _normalize_security_ids(security_ids: tuple[str, ...] | None) -> tuple[str, ...]:
    if not security_ids:
        return ()
    return tuple(dict.fromkeys(_clean_string(value).upper() for value in security_ids if _clean_string(value)))


def _clean_string(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _log_incremental_fallback(reasons: dict[str, str]) -> None:
    for target, reason in reasons.items():
        logger.warning("Derived incremental build falling back to full target={} reason={}", target, reason)
