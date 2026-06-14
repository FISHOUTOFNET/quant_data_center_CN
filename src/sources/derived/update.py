"""Unified entry point for derived dataset builds."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.sources.derived.common import build_derived_file_lock, refresh_derived_registry
from src.sources.derived.manifest import (
    current_source_signature_for_security,
    source_partition_pairs_for_security,
)
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
        try:
            security_ids = _changed_security_ids_for_target(store, master, target, changed_since)
        except RuntimeError as exc:
            full_targets.append(target)
            reasons[target] = str(exc)
            continue
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
    master: pd.DataFrame,
    target: str,
    changed_since: datetime | None,
) -> tuple[str, ...]:
    changed: set[str] = set()
    target_dataset = DATASET_BY_TARGET[target]
    target_manifests = store.read_dataset_partition_manifest(target_dataset)
    for _, security in master.iterrows():
        security_id = _clean_string(security.get("security_id"))
        if not security_id:
            continue
        source_pairs = _source_partition_pairs_for_security(store, security, target)
        target_path = store.dataset_path(target_dataset, {"security_id": security_id})
        target_exists = target_path.exists()
        if not source_pairs:
            if target_exists:
                changed.add(security_id)
            continue
        if not target_exists:
            changed.add(security_id)
            continue

        current_signature, current_master_hash, source_rows = current_source_signature_for_security(
            store,
            security,
            source_pairs,
        )
        target_manifest = _target_manifest_row(target_manifests, security_id)
        if target_manifest is None:
            changed.add(security_id)
            continue

        target_signature = _clean_string(target_manifest.get("source_signature"))
        target_master_hash = _clean_string(target_manifest.get("master_row_hash"))
        if (
            not target_signature
            or current_signature != target_signature
            or current_master_hash != target_master_hash
            or _source_updated_since(source_rows, changed_since)
        ):
            changed.add(security_id)
    return tuple(sorted(changed))


def _source_partition_pairs_for_security(store: ParquetStore, security: pd.Series, target: str):
    if target == "daily_bar":
        return source_partition_pairs_for_security(
            store,
            security,
            (
                *((dataset_id, "baostock_code") for dataset_id in BAOSTOCK_DAILY_SOURCES),
                *((dataset_id, "akshare_code") for dataset_id in AKSHARE_DAILY_SOURCES),
            ),
        )
    if target == "valuation":
        return source_partition_pairs_for_security(
            store,
            security,
            (
                (AKSHARE_VALUATION_DATASET, "akshare_code"),
                (BAOSTOCK_PERCENTILE_DATASET, "baostock_code"),
            ),
        )
    return ()


def _target_manifest_row(manifests: pd.DataFrame, security_id: str) -> pd.Series | None:
    if manifests.empty:
        return None
    matched = manifests.loc[
        (manifests["partition_column"].astype("string") == "security_id")
        & (manifests["partition_value"].astype("string") == security_id)
    ]
    if matched.empty:
        return None
    return matched.iloc[-1]


def _source_updated_since(source_rows: pd.DataFrame, changed_since: datetime | None) -> bool:
    if changed_since is None or source_rows.empty or "updated_at" not in source_rows.columns:
        return False
    updated_at = pd.to_datetime(source_rows["updated_at"], errors="coerce")
    return bool((updated_at >= pd.Timestamp(changed_since)).any())


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
