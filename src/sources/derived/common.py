"""Shared utilities for derived dataset materializers."""

from __future__ import annotations

import shutil
from collections.abc import Iterable

import pandas as pd

from src.storage.data_registry import DataRegistry
from src.storage.dataset_catalog import DATASET_CATALOG
from src.storage.parquet_store import ParquetStore
from src.utils.logging import logger


def dataset_partition_values(store: ParquetStore, dataset_id: str) -> tuple[str, ...]:
    return store.list_dataset_partitions(dataset_id)


def read_partition_or_empty(
    store: ParquetStore,
    dataset_id: str,
    partition_value: str,
) -> pd.DataFrame:
    definition = DATASET_CATALOG[dataset_id]
    partition_column = definition.partition_column
    if partition_column is None:
        return store.read_dataset(dataset_id)
    if partition_value not in store.list_dataset_partitions(dataset_id):
        return store.empty_dataset_frame(dataset_id)
    return store.read_dataset(dataset_id, {partition_column: partition_value})


def read_latest_or_empty(store: ParquetStore, dataset_id: str) -> pd.DataFrame:
    return store.read_latest_dataset(dataset_id)


def safe_remove_derived_dataset_dir(store: ParquetStore, dataset_id: str) -> None:
    definition = DATASET_CATALOG[dataset_id]
    if definition.source != "derived":
        raise ValueError(f"Refusing to remove non-derived dataset directory: {dataset_id}")
    dataset_dir = store.parquet_dir / dataset_id
    if dataset_dir.exists():
        shutil.rmtree(dataset_dir)
    dataset_dir.mkdir(parents=True, exist_ok=True)


def refresh_derived_registry(store: ParquetStore, dataset_ids: Iterable[str]) -> None:
    try:
        registry = DataRegistry(root=store.root)
        registry.write_catalog()
        registry.refresh_inventory(dataset_ids, status_rows=store.read_dataset_update_status())
    except Exception as exc:  # pragma: no cover - defensive registry refresh should never fail builds.
        logger.warning("Failed to refresh derived data registry: {}", exc)
