"""Derived partition manifest helpers."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, cast

import pandas as pd

from src.storage.dataset_catalog import dataset_definition
from src.storage.manifest_rebuild import rebuild_one_partition_manifest
from src.storage.parquet_store import ParquetStore
from src.storage.partition_manifest import master_row_hash, source_signature

SourcePartition = tuple[str, str]


def source_partition_pairs_for_security(
    store: ParquetStore,
    security: pd.Series,
    dataset_ids_by_code_field: Iterable[tuple[str, str]],
) -> tuple[SourcePartition, ...]:
    pairs: list[SourcePartition] = []
    for dataset_id, code_field in dataset_ids_by_code_field:
        definition = dataset_definition(dataset_id)
        if definition.partition_column is None:
            continue
        code = _clean_string(security.get(code_field))
        if code and store.dataset_exists(dataset_id, {definition.partition_column: code}):
            pairs.append((dataset_id, code))
    return tuple(pairs)


def source_manifest_rows_for_pairs(
    store: ParquetStore,
    pairs: Iterable[SourcePartition],
    *,
    rebuild_missing: bool = True,
) -> pd.DataFrame:
    requested = list(dict.fromkeys(pairs))
    if not requested:
        return pd.DataFrame()

    manifests = []
    for dataset_id, partition_value in requested:
        definition = dataset_definition(dataset_id)
        partition_column = definition.partition_column or ""
        row = _manifest_row(store, dataset_id, partition_column, partition_value)
        if row.empty and rebuild_missing:
            rebuild_one_partition_manifest(
                store=store,
                dataset_id=dataset_id,
                partition_value=partition_value or None,
                force=True,
            )
            row = _manifest_row(store, dataset_id, partition_column, partition_value)
        if row.empty:
            raise RuntimeError(f"Missing dataset_partition_manifest for {dataset_id}/{partition_value}")
        manifests.append(row)
    return pd.concat(manifests, ignore_index=True) if manifests else pd.DataFrame()


def current_source_signature_for_security(
    store: ParquetStore,
    security: pd.Series,
    pairs: Iterable[SourcePartition],
    *,
    rebuild_missing: bool = True,
) -> tuple[str, str, pd.DataFrame]:
    source_rows = source_manifest_rows_for_pairs(store, pairs, rebuild_missing=rebuild_missing)
    master_hash = master_row_hash(security)
    return source_signature(source_rows, master_hash), master_hash, source_rows


def upsert_derived_partition_manifest(
    store: ParquetStore,
    dataset_id: str,
    security_id: str,
    df: pd.DataFrame,
    source_signature_value: str,
    master_row_hash_value: str,
) -> None:
    partition_column = dataset_definition(dataset_id).partition_column
    if partition_column is None:
        raise ValueError(f"{dataset_id} is not partitioned")
    store.upsert_written_dataset_partition_manifest(
        dataset_id,
        df,
        {partition_column: security_id},
        source_signature_value=source_signature_value,
        master_row_hash_value=master_row_hash_value,
    )


def delete_derived_partition_manifest(store: ParquetStore, dataset_id: str, security_id: str) -> None:
    definition = dataset_definition(dataset_id)
    store.delete_dataset_partition_manifest(dataset_id, definition.partition_column or "", security_id)


def cleanup_stale_derived_manifests(store: ParquetStore, dataset_id: str) -> None:
    definition = dataset_definition(dataset_id)
    partition_column = definition.partition_column or ""
    existing_partitions = set(store.list_dataset_partitions(dataset_id))
    manifests = store.read_dataset_partition_manifest(dataset_id)
    if manifests.empty:
        return
    for _, row in manifests.iterrows():
        manifest_partition_column = _clean_string(row.get("partition_column"))
        partition_value = _clean_string(row.get("partition_value"))
        if manifest_partition_column == partition_column and partition_value not in existing_partitions:
            store.delete_dataset_partition_manifest(dataset_id, partition_column, partition_value)


def _manifest_row(
    store: ParquetStore,
    dataset_id: str,
    partition_column: str,
    partition_value: str,
) -> pd.DataFrame:
    manifest = store.read_dataset_partition_manifest(dataset_id)
    if manifest.empty:
        return manifest
    return manifest.loc[
        (manifest["partition_column"].astype("string") == partition_column)
        & (manifest["partition_value"].astype("string") == partition_value)
    ].reset_index(drop=True)


def _clean_string(value: object) -> str:
    if value is None or pd.isna(cast(Any, value)):
        return ""
    return str(value).strip()
