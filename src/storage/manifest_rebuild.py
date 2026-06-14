"""Local dataset partition manifest rebuild helpers."""

from __future__ import annotations

from dataclasses import dataclass

from src.storage.dataset_catalog import DATASET_CATALOG, dataset_definition
from src.storage.parquet_store import ParquetStore


@dataclass(frozen=True)
class ManifestRebuildResult:
    dataset: str
    partition_count: int
    updated_count: int
    skipped_count: int


def rebuild_partition_manifest(
    *,
    store: ParquetStore,
    dataset_id: str,
    include_derived: bool = False,
    force: bool = False,
) -> ManifestRebuildResult:
    definition = dataset_definition(dataset_id)
    if definition.source == "derived" and not include_derived:
        return ManifestRebuildResult(dataset=dataset_id, partition_count=0, updated_count=0, skipped_count=0)

    partitions = _dataset_partitions(store, dataset_id)
    existing = store.read_dataset_partition_manifest(dataset_id)
    existing_keys = set()
    if not existing.empty:
        existing_keys = set(
            zip(
                existing["partition_column"].astype("string").fillna(""),
                existing["partition_value"].astype("string").fillna(""),
                strict=False,
            )
        )

    updated = 0
    skipped = 0
    for partition in partitions:
        partition_column = definition.partition_column or ""
        partition_value = "" if partition is None else str(partition[partition_column])
        key = (partition_column, partition_value)
        if not force and key in existing_keys:
            skipped += 1
            continue
        df = store.read_dataset(dataset_id, partition)
        store.upsert_written_dataset_partition_manifest(dataset_id, df, partition)
        updated += 1

    return ManifestRebuildResult(
        dataset=dataset_id,
        partition_count=len(partitions),
        updated_count=updated,
        skipped_count=skipped,
    )


def rebuild_one_partition_manifest(
    *,
    store: ParquetStore,
    dataset_id: str,
    partition_value: str | None = None,
    force: bool = True,
) -> bool:
    definition = dataset_definition(dataset_id)
    partition = None if definition.partition_column is None else {definition.partition_column: partition_value}
    if not store.dataset_exists(dataset_id, partition):
        return False
    existing = store.read_dataset_partition_manifest(dataset_id)
    partition_column = definition.partition_column or ""
    normalized_partition_value = "" if partition is None else store._partition_value(definition, partition)
    if not force and not existing.empty:
        matched = existing.loc[
            (existing["partition_column"].astype("string") == partition_column)
            & (existing["partition_value"].astype("string") == normalized_partition_value)
        ]
        if not matched.empty:
            return False
    df = store.read_dataset(dataset_id, partition)
    store.upsert_written_dataset_partition_manifest(dataset_id, df, partition)
    return True


def rebuild_all_partition_manifests(
    *,
    store: ParquetStore,
    include_derived: bool = False,
    force: bool = False,
) -> list[ManifestRebuildResult]:
    results: list[ManifestRebuildResult] = []
    for dataset_id in sorted(DATASET_CATALOG):
        definition = DATASET_CATALOG[dataset_id]
        if definition.source == "derived" and not include_derived:
            continue
        results.append(
            rebuild_partition_manifest(
                store=store,
                dataset_id=dataset_id,
                include_derived=include_derived,
                force=force,
            )
        )
    return results


def _dataset_partitions(store: ParquetStore, dataset_id: str):
    definition = dataset_definition(dataset_id)
    if definition.partition_column is None:
        return [None] if store.dataset_exists(dataset_id) else []
    return [{definition.partition_column: value} for value in store.list_dataset_partitions(dataset_id)]
