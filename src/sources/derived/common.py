"""Shared utilities for derived dataset materializers."""

from __future__ import annotations

import shutil
import uuid
from collections.abc import Iterable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.storage.data_registry import DataRegistry
from src.storage.dataset_catalog import DATASET_CATALOG
from src.storage.parquet_store import ParquetStore
from src.utils.logging import logger
from src.utils.process_lock import ProcessLockError, acquire_process_lock


class BuildDerivedLockError(RuntimeError):
    """Raised when another build-derived process already owns the build lock."""


@dataclass(frozen=True)
class DerivedDatasetStagingArea:
    dataset_id: str
    staging_root: Path
    staging_dataset_dir: Path
    final_dir: Path
    backup_dir: Path
    final_existed: bool


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


@contextmanager
def build_derived_file_lock(
    root: Path,
    targets: tuple[str, ...],
    *,
    stale_after_seconds: int = 12 * 60 * 60,
) -> Iterator[Path]:
    """Acquire a cross-process lock for a complete build-derived run."""

    lock_dir = root.resolve() / "data" / "metadata" / "locks" / "build-derived.lock"
    try:
        with acquire_process_lock(
            lock_dir,
            lock_name="build-derived",
            purpose="build-derived",
            stale_after_seconds=stale_after_seconds,
            extra_owner={"target": list(targets)},
        ) as lock:
            yield lock.path
    except ProcessLockError as exc:
        raise BuildDerivedLockError(f"build-derived is already running; {exc}") from exc


def create_derived_dataset_staging_area(store: ParquetStore, dataset_id: str) -> DerivedDatasetStagingArea:
    definition = _require_derived_dataset(dataset_id)
    token = uuid.uuid4().hex
    staging_root = store.parquet_dir / ".staging" / f"{definition.id}.{token}"
    staging_dataset_dir = staging_root / definition.id
    final_dir = store.parquet_dir / definition.id
    backup_dir = store.parquet_dir / ".backup" / f"{definition.id}.{token}"
    staging_dataset_dir.mkdir(parents=True, exist_ok=False)
    return DerivedDatasetStagingArea(
        dataset_id=definition.id,
        staging_root=staging_root,
        staging_dataset_dir=staging_dataset_dir,
        final_dir=final_dir,
        backup_dir=backup_dir,
        final_existed=final_dir.exists(),
    )


def commit_derived_dataset_staging(area: DerivedDatasetStagingArea) -> None:
    """Move a staged derived dataset into the canonical Parquet directory."""

    _require_derived_dataset(area.dataset_id)
    area.backup_dir.parent.mkdir(parents=True, exist_ok=True)
    backup_created = False
    try:
        if area.final_dir.exists():
            area.final_dir.rename(area.backup_dir)
            backup_created = True
        area.staging_dataset_dir.rename(area.final_dir)
    except Exception as exc:
        _restore_staging_swap(area, backup_created)
        raise RuntimeError(f"Failed to promote staged derived dataset {area.dataset_id}") from exc
    finally:
        if backup_created and area.backup_dir.exists():
            shutil.rmtree(area.backup_dir, ignore_errors=True)
        if area.staging_root.exists():
            shutil.rmtree(area.staging_root, ignore_errors=True)


def cleanup_derived_dataset_staging(area: DerivedDatasetStagingArea) -> None:
    """Remove staging leftovers without touching a pre-existing canonical dataset."""

    if area.staging_root.exists():
        shutil.rmtree(area.staging_root, ignore_errors=True)
    if not area.final_existed and _is_empty_directory(area.final_dir):
        with suppress(OSError):
            area.final_dir.rmdir()


def _require_derived_dataset(dataset_id: str):
    definition = DATASET_CATALOG[dataset_id]
    if definition.source != "derived":
        raise ValueError(f"Refusing to stage non-derived dataset directory: {dataset_id}")
    return definition


def _restore_staging_swap(area: DerivedDatasetStagingArea, backup_created: bool) -> None:
    if not backup_created or not area.backup_dir.exists() or area.final_dir.exists():
        return
    with suppress(Exception):
        area.backup_dir.rename(area.final_dir)


def _is_empty_directory(path: Path) -> bool:
    if not path.is_dir():
        return False
    try:
        next(path.iterdir())
    except StopIteration:
        return True
    except OSError:
        return False
    return False


def refresh_derived_registry(store: ParquetStore, dataset_ids: Iterable[str]) -> None:
    try:
        registry = DataRegistry(root=store.root)
        registry.write_catalog()
        registry.refresh_inventory(dataset_ids, status_rows=store.read_dataset_update_status())
    except Exception as exc:  # pragma: no cover - defensive registry refresh should never fail builds.
        logger.warning("Failed to refresh derived data registry: {}", exc)
