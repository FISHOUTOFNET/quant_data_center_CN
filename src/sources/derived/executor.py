"""Bounded-concurrency partition executor and single-writer commit coordinator.

The executor is the *only* component that spawns worker threads. Each worker:

* reads source Parquet partitions (read-only),
* runs the pure transformation function,
* writes to its own independent staging directory,
* returns a :class:`PartitionBuildResult`.

The :class:`CommitCoordinator` is the *only* component that writes metadata
(manifest, journal, progress). It validates each worker result, atomically
replaces the final partition, and updates the derived manifest. This guarantees
that metadata writes are single-threaded even when partition builds run in
parallel.
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Callable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Event, RLock
from typing import Any

import pandas as pd

from src.sources.derived.common import (
    cleanup_derived_partition_staging,
    commit_derived_partition_staging,
    create_derived_partition_staging_area,
)
from src.sources.derived.journal import BuildJournal
from src.sources.derived.manifest import (
    delete_derived_partition_manifest,
    upsert_derived_partition_manifest,
)
from src.sources.derived.plan import DerivedBuildPlan, DerivedPartitionPlan
from src.sources.derived.progress import ProgressReporter
from src.storage.parquet_store import ParquetStore
from src.utils.logging import logger

DEFAULT_MAX_WORKERS = 4
MAX_WORKERS_CAP = 8


@dataclass(frozen=True)
class PartitionBuildResult:
    """Result of a single partition build (produced by a worker)."""

    security_id: str
    df: pd.DataFrame
    source_signature: str
    master_row_hash: str
    staging_root: Path
    staging_partition_dir: Path
    final_partition_dir: Path
    row_count: int


MaterializeFn = Callable[[pd.Series, Mapping[str, pd.DataFrame]], pd.DataFrame]
SourceReadFn = Callable[[ParquetStore, str, str], pd.DataFrame]
SecurityLookupFn = Callable[[str], pd.Series | None]


class PartitionExecutor:
    """Run pure partition transformations with bounded thread concurrency.

    Each worker:
    1. Looks up the security master row via ``security_lookup``.
    2. Reads source Parquet frames (read-only) via ``source_read_fn``.
    3. Calls ``materialize_fn(security, source_frames)`` — a pure transform.
    4. Writes the output to its own independent staging directory.
    5. Returns a :class:`PartitionBuildResult`.

    Workers never write metadata, never update manifests, and never touch shared
    state. The :class:`CommitCoordinator` is the sole metadata writer.

    Each worker creates its own :class:`ParquetStore` instance for writing to
    avoid sharing the DuckDB connection across threads.
    """

    def __init__(
        self,
        *,
        store: ParquetStore,
        dataset_id: str,
        materialize_fn: MaterializeFn,
        source_read_fn: SourceReadFn,
        security_lookup: SecurityLookupFn,
        max_workers: int = DEFAULT_MAX_WORKERS,
        updated_at: datetime | None = None,
    ) -> None:
        self._store = store
        self._dataset_id = dataset_id
        self._materialize_fn = materialize_fn
        self._source_read_fn = source_read_fn
        self._security_lookup = security_lookup
        self._max_workers = max(1, min(max_workers, MAX_WORKERS_CAP))
        self._updated_at = updated_at or datetime.now()

    def execute(
        self,
        plan: DerivedBuildPlan,
        *,
        progress: ProgressReporter | None = None,
        journal: BuildJournal | None = None,
        cancel_event: Event | None = None,
    ) -> list[PartitionBuildResult | None]:
        """Submit all partition builds to a thread pool and collect results.

        Returns a list of results (or ``None`` for failed/skipped partitions)
        in the same order as ``plan.partitions``.
        """

        futures: list[Future[PartitionBuildResult | None]] = []
        results: list[PartitionBuildResult | None] = []
        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            for item in plan.partitions:
                if cancel_event is not None and cancel_event.is_set():
                    break
                if journal is not None and journal.is_completed(item.security_id):
                    if progress is not None:
                        progress.record_processed(item.security_id)
                    results.append(None)
                    continue
                futures.append(pool.submit(self._build_one, item, progress, cancel_event))
            for future in futures:
                results.append(future.result())
        return results

    def _build_one(
        self,
        item: DerivedPartitionPlan,
        progress: ProgressReporter | None,
        cancel_event: Event | None,
    ) -> PartitionBuildResult | None:
        if cancel_event is not None and cancel_event.is_set():
            return None
        if item.change_reason.value == "manifest_missing":
            logger.warning(
                "Skipping partition build for {} (manifest missing after preflight)",
                item.security_id,
            )
            if progress is not None:
                progress.record_failed(item.security_id)
            return None
        # ``staging`` is tracked outside the try block so the exception handler
        # can clean it up. A leaked staging directory would be reclaimed later
        # by ``cleanup_stale_derived_staging`` (1-hour age threshold), but we
        # prefer to clean up eagerly when the worker knows it failed.
        staging: Any = None
        try:
            security = self._security_lookup(item.security_id)
            if security is None:
                logger.warning(
                    "Worker could not find security master row for {}",
                    item.security_id,
                )
                if progress is not None:
                    progress.record_failed(item.security_id)
                return None
            source_frames = self._read_source_frames(item)
            df = self._materialize_fn(security, source_frames)
            staging = create_derived_partition_staging_area(
                self._store, self._dataset_id, item.security_id
            )
            # Each worker creates its own ParquetStore for writing to avoid
            # sharing the DuckDB connection across threads. The metadata_dir
            # is shared but workers do not write metadata (only the coordinator
            # does). The parquet_dir points to the staging root so writes land
            # in the worker's independent staging area.
            write_store = ParquetStore(
                root=self._store.root,
                parquet_dir=staging.staging_root,
                metadata_dir=self._store.metadata_dir,
            )
            if df.empty:
                cleanup_derived_partition_staging(staging)
                return PartitionBuildResult(
                    security_id=item.security_id,
                    df=df,
                    source_signature=item.source_signature,
                    master_row_hash=item.master_row_hash,
                    staging_root=staging.staging_root,
                    staging_partition_dir=staging.staging_partition_dir,
                    final_partition_dir=staging.final_partition_dir,
                    row_count=0,
                )
            result = write_store.write_dataset(
                self._dataset_id,
                df,
                partition={"security_id": item.security_id},
                mode="replace",
            )
            return PartitionBuildResult(
                security_id=item.security_id,
                df=df,
                source_signature=item.source_signature,
                master_row_hash=item.master_row_hash,
                staging_root=staging.staging_root,
                staging_partition_dir=staging.staging_partition_dir,
                final_partition_dir=staging.final_partition_dir,
                row_count=result.row_count,
            )
        except Exception:
            logger.exception(
                "Worker failed to build partition security_id={}",
                item.security_id,
            )
            if staging is not None:
                # Best-effort cleanup; never let cleanup failures mask the
                # original exception. The staging directory is independent
                # (per-worker UUID) so removal is safe.
                try:
                    cleanup_derived_partition_staging(staging)
                except Exception:
                    logger.warning(
                        "Worker failed to clean up staging for {} (will be reclaimed later)",
                        item.security_id,
                    )
            if progress is not None:
                progress.record_failed(item.security_id)
            return None

    def _read_source_frames(self, item: DerivedPartitionPlan) -> dict[str, pd.DataFrame]:
        """Read source frames keyed by ``dataset_id``.

        The materialize function maps each dataset_id to its adjustment type
        (unadjusted/qfq/hfq) using the source dataset catalog, so we key by
        dataset_id rather than ``dataset_id::partition_value``.
        """

        frames: dict[str, pd.DataFrame] = {}
        for dataset_id, partition_value in item.source_partitions:
            if dataset_id not in frames:
                frames[dataset_id] = self._source_read_fn(
                    self._store, dataset_id, partition_value
                )
        return frames


class CommitCoordinator:
    """Single-writer metadata committer. Processes worker results sequentially.

    The coordinator is the *only* component that:
    - Validates worker results.
    - Atomically replaces the final partition.
    - Updates the derived manifest.
    - Updates the journal and progress.

    This guarantees that metadata writes are single-threaded even when
    partition builds run in parallel.
    """

    def __init__(
        self,
        *,
        store: ParquetStore,
        dataset_id: str,
        progress: ProgressReporter | None = None,
        journal: BuildJournal | None = None,
    ) -> None:
        self._store = store
        self._dataset_id = dataset_id
        self._progress = progress
        self._journal = journal
        self._lock = RLock()

    def commit(self, result: PartitionBuildResult | None) -> bool:
        """Atomically commit one partition. Returns True on success."""

        if result is None:
            return False
        with self._lock:
            try:
                if result.row_count == 0:
                    if result.final_partition_dir.exists():
                        shutil.rmtree(result.final_partition_dir, ignore_errors=True)
                    delete_derived_partition_manifest(
                        self._store, self._dataset_id, result.security_id
                    )
                else:
                    from src.sources.derived.common import DerivedPartitionStagingArea

                    staging = DerivedPartitionStagingArea(
                        dataset_id=self._dataset_id,
                        partition_column="security_id",
                        partition_value=result.security_id,
                        staging_root=result.staging_root,
                        staging_partition_dir=result.staging_partition_dir,
                        final_partition_dir=result.final_partition_dir,
                        backup_dir=result.staging_root.parent
                        / f"{self._dataset_id}.{result.security_id}.backup",
                        final_existed=result.final_partition_dir.exists(),
                    )
                    commit_derived_partition_staging(staging)
                    upsert_derived_partition_manifest(
                        self._store,
                        self._dataset_id,
                        result.security_id,
                        result.df,
                        result.source_signature,
                        result.master_row_hash,
                    )
                if self._journal is not None:
                    self._journal.record_completed(result.security_id)
                if self._progress is not None:
                    self._progress.record_processed(result.security_id)
                return True
            except Exception as exc:
                logger.exception(
                    "CommitCoordinator failed to commit partition security_id={}",
                    result.security_id,
                )
                if self._journal is not None:
                    self._journal.record_failed(
                        result.security_id, f"{type(exc).__name__}: {exc}"
                    )
                if self._progress is not None:
                    self._progress.record_failed(result.security_id)
                return False

    def commit_all(self, results: list[PartitionBuildResult | None]) -> tuple[int, int]:
        """Commit all results with batched manifest upserts.

        Phase 1 (per-partition): atomic replacement of the final partition file.
        Phase 2 (batch): a single DuckDB call to upsert all manifest rows.
        Phase 3 (per-partition): journal and progress updates.

        This eliminates the per-partition DuckDB connect/close overhead (each
        ``upsert_dataset_partition_manifest`` call opens a new connection). With
        100 partitions, that's 100 → 1 DuckDB round-trips for the manifest
        upsert phase.

        Failure semantics:
        - Atomic replacement failure → partition marked failed, not in batch.
        - Batch manifest upsert failure → all pending partitions marked failed
          (data is committed but manifest missing; planner will rebuild on resume).
        - Journal/progress failure → warning logged, does not undo the commit.
        """

        from src.sources.derived.manifest import (
            ManifestBatchEntry,
            batch_upsert_derived_partition_manifests,
        )

        committed = 0
        failed = 0
        # Phase 1: atomic replacement + collect manifest entries.
        pending_manifest_entries: list[ManifestBatchEntry] = []
        pending_security_ids: list[str] = []
        deleted_security_ids: list[str] = []
        for result in results:
            if result is None:
                failed += 1
                continue
            try:
                with self._lock:
                    if result.row_count == 0:
                        if result.final_partition_dir.exists():
                            shutil.rmtree(result.final_partition_dir, ignore_errors=True)
                        deleted_security_ids.append(result.security_id)
                    else:
                        from src.sources.derived.common import DerivedPartitionStagingArea

                        staging = DerivedPartitionStagingArea(
                            dataset_id=self._dataset_id,
                            partition_column="security_id",
                            partition_value=result.security_id,
                            staging_root=result.staging_root,
                            staging_partition_dir=result.staging_partition_dir,
                            final_partition_dir=result.final_partition_dir,
                            backup_dir=result.staging_root.parent
                            / f"{self._dataset_id}.{result.security_id}.backup",
                            final_existed=result.final_partition_dir.exists(),
                        )
                        commit_derived_partition_staging(staging)
                    pending_manifest_entries.append(
                        ManifestBatchEntry(
                            security_id=result.security_id,
                            df=result.df,
                            source_signature_value=result.source_signature,
                            master_row_hash_value=result.master_row_hash,
                        )
                    )
                    pending_security_ids.append(result.security_id)
            except Exception as exc:
                logger.exception(
                    "CommitCoordinator: atomic replacement failed for {}",
                    result.security_id,
                )
                if self._journal is not None:
                    self._journal.record_failed(
                        result.security_id, f"{type(exc).__name__}: {exc}"
                    )
                if self._progress is not None:
                    self._progress.record_failed(result.security_id)
                failed += 1

        # Phase 1b: batch delete manifests for empty partitions.
        for security_id in deleted_security_ids:
            try:
                delete_derived_partition_manifest(
                    self._store, self._dataset_id, security_id
                )
            except Exception:
                logger.warning(
                    "CommitCoordinator: failed to delete manifest for {}",
                    security_id,
                    exc_info=True,
                )

        # Phase 2: batch manifest upsert (single DuckDB connection).
        batch_failed = False
        if pending_manifest_entries:
            try:
                batch_upsert_derived_partition_manifests(
                    self._store,
                    self._dataset_id,
                    pending_manifest_entries,
                )
            except Exception as exc:
                logger.exception(
                    "CommitCoordinator: batch manifest upsert failed for {} partitions",
                    len(pending_manifest_entries),
                )
                batch_failed = True
                # All pending partitions have committed data but missing manifests.
                # Mark them as failed so the planner rebuilds them on resume.
                for security_id in pending_security_ids:
                    if self._journal is not None:
                        self._journal.record_failed(
                            security_id,
                            f"batch_manifest_upsert_failed: {type(exc).__name__}: {exc}",
                        )
                    if self._progress is not None:
                        self._progress.record_failed(security_id)
                    failed += 1

        # Phase 3: journal and progress updates for successful commits.
        if not batch_failed:
            for security_id in pending_security_ids:
                if self._journal is not None:
                    self._journal.record_completed(security_id)
                if self._progress is not None:
                    self._progress.record_processed(security_id)
                committed += 1

        return committed, failed
