"""Bounded-concurrency partition executor and streaming commit coordinator.

Architecture:

    BuildRunContext
        ├── BuildPlanner
        └── StreamingBuildCoordinator
                ├── BoundedPartitionExecutor (worker threads)
                ├── PartitionCommitter (ManifestWriteSession + PartitionPromotion)
                ├── BuildJournal
                └── ProgressReporter

Key invariants:

* The executor submits at most ``max_workers * 2`` futures at any time
  (bounded sliding window via ``concurrent.futures.wait(FIRST_COMPLETED)``).
* Each worker computes the manifest fields (content hash, semantic hash, ...)
  while the DataFrame is still in memory, writes the staging Parquet, then
  drops the DataFrame reference before returning. The committer never needs
  to re-read the Parquet to build the manifest row.
* The coordinator commits each partition immediately when its future
  completes (no "commit all at the end" batch). This keeps the failure window
  to a single partition.
* A single :class:`ManifestWriteSession` (one DuckDB connection) is used for
  the entire build.
* Heartbeat is called every ``heartbeat_interval`` seconds in the main loop,
  independent of whether any partition completed.
"""

from __future__ import annotations

import os
import shutil
import time
from collections.abc import Callable, Mapping
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from threading import Event
from typing import Any

import pandas as pd

from src.sources.derived.common import (
    DerivedPartitionStagingArea,
    cleanup_derived_partition_staging,
    create_derived_partition_staging_area,
)
from src.sources.derived.config import (
    DEFAULT_HEARTBEAT_SECONDS,
    DEFAULT_MAX_IN_FLIGHT_MULTIPLIER,
    DEFAULT_MAX_WORKERS,
    MAX_WORKERS_RANGE,
)
from src.sources.derived.journal import BuildJournal
from src.sources.derived.plan import ChangeReason, DerivedBuildPlan, DerivedPartitionPlan
from src.sources.derived.progress import ProgressReporter
from src.storage.metadata_store import ManifestWriteSession
from src.storage.parquet_store import ParquetStore
from src.storage.partition_manifest import (
    dataframe_content_hash,
    dataframe_semantic_hash,
    date_range,
    schema_hash,
)
from src.utils.logging import logger

# Defaults are sourced from :mod:`src.sources.derived.config` so the executor,
# the streaming coordinator, and the orchestrator all share one set of values.
MAX_WORKERS_CAP = MAX_WORKERS_RANGE[1]
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = DEFAULT_HEARTBEAT_SECONDS


# ---------------------------------------------------------------------------
# Aggregate counters (no list of full results)
# ---------------------------------------------------------------------------


@dataclass
class BuildCounters:
    """Aggregate build counters. The coordinator returns this — not a list."""

    planned: int = 0
    committed: int = 0
    failed: int = 0
    skipped: int = 0
    deleted: int = 0
    rows: int = 0
    cancelled: bool = False
    max_in_flight_observed: int = 0


# ---------------------------------------------------------------------------
# Compact worker result types (no full DataFrame)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PreparedPartitionManifest:
    """Manifest fields computed by the worker while the DataFrame is in memory.

    The committer supplements these with final-path stat (size, mtime) and
    run metadata (run_id, writer_pid, ...) after promotion.
    """

    row_count: int
    min_date: str
    max_date: str
    content_hash: str
    semantic_hash: str
    schema_hash_value: str
    source_signature: str
    master_row_hash: str


@dataclass(frozen=True)
class StagedPartitionResult:
    """Compact result returned by a worker after staging write.

    The DataFrame has already been released; the committer only needs the
    staging paths and the prepared manifest fields.
    """

    security_id: str
    staging: DerivedPartitionStagingArea
    manifest: PreparedPartitionManifest | None
    delete_partition: bool


MaterializeFn = Callable[[pd.Series, Mapping[str, pd.DataFrame]], pd.DataFrame]
SourceReadFn = Callable[[ParquetStore, str, str], pd.DataFrame]
SecurityLookupFn = Callable[[str], pd.Series | None]


# ---------------------------------------------------------------------------
# Partition promotion (atomic swap + rollback)
# ---------------------------------------------------------------------------


class PromotionState(Enum):
    """States in the partition-promotion state machine.

    Transitions::

        INITIAL → BACKUP_CREATED → FINAL_PROMOTED → FINALIZED
        INITIAL → FINAL_PROMOTED → FINALIZED          (no prior final)
        any *   → ROLLED_BACK                          (on failure)

    ``promote()`` performs internal rollback on any intermediate failure, so
    a caller seeing ``PartitionPromotionRecoveryError`` knows the partition
    is in an indeterminate state that must be reported as unrecoverable.
    """

    INITIAL = "initial"
    BACKUP_CREATED = "backup_created"
    FINAL_PROMOTED = "final_promoted"
    FINALIZED = "finalized"
    ROLLED_BACK = "rolled_back"


class PartitionPromotionRecoveryError(RuntimeError):
    """Raised when rollback fails after a partial promotion.

    The partition is in an indeterminate state: neither the old final nor the
    new final is guaranteed to be consistent. The original exception that
    triggered the rollback is chained via ``__cause__``. Callers must record
    the partition as failed and must NOT mark the journal as completed.
    """


class PartitionPromotion:
    """Atomic single-partition promotion with state-machine rollback.

    Encapsulates the file-swap protocol so that file replacement and manifest
    update are coupled: either both succeed or the file state is restored.

    State machine (see :class:`PromotionState`):

    * ``promote()`` performs ``final → backup`` (if final exists) then
      ``staging → final`` (non-delete) or nothing (delete). Any failure
      during these steps triggers :meth:`_rollback_partial_promotion`
      *inside* ``promote()``, so a caller that catches the re-raised
      exception knows the file state was restored (or that a
      :class:`PartitionPromotionRecoveryError` was raised because rollback
      also failed).
    * ``rollback()`` is the *external* rollback called by the coordinator
      when the manifest write fails *after* ``promote()`` succeeded. It
      restores the pre-promotion file state.
    * ``finalize()`` removes backup and staging directories after the
      manifest write succeeds. Backup-cleanup failure is a warning (data is
      consistent); it does NOT raise.

    Replace-existing flow:
        old final → backup; staging → final; manifest upsert.
        On failure: delete new final, backup → final.

    New-partition flow:
        staging → final; manifest upsert.
        On failure: delete new final.

    Delete-partition flow:
        final → backup; manifest delete.
        On failure: backup → final.
    """

    def __init__(
        self,
        *,
        staging: DerivedPartitionStagingArea,
        delete_partition: bool = False,
    ) -> None:
        self._staging = staging
        self._delete_partition = delete_partition
        self._state = PromotionState.INITIAL
        self._backup_created = False

    @property
    def state(self) -> PromotionState:
        return self._state

    @property
    def promoted(self) -> bool:
        return self._state in (PromotionState.FINAL_PROMOTED, PromotionState.FINALIZED)

    @property
    def backup_created(self) -> bool:
        return self._backup_created

    def promote(self) -> None:
        """Perform the file swap (staging → final, or final → backup for delete).

        On any intermediate failure, :meth:`_rollback_partial_promotion` is
        called *inside* this method to restore the pre-promotion file state.
        If rollback also fails, :class:`PartitionPromotionRecoveryError` is
        raised (chained from the rollback error); otherwise the original
        promotion exception is re-raised.
        """

        area = self._staging
        try:
            area.backup_dir.parent.mkdir(parents=True, exist_ok=True)
            area.final_partition_dir.parent.mkdir(parents=True, exist_ok=True)
            if area.final_partition_dir.exists():
                area.final_partition_dir.rename(area.backup_dir)
                self._backup_created = True
                self._state = PromotionState.BACKUP_CREATED
            if not self._delete_partition:
                area.staging_partition_dir.rename(area.final_partition_dir)
            self._state = PromotionState.FINAL_PROMOTED
        except Exception:
            try:
                self._rollback_partial_promotion()
            except Exception as rollback_exc:
                raise PartitionPromotionRecoveryError(
                    f"PartitionPromotion promote failed and rollback also failed "
                    f"for partition={area.partition_value}; "
                    f"state={self._state}, rollback_error={rollback_exc}"
                ) from rollback_exc
            raise

    def rollback(self) -> None:
        """External rollback: restore pre-promotion file state.

        Called by the coordinator when the manifest write fails *after*
        ``promote()`` succeeded. If the partition was already finalized or
        rolled back, this is a no-op. If rollback itself fails,
        :class:`PartitionPromotionRecoveryError` is raised.
        """

        if self._state in (PromotionState.FINALIZED, PromotionState.ROLLED_BACK):
            return
        try:
            self._rollback_partial_promotion()
        except Exception as rollback_exc:
            raise PartitionPromotionRecoveryError(
                f"PartitionPromotion rollback failed for partition={self._staging.partition_value}: {rollback_exc}"
            ) from rollback_exc

    def _rollback_partial_promotion(self) -> None:
        """Restore pre-promotion file state based on the current state.

        This is the single internal rollback routine used by both
        :meth:`promote` (on intermediate failure) and :meth:`rollback`
        (external, on manifest-write failure). It must never raise silently:
        any failure propagates so the caller can wrap it in
        :class:`PartitionPromotionRecoveryError`.
        """

        area = self._staging
        if self._state == PromotionState.INITIAL:
            # Nothing happened yet; nothing to undo.
            return
        if self._state == PromotionState.BACKUP_CREATED:
            # final → backup succeeded, but staging → final (or delete) failed.
            # Restore backup → final. If final somehow exists, we have a
            # conflict — do NOT destroy it; leave backup in place for forensic
            # inspection and raise.
            if area.final_partition_dir.exists():
                raise RuntimeError(
                    f"Cannot rollback BACKUP_CREATED: final still exists at "
                    f"{area.final_partition_dir} (backup at {area.backup_dir})"
                )
            if area.backup_dir.exists():
                area.backup_dir.rename(area.final_partition_dir)
            self._backup_created = False
            self._state = PromotionState.ROLLED_BACK
            return
        if self._state == PromotionState.FINAL_PROMOTED:
            if self._delete_partition:
                # final → backup done (delete mode). Restore backup → final.
                if self._backup_created:
                    if area.final_partition_dir.exists():
                        # Unexpected: final exists in delete mode after promotion.
                        raise RuntimeError(
                            f"Cannot rollback delete promotion: final reappeared at {area.final_partition_dir}"
                        )
                    if area.backup_dir.exists():
                        area.backup_dir.rename(area.final_partition_dir)
                    self._backup_created = False
            else:
                if self._backup_created:
                    # Replace path: final→backup then staging→final done.
                    # Remove new final, restore backup → final.
                    if area.final_partition_dir.exists():
                        shutil.rmtree(area.final_partition_dir, ignore_errors=False)
                    if area.backup_dir.exists():
                        area.backup_dir.rename(area.final_partition_dir)
                    self._backup_created = False
                else:
                    # Insert path: staging→final done (no prior final).
                    # Remove new final.
                    if area.final_partition_dir.exists():
                        shutil.rmtree(area.final_partition_dir, ignore_errors=False)
            self._state = PromotionState.ROLLED_BACK
            return
        # FINALIZED or ROLLED_BACK: nothing to do.

    def finalize(self) -> None:
        """Clean up backup and staging after a successful commit.

        Backup-cleanup failure is a warning (data is already consistent);
        it does NOT raise. Only call this after the manifest write succeeds.
        """

        if self._state != PromotionState.FINAL_PROMOTED:
            # Cannot finalize from a non-promoted state.
            return
        area = self._staging
        if self._backup_created and area.backup_dir.exists():
            try:
                shutil.rmtree(area.backup_dir)
            except OSError as exc:
                # Backup cleanup failure must NOT be silent — the data is
                # consistent, but leftover backup dirs waste disk. Log as a
                # warning so it is observable.
                logger.warning(
                    "PartitionPromotion: failed to clean up backup {} for {}: {}",
                    area.backup_dir,
                    area.partition_value,
                    exc,
                )
        if area.staging_root.exists():
            try:
                shutil.rmtree(area.staging_root)
            except OSError as exc:
                logger.warning(
                    "PartitionPromotion: failed to clean up staging {} for {}: {}",
                    area.staging_root,
                    area.partition_value,
                    exc,
                )
        self._state = PromotionState.FINALIZED


# ---------------------------------------------------------------------------
# Bounded partition executor (worker threads)
# ---------------------------------------------------------------------------


class PartitionExecutor:
    """Run pure partition transformations with bounded thread concurrency.

    Each worker:
    1. Looks up the security master row via ``security_lookup``.
    2. Reads source Parquet frames (read-only) via ``source_read_fn``.
    3. Calls ``materialize_fn(security, source_frames)`` — a pure transform.
    4. Computes manifest hash fields while the DataFrame is in memory.
    5. Writes the output to its own independent staging directory.
    6. Drops the DataFrame reference and returns a compact result.

    Workers never write metadata, never update manifests, and never touch shared
    state. The :class:`StreamingBuildCoordinator` is the sole metadata writer.
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

    @property
    def max_workers(self) -> int:
        return self._max_workers

    def build_staged(
        self,
        item: DerivedPartitionPlan,
        *,
        cancel_event: Event | None = None,
    ) -> StagedPartitionResult | None:
        """Build one partition and return a compact staged result (no DataFrame).

        This is the worker entry point used by the streaming coordinator. The
        DataFrame is released before returning.
        """

        return self._build_one_staged(item, cancel_event)

    def _build_one_staged(
        self,
        item: DerivedPartitionPlan,
        cancel_event: Event | None,
    ) -> StagedPartitionResult | None:
        """Worker path that computes manifest fields and drops the DataFrame."""

        if cancel_event is not None and cancel_event.is_set():
            return None
        if _is_manifest_missing_reason(item.change_reason):
            logger.warning(
                "Skipping partition build for {} (source manifest missing after preflight)",
                item.security_id,
            )
            return None
        staging: Any = None
        try:
            security = self._security_lookup(item.security_id)
            if security is None:
                logger.warning("Worker could not find security master row for {}", item.security_id)
                return None
            source_frames = self._read_source_frames(item)
            df = self._materialize_fn(security, source_frames)
            staging = create_derived_partition_staging_area(self._store, self._dataset_id, item.security_id)
            write_store = ParquetStore(
                root=self._store.root,
                parquet_dir=staging.staging_root,
                metadata_dir=self._store.metadata_dir,
            )
            delete_partition = df.empty
            if delete_partition:
                cleanup_derived_partition_staging(staging)
                manifest = PreparedPartitionManifest(
                    row_count=0,
                    min_date="",
                    max_date="",
                    content_hash="",
                    semantic_hash="",
                    schema_hash_value="",
                    source_signature=item.source_signature,
                    master_row_hash=item.master_row_hash,
                )
            else:
                write_result = write_store.write_dataset(
                    self._dataset_id,
                    df,
                    partition={"security_id": item.security_id},
                    mode="replace",
                )
                manifest = self._prepare_manifest(df, item, write_result.row_count, staging.staging_partition_dir)
            # Release the DataFrame reference before returning.
            del df
            return StagedPartitionResult(
                security_id=item.security_id,
                staging=staging,
                manifest=manifest,
                delete_partition=delete_partition,
            )
        except Exception:
            logger.exception("Worker failed to build partition security_id={}", item.security_id)
            if staging is not None:
                try:
                    cleanup_derived_partition_staging(staging)
                except Exception:
                    logger.warning("Worker failed to clean up staging for {}", item.security_id)
            return None

    def _prepare_manifest(
        self,
        df: pd.DataFrame,
        item: DerivedPartitionPlan,
        row_count: int,
        staging_partition_dir: Path,
    ) -> PreparedPartitionManifest:
        """Compute manifest hash fields while the DataFrame is in memory."""

        from src.sources.derived.common import _require_derived_dataset

        definition = _require_derived_dataset(self._dataset_id)
        schema = definition.schema
        min_date, max_date = date_range(df)
        return PreparedPartitionManifest(
            row_count=row_count,
            min_date=min_date,
            max_date=max_date,
            content_hash=dataframe_content_hash(df, schema),
            semantic_hash=dataframe_semantic_hash(df, schema, self._dataset_id),
            schema_hash_value=schema_hash(schema),
            source_signature=item.source_signature,
            master_row_hash=item.master_row_hash,
        )

    def _read_source_frames(self, item: DerivedPartitionPlan) -> dict[str, pd.DataFrame]:
        frames: dict[str, pd.DataFrame] = {}
        for dataset_id, partition_value in item.source_partitions:
            if dataset_id not in frames:
                frames[dataset_id] = self._source_read_fn(self._store, dataset_id, partition_value)
        return frames


def _is_manifest_missing_reason(reason: ChangeReason) -> bool:
    """Return True for source-manifest-missing (unrecoverable, must fail)."""

    return reason in {ChangeReason.SOURCE_MANIFEST_MISSING, ChangeReason.MANIFEST_MISSING}


# ---------------------------------------------------------------------------
# Streaming build coordinator (production path)
# ---------------------------------------------------------------------------


class StreamingBuildCoordinator:
    """Bounded execution + per-partition commit in a single streaming loop.

    This is the production execution path. It:
    * submits at most ``max_workers * 2`` futures (bounded sliding window),
    * commits each partition immediately when its future completes,
    * uses one :class:`ManifestWriteSession` for the entire build,
    * calls heartbeat every ``heartbeat_interval`` seconds,
    * supports cooperative cancellation (stops submitting, lets in-flight
      finish, commits completed, sets CANCELLING stage),
    * returns :class:`BuildCounters` (not a list of results).
    """

    def __init__(
        self,
        *,
        executor: PartitionExecutor,
        store: ParquetStore,
        dataset_id: str,
        progress: ProgressReporter | None = None,
        journal: BuildJournal | None = None,
        cancel_event: Event | None = None,
        heartbeat_interval_seconds: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        max_in_flight_multiplier: int = DEFAULT_MAX_IN_FLIGHT_MULTIPLIER,
    ) -> None:
        self._executor = executor
        self._store = store
        self._dataset_id = dataset_id
        self._progress = progress
        self._journal = journal
        self._cancel_event = cancel_event or Event()
        self._heartbeat_interval = heartbeat_interval_seconds
        self._max_in_flight = executor.max_workers * max_in_flight_multiplier
        self._last_heartbeat = time.monotonic()

    def run(self, plan: DerivedBuildPlan) -> BuildCounters:
        """Run the bounded streaming build and return aggregate counters."""

        counters = BuildCounters(planned=len(plan.partitions))
        if not plan.partitions:
            return counters
        pending = iter(plan.partitions)
        in_flight: dict[Future[StagedPartitionResult | None], DerivedPartitionPlan] = {}
        with ThreadPoolExecutor(max_workers=self._executor.max_workers) as pool:
            self._submit_until_full(pool, pending, in_flight, plan)
            while in_flight:
                done, _ = wait(
                    in_flight,
                    timeout=self._heartbeat_interval,
                    return_when=FIRST_COMPLETED,
                )
                if not done:
                    self._heartbeat()
                    continue
                for future in done:
                    item = in_flight.pop(future)
                    try:
                        result = future.result()
                    except Exception:
                        result = None
                    self._commit_one(result, item, counters)
                    del result
                    if len(in_flight) > counters.max_in_flight_observed:
                        counters.max_in_flight_observed = len(in_flight)
                    self._submit_until_full(pool, pending, in_flight, plan)
        # After the loop, if cancellation was requested, commit nothing more
        # (in-flight already drained above).
        if self._cancel_event.is_set():
            counters.cancelled = True
        return counters

    def _submit_until_full(
        self,
        pool: ThreadPoolExecutor,
        pending: Any,
        in_flight: dict[Future[StagedPartitionResult | None], DerivedPartitionPlan],
        plan: DerivedBuildPlan,
    ) -> None:
        while len(in_flight) < self._max_in_flight:
            if self._cancel_event.is_set():
                return
            try:
                item = next(pending)
            except StopIteration:
                return
            if self._journal is not None and self._journal.is_completed(item.security_id):
                if self._progress is not None:
                    self._progress.record_processed(item.security_id)
                continue
            future = pool.submit(self._executor.build_staged, item, cancel_event=self._cancel_event)
            in_flight[future] = item

    def _commit_one(
        self,
        result: StagedPartitionResult | None,
        item: DerivedPartitionPlan,
        counters: BuildCounters,
    ) -> None:
        if result is None:
            if _is_manifest_missing_reason(item.change_reason):
                self._record_failed(item.security_id, "source_manifest_missing")
            else:
                self._record_failed(item.security_id, "worker_returned_none")
            counters.failed += 1
            return
        try:
            self._commit_partition(result)
        except Exception as exc:
            logger.exception("StreamingBuildCoordinator: commit failed for {}", result.security_id)
            self._record_failed(result.security_id, f"{type(exc).__name__}: {exc}")
            counters.failed += 1
            return
        if result.delete_partition:
            counters.deleted += 1
        else:
            counters.committed += 1
            if result.manifest is not None:
                counters.rows += result.manifest.row_count
        if self._journal is not None:
            self._journal.record_completed(result.security_id)
        if self._progress is not None:
            self._progress.record_processed(result.security_id)

    def _commit_partition(self, result: StagedPartitionResult) -> None:
        """Promote staging → final, write manifest, finalize (with rollback).

        Transaction window (all three stages must succeed for the partition
        to be recorded as committed):

        1. ``promotion.promote()`` — file swap. Internal rollback on failure.
        2. ``_write_manifest()`` — manifest upsert/delete via the session.
           On failure, ``promotion.rollback()`` restores the old final.
        3. ``promotion.finalize()`` — backup/staging cleanup. Failure here is
           a warning (data is already consistent); it does NOT raise.

        If ``promote()`` itself raises ``PartitionPromotionRecoveryError``,
        the partition is in an indeterminate state — re-raise so the caller
        records it as failed (journal is NOT marked completed).
        """

        from src.storage.dataset_catalog import dataset_definition

        definition = dataset_definition(self._dataset_id)
        partition_column = definition.partition_column or "security_id"
        promotion = PartitionPromotion(
            staging=result.staging,
            delete_partition=result.delete_partition,
        )
        # Stage 1: promote (internal rollback on failure).
        promotion.promote()
        # Stage 2: manifest write (external rollback on failure).
        try:
            self._write_manifest(result, promotion, partition_column)
        except Exception:
            try:
                promotion.rollback()
            except PartitionPromotionRecoveryError:
                # Rollback failed — data may be inconsistent. Re-raise so the
                # caller records the partition as failed and the journal is
                # NOT marked completed.
                logger.error(
                    "PartitionPromotion rollback FAILED for {}: partition is in "
                    "an indeterminate state (data may be inconsistent)",
                    result.security_id,
                )
                raise
            except Exception as rollback_exc:
                logger.error(
                    "PartitionPromotion rollback FAILED for {}: {} (data may be inconsistent)",
                    result.security_id,
                    rollback_exc,
                )
                raise
            raise
        # Stage 3: finalize (backup cleanup; warning only).
        promotion.finalize()

    def _write_manifest(
        self,
        result: StagedPartitionResult,
        promotion: PartitionPromotion,
        partition_column: str,
    ) -> None:
        """Write (or delete) the manifest row via the session."""

        session = self._manifest_session
        if session is None:
            raise RuntimeError("manifest session not attached before _write_manifest")
        if result.delete_partition:
            session.delete_partition(self._dataset_id, partition_column, result.security_id)
            return
        if result.manifest is None:
            raise RuntimeError(f"commit_partition: no manifest for non-delete partition {result.security_id}")
        manifest = result.manifest
        final_path = result.staging.final_partition_dir / "data.parquet"
        if not final_path.exists():
            # Some datasets write multiple parquet files; fall back to the
            # first file in the partition directory.
            files = [p for p in result.staging.final_partition_dir.iterdir() if p.is_file()]
            if files:
                final_path = files[0]
        stat = final_path.stat()
        row = {
            "dataset": self._dataset_id,
            "partition_column": partition_column,
            "partition_value": result.security_id,
            "output_path": _relative_output_path(final_path, self._store.root),
            "row_count": manifest.row_count,
            "min_date": manifest.min_date,
            "max_date": manifest.max_date,
            "content_hash": manifest.content_hash,
            "semantic_hash": manifest.semantic_hash,
            "schema_hash": manifest.schema_hash_value,
            "source_signature": manifest.source_signature,
            "master_row_hash": manifest.master_row_hash,
            "file_size_bytes": stat.st_size,
            "file_mtime": pd.Timestamp(datetime.fromtimestamp(stat.st_mtime)).floor("ms").to_pydatetime(),
            "run_id": self._run_id,
            "writer_pid": os.getpid(),
            "writer_thread": self._writer_thread,
            "updated_at": datetime.now(),
        }
        session.upsert_partition(row)

    def _heartbeat(self) -> None:
        now = time.monotonic()
        if now - self._last_heartbeat >= self._heartbeat_interval:
            self._last_heartbeat = now
            if self._progress is not None:
                self._progress.heartbeat()
            if self._journal is not None:
                self._journal.heartbeat()

    def _record_failed(self, security_id: str, reason: str) -> None:
        if self._journal is not None:
            self._journal.record_failed(security_id, reason)
        if self._progress is not None:
            self._progress.record_failed(security_id)

    # The manifest session and run metadata are injected by the orchestrator
    # via these setters so the coordinator doesn't need to know about the
    # BuildRunContext or DuckDBMetadataStore directly.
    _manifest_session: ManifestWriteSession | None = None
    _run_id: str = ""
    _writer_thread: str = ""

    def attach_manifest_session(
        self,
        session: ManifestWriteSession,
        *,
        run_id: str,
        writer_thread: str = "",
    ) -> None:
        self._manifest_session = session
        self._run_id = run_id
        self._writer_thread = writer_thread or __import__("threading").current_thread().name


def _relative_output_path(path: Path, root: Path) -> str:
    resolved_path = path.resolve()
    resolved_root = root.resolve()
    try:
        return resolved_path.relative_to(resolved_root).as_posix()
    except ValueError:
        return resolved_path.as_posix()
