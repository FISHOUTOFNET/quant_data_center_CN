"""Lightweight build journal for crash-safe derived builds.

The journal persists enough state to resume a derived build after a crash:

* ``run_id`` / ``target`` / ``dataset_id`` identify the build.
* ``plan_hash`` / ``schema_version`` / ``source_snapshot_hash`` guard against
  resuming a plan whose inputs have changed underneath us.
* ``completed`` / ``failed`` record per-partition outcomes so we can skip
  already-committed partitions on resume.
* ``heartbeat_at`` lets external monitors detect a stalled build.

Design constraints (from the task spec):

* Atomic writes via temp-file + ``os.replace``.
* The ``completed`` set is persisted in the MAIN JSON file (there is no
  ``.completed`` sidecar file). To avoid O(n²) write cost the JSON snapshot
  is flushed every ``flush_every`` completed partitions (default 50) or every
  ``flush_interval_seconds`` (default 30s), whichever comes first. Heartbeat
  and finalize calls always force a flush.
* Recovery validates target / schema / plan / source-snapshot before resuming.
* Already-completed partitions are re-validated (target file + manifest exist
  + file size/mtime match + output_path matches) before being skipped.
* If source data has changed, the old plan is discarded and a fresh plan is
  built.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import TYPE_CHECKING, Any

import pandas as pd

from src.utils.logging import logger

if TYPE_CHECKING:
    from src.sources.derived.plan import DerivedBuildPlan, DerivedPartitionPlan
    from src.storage.parquet_store import ParquetStore

JOURNAL_STATUS_RUNNING = "running"
JOURNAL_STATUS_COMPLETED = "completed"
JOURNAL_STATUS_FAILED = "failed"
JOURNAL_STATUS_ABANDONED = "abandoned"
JOURNAL_STATUS_CANCELLED = "cancelled"

DEFAULT_FLUSH_INTERVAL_SECONDS = 30.0
DEFAULT_FLUSH_EVERY = 50


@dataclass
class BuildJournal:
    """Crash-safe build journal persisted under ``data/metadata/derived-runs/``.

    The journal is intentionally minimal: it records what the planner decided
    to build, which partitions have been committed, and which failed. On
    resume, the planner checks the journal before re-planning; if the plan
    hash and source snapshot match, already-committed partitions are skipped
    (after re-validating that their target parquet + manifest row exist).
    """

    run_id: str
    target: str
    dataset_id: str
    plan_hash: str
    schema_version: str
    source_snapshot_hash: str
    total: int
    path: Path
    started_at: datetime = field(default_factory=datetime.now)
    heartbeat_at: datetime = field(default_factory=datetime.now)
    status: str = JOURNAL_STATUS_RUNNING
    completed: set[str] = field(default_factory=set)
    failed: dict[str, str] = field(default_factory=dict)
    _lock: RLock = field(default_factory=RLock, repr=False)
    _dirty: bool = field(default=False, repr=False)
    _last_flush_monotonic: float = field(default_factory=time.monotonic, repr=False)
    _clock: Callable[[], float] = field(default=time.monotonic, repr=False)
    _flush_interval: float = DEFAULT_FLUSH_INTERVAL_SECONDS
    _flush_every: int = DEFAULT_FLUSH_EVERY

    @classmethod
    def create(
        cls,
        *,
        run_id: str,
        target: str,
        dataset_id: str,
        plan_hash: str,
        schema_version: str,
        source_snapshot_hash: str,
        total: int,
        metadata_dir: Path,
        now: datetime | None = None,
    ) -> BuildJournal:
        """Create a new journal file on disk."""

        timestamp = now or datetime.now()
        journal_dir = metadata_dir / "derived-runs"
        journal_dir.mkdir(parents=True, exist_ok=True)
        path = journal_dir / f"{run_id}.json"
        journal = cls(
            run_id=run_id,
            target=target,
            dataset_id=dataset_id,
            plan_hash=plan_hash,
            schema_version=schema_version,
            source_snapshot_hash=source_snapshot_hash,
            total=total,
            path=path,
            started_at=timestamp,
            heartbeat_at=timestamp,
        )
        journal._write_full(force=True)
        return journal

    @classmethod
    def load(cls, path: Path) -> BuildJournal | None:
        """Load a journal from disk. Returns ``None`` if missing or corrupt."""

        if not path.exists():
            return None
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        if not isinstance(raw, dict):
            return None
        required = {"run_id", "target", "dataset_id", "plan_hash", "schema_version", "source_snapshot_hash", "total"}
        if not required.issubset(raw.keys()):
            return None
        completed_raw = raw.get("completed", [])
        failed_raw = raw.get("failed", {})
        return cls(
            run_id=str(raw["run_id"]),
            target=str(raw["target"]),
            dataset_id=str(raw["dataset_id"]),
            plan_hash=str(raw["plan_hash"]),
            schema_version=str(raw["schema_version"]),
            source_snapshot_hash=str(raw["source_snapshot_hash"]),
            total=int(raw["total"]),
            path=path,
            started_at=_parse_dt(raw.get("started_at")) or datetime.now(),
            heartbeat_at=_parse_dt(raw.get("heartbeat_at")) or datetime.now(),
            status=str(raw.get("status", JOURNAL_STATUS_RUNNING)),
            completed={str(s) for s in completed_raw if s},
            failed={str(k): str(v) for k, v in (failed_raw.items() if isinstance(failed_raw, dict) else [])},
        )

    def matches(
        self,
        *,
        target: str,
        schema_version: str,
        plan_hash: str,
        source_snapshot_hash: str,
    ) -> bool:
        """Return True if this journal's plan matches the current plan."""

        return (
            self.target == target
            and self.schema_version == schema_version
            and self.plan_hash == plan_hash
            and self.source_snapshot_hash == source_snapshot_hash
        )

    def record_completed(self, security_id: str) -> None:
        with self._lock:
            self.completed.add(security_id)
            self.failed.pop(security_id, None)
            self.heartbeat_at = datetime.now()
            self._dirty = True
        self._maybe_flush()

    def record_failed(self, security_id: str, reason: str) -> None:
        with self._lock:
            self.failed[security_id] = reason
            self.completed.discard(security_id)
            self.heartbeat_at = datetime.now()
            self._dirty = True
        self._maybe_flush()

    def heartbeat(self) -> None:
        with self._lock:
            self.heartbeat_at = datetime.now()
            self._dirty = True
        self._maybe_flush(force=True)

    def finalize(self, status: str) -> dict[str, Any]:
        with self._lock:
            self.status = status
            self.heartbeat_at = datetime.now()
            self._dirty = True
            snapshot = self._snapshot()
        self._write_full(force=True)
        return snapshot

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return self._snapshot()

    def is_completed(self, security_id: str) -> bool:
        with self._lock:
            return security_id in self.completed

    def discard_completed(self, security_id: str, *, reason: str = "") -> None:
        """Remove a partition from the completed set (resume validation failed).

        Called when :func:`validate_completed_partition` determines that a
        previously-committed partition is no longer consistent (file missing,
        manifest missing, signature mismatch, ...). The partition will be
        rebuilt on this resume.
        """

        with self._lock:
            self.completed.discard(security_id)
            self._dirty = True
        logger.info(
            "BuildJournal: discarding completed partition {} (reason: {})",
            security_id,
            reason or "unspecified",
        )
        self._maybe_flush(force=True)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _snapshot(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "target": self.target,
            "dataset_id": self.dataset_id,
            "plan_hash": self.plan_hash,
            "schema_version": self.schema_version,
            "source_snapshot_hash": self.source_snapshot_hash,
            "total": self.total,
            "completed": sorted(self.completed),
            "failed": dict(sorted(self.failed.items())),
            "started_at": self.started_at.isoformat(timespec="seconds"),
            "heartbeat_at": self.heartbeat_at.isoformat(timespec="seconds"),
            "status": self.status,
        }

    def _maybe_flush(self, *, force: bool = False) -> None:
        with self._lock:
            if not self._dirty:
                return
            now = self._clock()
            time_due = (now - self._last_flush_monotonic) >= self._flush_interval
            count_due = len(self.completed) > 0 and len(self.completed) % self._flush_every == 0
            if not force and not time_due and not count_due:
                return
            self._last_flush_monotonic = now
            self._dirty = False
            snapshot = self._snapshot()
        self._write_full_snapshot(snapshot)

    def _write_full(self, *, force: bool) -> None:
        with self._lock:
            snapshot = self._snapshot()
            self._dirty = False
        self._write_full_snapshot(snapshot)

    def _write_full_snapshot(self, snapshot: dict[str, Any]) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(
                json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            tmp.replace(self.path)
        except OSError as exc:
            logger.warning(
                "Failed to flush build journal run_id={} path={}: {}",
                self.run_id,
                self.path,
                exc,
            )


def find_resumable_journal(
    *,
    metadata_dir: Path,
    target: str,
    dataset_id: str,
    schema_version: str,
    plan_hash: str,
    source_snapshot_hash: str,
) -> BuildJournal | None:
    """Find a journal that can be resumed for the given plan.

    Scans ``metadata_dir/derived-runs/*.json`` for a journal whose plan matches.
    Only journals with status ``running`` or ``abandoned`` are candidates.
    """

    journal_dir = metadata_dir / "derived-runs"
    if not journal_dir.exists():
        return None
    candidates: list[Path] = sorted(p for p in journal_dir.glob("*.json") if p.is_file())
    for path in reversed(candidates):  # most recent first
        journal = BuildJournal.load(path)
        if journal is None:
            continue
        if journal.status not in {JOURNAL_STATUS_RUNNING, JOURNAL_STATUS_ABANDONED}:
            continue
        if journal.matches(
            target=target,
            schema_version=schema_version,
            plan_hash=plan_hash,
            source_snapshot_hash=source_snapshot_hash,
        ):
            return journal
    return None


def cleanup_old_journals(
    metadata_dir: Path,
    *,
    keep_recent: int = 20,
    now: datetime | None = None,
) -> int:
    """Remove completed/failed/abandoned journals beyond ``keep_recent``.

    Returns the number of deleted journal files. Active (``running``) journals
    are always kept.
    """

    journal_dir = metadata_dir / "derived-runs"
    if not journal_dir.exists():
        return 0
    reference = now or datetime.now()
    candidates: list[tuple[Path, datetime, str]] = []
    for path in journal_dir.glob("*.json"):
        if not path.is_file():
            continue
        journal = BuildJournal.load(path)
        if journal is None:
            # Corrupt/empty journal older than 7 days → remove.
            try:
                mtime = datetime.fromtimestamp(path.stat().st_mtime)
                if (reference - mtime).days > 7:
                    path.unlink(missing_ok=True)
            except OSError:
                pass
            continue
        if journal.status == JOURNAL_STATUS_RUNNING:
            continue
        candidates.append((path, journal.heartbeat_at, journal.status))

    candidates.sort(key=lambda item: item[1], reverse=True)
    deleted = 0
    for path, _, _ in candidates[keep_recent:]:
        try:
            path.unlink(missing_ok=True)
            deleted += 1
        except OSError:
            pass
    return deleted


def _parse_dt(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Resume validation: verify a journal "completed" partition is truly committed
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CompletedValidation:
    """Result of validating one journal-completed partition on resume.

    If validation fails, the partition must be rebuilt (removed from the
    journal's ``completed`` set).
    """

    valid: bool
    reason: str | None = None


def validate_completed_partition(
    store: ParquetStore,
    item: DerivedPartitionPlan,
    *,
    dataset_id: str,
) -> CompletedValidation:
    """Verify that a journal-completed partition is truly committed.

    Checks (all must pass for the partition to be skipped on resume):

    * The target Parquet file exists on disk.
    * A manifest row exists for ``(dataset, partition_column, partition_value)``.
    * The manifest ``source_signature`` equals the plan's ``source_signature``.
    * The manifest ``master_row_hash`` equals the plan's ``master_row_hash``.
    * The manifest ``output_path`` is non-empty, resolves inside the store
      root, and points at the current final partition file (fail-closed: any
      resolution failure → validation failure).
    * The manifest ``file_size_bytes`` matches the actual file size on disk.
    * The manifest ``file_mtime`` matches the actual file mtime (floor to
      millisecond precision to absorb filesystem time-precision differences).

    This replaces the unsafe ``security_id in journal.completed`` check that
    could skip a partition whose file was lost, replaced, or whose manifest
    write failed in the previous run's failure window. All checks fail CLOSED:
    a partition that cannot be fully validated is rebuilt, not skipped.
    """

    from src.storage.dataset_catalog import dataset_definition

    definition = dataset_definition(dataset_id)
    partition_column = definition.partition_column or "security_id"
    partition_value = item.security_id

    # 1. Target file exists.
    try:
        target_path = store.dataset_path(dataset_id, {partition_column: partition_value})
    except (KeyError, ValueError):
        return CompletedValidation(False, "could not resolve target partition path")
    if not target_path.exists():
        return CompletedValidation(False, "target parquet file missing")

    # 2. Manifest row exists.
    manifests = store.read_dataset_partition_manifest_batch([dataset_id])
    if manifests.empty:
        return CompletedValidation(False, "manifest batch empty")
    mask = (
        (manifests["dataset"].astype("string") == dataset_id)
        & (manifests["partition_column"].astype("string") == partition_column)
        & (manifests["partition_value"].astype("string") == partition_value)
    )
    matched = manifests.loc[mask]
    if matched.empty:
        return CompletedValidation(False, "manifest row missing")
    row = matched.iloc[-1]

    # 3. source_signature matches the plan.
    manifest_sig = str(row.get("source_signature") or "")
    if manifest_sig != item.source_signature:
        return CompletedValidation(False, "source_signature mismatch")

    # 4. master_row_hash matches the plan.
    manifest_master = str(row.get("master_row_hash") or "")
    if manifest_master != item.master_row_hash:
        return CompletedValidation(False, "master_row_hash mismatch")

    # 5. manifest output_path points at the current final file (fail-closed).
    #    A missing/empty/unresolvable/escaping output_path is a validation
    #    failure, NOT a best-effort pass. This prevents a tampered manifest
    #    from causing us to skip a partition whose real file was moved.
    manifest_output = str(row.get("output_path") or "")
    if not manifest_output:
        return CompletedValidation(False, "output_path missing")
    try:
        resolved_file = target_path.resolve()
        resolved_manifest = (store.root / manifest_output).resolve()
    except (OSError, ValueError) as exc:
        return CompletedValidation(False, f"output_path resolution failed: {exc}")
    # Reject output_path that escapes the store root.
    try:
        resolved_manifest.relative_to(store.root.resolve())
    except ValueError:
        return CompletedValidation(False, "output_path escapes store root")
    if resolved_file != resolved_manifest:
        return CompletedValidation(False, "output_path points elsewhere")

    # 6. file_size_bytes matches actual file size.
    try:
        actual_stat = target_path.stat()
    except OSError as exc:
        return CompletedValidation(False, f"target file stat failed: {exc}")
    manifest_size = int(row.get("file_size_bytes") or -1)
    if manifest_size != actual_stat.st_size:
        return CompletedValidation(
            False,
            f"file size mismatch: manifest={manifest_size} actual={actual_stat.st_size}",
        )

    # 7. file_mtime matches actual file mtime (floor to millisecond precision).
    #    Filesystem time precision varies (NTFS ~100ns, FAT ~2s, ext4 ~1ns);
    #    we floor both sides to milliseconds so the check is robust across
    #    platforms without admitting wide minute-level tolerance that could
    #    mask a tampered file.
    manifest_mtime_raw = row.get("file_mtime")
    actual_mtime_ms = int(actual_stat.st_mtime * 1000)
    try:
        # The manifest stores ``file_mtime`` as a naive local datetime created
        # via ``datetime.fromtimestamp(stat.st_mtime)``. We must convert it
        # back using the SAME naive-local-time semantics to avoid a timezone
        # skew. ``pd.Timestamp.timestamp()`` treats naive timestamps as UTC
        # (unlike ``datetime.timestamp()`` which treats them as local), so we
        # explicitly convert to a naive ``datetime`` before calling
        # ``.timestamp()`` to keep the round-trip consistent.
        if isinstance(manifest_mtime_raw, pd.Timestamp):
            manifest_dt = manifest_mtime_raw.to_pydatetime()
        elif isinstance(manifest_mtime_raw, datetime):
            manifest_dt = manifest_mtime_raw
        elif manifest_mtime_raw is not None:
            manifest_dt = pd.Timestamp(manifest_mtime_raw).to_pydatetime()
        else:
            return CompletedValidation(False, "file_mtime missing from manifest")
        manifest_mtime_ms = int(manifest_dt.timestamp() * 1000)
    except (TypeError, ValueError, OSError) as exc:
        return CompletedValidation(False, f"file_mtime parse failed: {exc}")
    if manifest_mtime_ms != actual_mtime_ms:
        return CompletedValidation(
            False,
            f"file mtime mismatch: manifest_ms={manifest_mtime_ms} actual_ms={actual_mtime_ms}",
        )

    return CompletedValidation(True)


def resume_filter_completed(
    store: ParquetStore,
    journal: BuildJournal,
    plan: DerivedBuildPlan,
    *,
    dataset_id: str,
) -> int:
    """Remove journal-completed partitions that fail validation.

    Returns the number of partitions removed from ``completed`` (i.e. the
    number that must be rebuilt on this resume).
    """

    removed = 0
    for item in plan.partitions:
        if not journal.is_completed(item.security_id):
            continue
        result = validate_completed_partition(store, item, dataset_id=dataset_id)
        if not result.valid:
            journal.discard_completed(item.security_id, reason=result.reason or "validation failed")
            removed += 1
    return removed
