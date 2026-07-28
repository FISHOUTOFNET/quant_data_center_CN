"""P0-5 partition promotion fault injection tests.

Each test injects a failure at one of the 8 fault points in the partition
promotion state machine (``PartitionPromotion``) and the streaming commit
path (``StreamingBuildCoordinator._commit_partition``) and verifies the
recovery invariants:

1. final -> backup rename fails          -> original final preserved
2. staging -> final rename fails         -> backup restored to final
3. new-partition staging -> final fails  -> no half-baked final left
4. manifest upsert fails                 -> rollback() restores old final
5. manifest delete fails                 -> backup restored to final
6. rollback rmtree(new final) fails      -> PartitionPromotionRecoveryError
7. rollback backup -> final rename fails -> PartitionPromotionRecoveryError
8. finalize backup cleanup fails         -> warning only, data consistent

For every fault point we verify (where applicable):

* final file exists / does not exist as expected
* final content is the old version (when applicable)
* backup / staging directory state
* manifest stays in the old state (or is updated, for fault point 8)
* journal is NOT marked completed (or IS, for fault point 8)
* counters record the partition as failed (or committed, for fault point 8)
* a subsequent run without the fault can rebuild the partition

Fault injection is done by monkeypatching ``Path.rename`` /
``shutil.rmtree`` (used by the promotion file-swap protocol) or the
``ManifestWriteSession`` methods (used by the manifest-write stage). The
worker's parquet write uses ``os.replace`` (not ``Path.rename``), so
rename patches never interfere with staging writes.
"""

from __future__ import annotations

import shutil
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pytest

from src.sources.derived.common import create_derived_partition_staging_area
from src.sources.derived.executor import (
    BuildCounters,
    PartitionExecutor,
    PartitionPromotion,
    PartitionPromotionRecoveryError,
    StreamingBuildCoordinator,
)
from src.sources.derived.journal import BuildJournal
from src.sources.derived.plan import BuildPlanner, DerivedBuildPlan
from src.sources.derived.progress import ProgressReporter
from src.storage.metadata_store import DuckDBMetadataStore, ManifestWriteSession
from src.storage.parquet_store import ParquetStore

NOW = datetime(2024, 1, 5, 12, 0)
DATASET_ID = "cn_stock_daily_bar"
TARGET = "daily_bar"


# ---------------------------------------------------------------------------
# Shared fixtures / helpers (mirrors test_derived_pipeline_refactor.py)
# ---------------------------------------------------------------------------


def _master(n: int = 1) -> pd.DataFrame:
    rows = []
    for i in range(n):
        code_num = 600000 + i
        rows.append(
            {
                "security_id": f"SH.{code_num}",
                "code": str(code_num),
                "exchange": "SH",
                "name": f"Stock{i}",
                "security_type": "1",
                "board": "main",
                "baostock_code": f"sh.{code_num}",
                "akshare_code": str(code_num),
                "qlib_symbol": f"sh{code_num}",
                "ipo_date": date(1999, 11, 10),
                "delist_date": None,
                "listing_status": "active",
                "is_active": True,
                "source_priority": "mixed",
                "latest_source_date": date(2024, 1, 5),
                "updated_at": NOW,
            }
        )
    return pd.DataFrame(rows)


def _daily(code: str, close: float = 8.0) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": date(2024, 1, 2),
                "code": code,
                "open": close,
                "high": close + 0.2,
                "low": close - 0.2,
                "close": close,
                "prev_close": close - 0.1,
                "volume": 1000,
                "amount": close * 1000,
                "adjust_flag": "2",
                "turnover_rate": 0.1,
                "trade_status": "1",
                "pct_change": 1.0,
                "pe_ttm": 5.0,
                "pb_mrq": 0.7,
                "ps_ttm": 1.2,
                "pcf_ncf_ttm": 3.0,
                "is_st": "0",
            }
        ]
    )


def _setup_store(tmp_path: Path, n: int = 1) -> ParquetStore:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("cn_security_master", _master(n))
    for i in range(n):
        code_num = 600000 + i
        code = f"sh.{code_num}"
        store.write_dataset(
            "baostock_cn_stock_daily_bar_unadjusted",
            _daily(code, close=8.0 + i * 0.1),
            {"code": code},
        )
    return store


def _read_source(store: ParquetStore, dataset_id: str, partition_value: str) -> pd.DataFrame:
    return store.read_dataset(dataset_id, {"code": partition_value})


def _security_lookup_factory(master: pd.DataFrame) -> Callable[[str], pd.Series | None]:
    index = {row["security_id"]: row for _, row in master.iterrows()}
    return lambda sid: index.get(sid)


def _real_materialize_factory(updated_at: datetime) -> Callable:
    from src.sources.derived.stock_daily_bar import materialize_security_daily_bar

    def _materialize(security, source_frames):
        return materialize_security_daily_bar(security, source_frames, updated_at=updated_at)

    return _materialize


def _empty_materialize(security, source_frames) -> pd.DataFrame:
    """Materialize that returns an empty frame -> triggers delete-partition path."""

    return pd.DataFrame()


def _make_plan(store: ParquetStore, master: pd.DataFrame, *, force: bool = True) -> DerivedBuildPlan:
    from src.sources.derived.stock_daily_bar import BAOSTOCK_DAILY_SOURCES

    planner = BuildPlanner(
        store=store,
        target=TARGET,
        dataset_id=DATASET_ID,
        source_dataset_specs=tuple((dataset_id, "baostock_code") for dataset_id in BAOSTOCK_DAILY_SOURCES),
        master=master,
        force_rebuild=force,
    )
    return planner.plan()


def _final_dir(store: ParquetStore, sid: str) -> Path:
    return store.parquet_dir / DATASET_ID / f"security_id={sid}"


def _read_manifest_row(store: ParquetStore, sid: str) -> pd.Series | None:
    df = store.read_dataset_partition_manifest_batch([DATASET_ID])
    if df.empty:
        return None
    mask = df["partition_value"].astype("string") == sid
    matched = df.loc[mask]
    if matched.empty:
        return None
    return matched.iloc[-1]


def _backup_dirs(store: ParquetStore, sid: str) -> list[Path]:
    backup_root = store.parquet_dir / ".backup"
    if not backup_root.exists():
        return []
    return sorted(backup_root.glob(f"{DATASET_ID}.{sid}.*"))


def _staging_dirs(store: ParquetStore, sid: str) -> list[Path]:
    staging_root = store.parquet_dir / ".staging"
    if not staging_root.exists():
        return []
    return sorted(staging_root.glob(f"{DATASET_ID}.{sid}.*"))


def _run_build(
    store: ParquetStore,
    master: pd.DataFrame,
    plan: DerivedBuildPlan,
    *,
    materialize_fn: Callable,
    run_id: str,
    tmp_path: Path,
) -> tuple[BuildCounters, BuildJournal, ProgressReporter]:
    """Run a single-partition build and return (counters, journal, progress)."""

    journal = BuildJournal.create(
        run_id=run_id,
        target=TARGET,
        dataset_id=DATASET_ID,
        plan_hash=plan.plan_hash,
        schema_version="1",
        source_snapshot_hash=plan.source_snapshot_hash,
        total=plan.total,
        metadata_dir=store.metadata_dir,
    )
    progress = ProgressReporter()
    executor = PartitionExecutor(
        store=store,
        dataset_id=DATASET_ID,
        materialize_fn=materialize_fn,
        source_read_fn=_read_source,
        security_lookup=_security_lookup_factory(master),
        max_workers=1,
        updated_at=NOW,
    )
    coordinator = StreamingBuildCoordinator(
        executor=executor,
        store=store,
        dataset_id=DATASET_ID,
        progress=progress,
        journal=journal,
        heartbeat_interval_seconds=0.05,
    )
    meta_store = DuckDBMetadataStore(root=tmp_path)
    with meta_store.manifest_write_session() as session:
        coordinator.attach_manifest_session(session, run_id=run_id)
        counters = coordinator.run(plan)
    return counters, journal, progress


# ---------------------------------------------------------------------------
# Fault injection helpers
# ---------------------------------------------------------------------------


def _patch_rename_to_fail(monkeypatch, predicate: Callable[[Path, Path], bool]) -> None:
    """Patch ``Path.rename`` to raise ``OSError`` when ``predicate(src, dst)``.

    All other renames delegate to the original implementation. The worker's
    parquet write uses ``os.replace`` (not ``Path.rename``), so this never
    interferes with staging writes.
    """

    original = Path.rename

    def patched_rename(self: Path, target: Path) -> Path:
        if predicate(self, Path(target)):
            raise OSError(f"injected rename failure: {self} -> {target}")
        return original(self, target)

    monkeypatch.setattr(Path, "rename", patched_rename)


def _patch_rmtree_to_fail(monkeypatch, predicate: Callable[[Path], bool]) -> None:
    """Patch ``shutil.rmtree`` to raise ``OSError`` when ``predicate(path)``."""

    original = shutil.rmtree

    def patched_rmtree(path, *args, **kwargs):
        if predicate(Path(str(path))):
            raise OSError(f"injected rmtree failure: {path}")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(shutil, "rmtree", patched_rmtree)


# ---------------------------------------------------------------------------
# Fault point 1: final -> backup rename fails (original final preserved)
# ---------------------------------------------------------------------------


def test_fault1_final_to_backup_fails_original_final_preserved(tmp_path: Path, monkeypatch) -> None:
    """The first rename in ``promote()`` (final -> backup) fails. The original
    final must remain untouched, no backup is created, and the partition is
    recorded as failed."""

    store = _setup_store(tmp_path, n=1)
    master = _master(1)
    sid = "SH.600000"
    plan = _make_plan(store, master, force=True)

    # Pre-build so an old final + old manifest exist.
    counters0, _, _ = _run_build(
        store,
        master,
        plan,
        materialize_fn=_real_materialize_factory(NOW),
        run_id="prebuild",
        tmp_path=tmp_path,
    )
    assert counters0.committed == 1

    final = _final_dir(store, sid)
    assert final.exists()
    old_bytes = (final / "data.parquet").read_bytes()
    marker = final / "OLD_MARKER"
    marker.write_text("old", encoding="utf-8")
    old_manifest = _read_manifest_row(store, sid)
    assert old_manifest is not None
    assert str(old_manifest["run_id"]) == "prebuild"

    # Inject: final -> backup rename fails (source is the final dir).
    _patch_rename_to_fail(monkeypatch, lambda src, dst: src.resolve() == final.resolve())

    plan2 = _make_plan(store, master, force=True)
    counters, journal, _ = _run_build(
        store,
        master,
        plan2,
        materialize_fn=_real_materialize_factory(NOW),
        run_id="fault1",
        tmp_path=tmp_path,
    )

    # Counters: failed, not committed.
    assert counters.failed == 1
    assert counters.committed == 0

    # Journal: NOT completed; recorded as failed.
    assert not journal.is_completed(sid)
    assert sid in journal.failed

    # Final: still exists with OLD content (rename never happened).
    assert final.exists(), "original final must be preserved"
    assert (final / "data.parquet").read_bytes() == old_bytes
    assert marker.exists(), "old marker must still be present"

    # No backup was created.
    assert _backup_dirs(store, sid) == []

    # Staging left as leftover (promote failed before moving it).
    assert len(_staging_dirs(store, sid)) >= 1

    # Manifest: unchanged (still old run_id).
    manifest = _read_manifest_row(store, sid)
    assert manifest is not None
    assert str(manifest["run_id"]) == "prebuild"

    # Next run (no fault) can rebuild.
    monkeypatch.undo()
    plan3 = _make_plan(store, master, force=True)
    counters3, _, _ = _run_build(
        store,
        master,
        plan3,
        materialize_fn=_real_materialize_factory(NOW),
        run_id="rebuild1",
        tmp_path=tmp_path,
    )
    assert counters3.committed == 1
    assert counters3.failed == 0
    manifest3 = _read_manifest_row(store, sid)
    assert manifest3 is not None
    assert str(manifest3["run_id"]) == "rebuild1"


# ---------------------------------------------------------------------------
# Fault point 2: staging -> final fails (backup restored to final)
# ---------------------------------------------------------------------------


def test_fault2_staging_to_final_fails_backup_restored(tmp_path: Path, monkeypatch) -> None:
    """final -> backup succeeds, then staging -> final rename fails inside
    ``promote()``. The internal rollback restores backup -> final, so the old
    content is preserved."""

    store = _setup_store(tmp_path, n=1)
    master = _master(1)
    sid = "SH.600000"
    plan = _make_plan(store, master, force=True)

    counters0, _, _ = _run_build(
        store,
        master,
        plan,
        materialize_fn=_real_materialize_factory(NOW),
        run_id="prebuild",
        tmp_path=tmp_path,
    )
    assert counters0.committed == 1

    final = _final_dir(store, sid)
    old_bytes = (final / "data.parquet").read_bytes()
    marker = final / "OLD_MARKER"
    marker.write_text("old", encoding="utf-8")

    # Inject: staging -> final rename fails (source is under .staging).
    # The backup -> final restore rename has source under .backup, so it
    # is NOT affected and rollback succeeds.
    _patch_rename_to_fail(monkeypatch, lambda src, dst: ".staging" in src.parts)

    plan2 = _make_plan(store, master, force=True)
    counters, journal, _ = _run_build(
        store,
        master,
        plan2,
        materialize_fn=_real_materialize_factory(NOW),
        run_id="fault2",
        tmp_path=tmp_path,
    )

    assert counters.failed == 1
    assert counters.committed == 0
    assert not journal.is_completed(sid)
    assert sid in journal.failed

    # Final restored from backup with OLD content.
    assert final.exists(), "final must be restored from backup"
    assert (final / "data.parquet").read_bytes() == old_bytes
    assert marker.exists(), "old marker must be restored"

    # Backup was consumed by the rollback restore.
    assert _backup_dirs(store, sid) == []

    # Manifest unchanged.
    manifest = _read_manifest_row(store, sid)
    assert manifest is not None
    assert str(manifest["run_id"]) == "prebuild"

    # Next run can rebuild.
    monkeypatch.undo()
    plan3 = _make_plan(store, master, force=True)
    counters3, _, _ = _run_build(
        store,
        master,
        plan3,
        materialize_fn=_real_materialize_factory(NOW),
        run_id="rebuild2",
        tmp_path=tmp_path,
    )
    assert counters3.committed == 1
    assert counters3.failed == 0


# ---------------------------------------------------------------------------
# Fault point 3: new partition staging -> final fails (no half-baked final)
# ---------------------------------------------------------------------------


def test_fault3_new_partition_staging_to_final_fails(tmp_path: Path, monkeypatch) -> None:
    """For a brand-new partition (no prior final), the staging -> final rename
    fails. No half-baked final directory is left behind."""

    store = _setup_store(tmp_path, n=1)
    master = _master(1)
    sid = "SH.600000"
    plan = _make_plan(store, master, force=True)

    # No pre-build: this is a new partition with no prior final.
    final = _final_dir(store, sid)
    assert not final.exists()
    assert _read_manifest_row(store, sid) is None

    # Inject: staging -> final rename fails.
    _patch_rename_to_fail(monkeypatch, lambda src, dst: ".staging" in src.parts)

    counters, journal, _ = _run_build(
        store,
        master,
        plan,
        materialize_fn=_real_materialize_factory(NOW),
        run_id="fault3",
        tmp_path=tmp_path,
    )

    assert counters.failed == 1
    assert counters.committed == 0
    assert not journal.is_completed(sid)
    assert sid in journal.failed

    # No half-baked final left behind.
    assert not final.exists(), "no half-baked final directory should exist"

    # No backup (there was no prior final to back up).
    assert _backup_dirs(store, sid) == []

    # No manifest row.
    assert _read_manifest_row(store, sid) is None

    # Next run (no fault) can build the new partition.
    monkeypatch.undo()
    plan2 = _make_plan(store, master, force=True)
    counters2, _, _ = _run_build(
        store,
        master,
        plan2,
        materialize_fn=_real_materialize_factory(NOW),
        run_id="rebuild3",
        tmp_path=tmp_path,
    )
    assert counters2.committed == 1
    assert counters2.failed == 0
    assert final.exists()
    manifest2 = _read_manifest_row(store, sid)
    assert manifest2 is not None
    assert str(manifest2["run_id"]) == "rebuild3"


# ---------------------------------------------------------------------------
# Fault point 4: manifest upsert fails (rollback restores old final)
# ---------------------------------------------------------------------------


def test_fault4_manifest_upsert_fails_rollback_restores_old_final(tmp_path: Path, monkeypatch) -> None:
    """``promote()`` succeeds but the manifest upsert fails. The coordinator
    calls ``promotion.rollback()`` which restores the old final. The manifest
    stays in its old state."""

    store = _setup_store(tmp_path, n=1)
    master = _master(1)
    sid = "SH.600000"
    plan = _make_plan(store, master, force=True)

    counters0, _, _ = _run_build(
        store,
        master,
        plan,
        materialize_fn=_real_materialize_factory(NOW),
        run_id="prebuild",
        tmp_path=tmp_path,
    )
    assert counters0.committed == 1

    final = _final_dir(store, sid)
    old_bytes = (final / "data.parquet").read_bytes()
    marker = final / "OLD_MARKER"
    marker.write_text("old", encoding="utf-8")

    # Inject: manifest upsert always fails.
    def failing_upsert(self: ManifestWriteSession, row: dict) -> None:
        raise RuntimeError("injected manifest upsert failure")

    monkeypatch.setattr(ManifestWriteSession, "upsert_partition", failing_upsert)

    plan2 = _make_plan(store, master, force=True)
    counters, journal, _ = _run_build(
        store,
        master,
        plan2,
        materialize_fn=_real_materialize_factory(NOW),
        run_id="fault4",
        tmp_path=tmp_path,
    )

    assert counters.failed == 1
    assert counters.committed == 0
    assert not journal.is_completed(sid)
    assert sid in journal.failed

    # Final restored from backup with OLD content.
    assert final.exists(), "final must be restored by rollback"
    assert (final / "data.parquet").read_bytes() == old_bytes
    assert marker.exists(), "old marker must be restored"

    # Backup consumed by rollback.
    assert _backup_dirs(store, sid) == []

    # Manifest unchanged (still old run_id).
    manifest = _read_manifest_row(store, sid)
    assert manifest is not None
    assert str(manifest["run_id"]) == "prebuild"

    # Next run (no fault) can rebuild.
    monkeypatch.undo()
    plan3 = _make_plan(store, master, force=True)
    counters3, _, _ = _run_build(
        store,
        master,
        plan3,
        materialize_fn=_real_materialize_factory(NOW),
        run_id="rebuild4",
        tmp_path=tmp_path,
    )
    assert counters3.committed == 1
    assert counters3.failed == 0
    manifest3 = _read_manifest_row(store, sid)
    assert manifest3 is not None
    assert str(manifest3["run_id"]) == "rebuild4"


# ---------------------------------------------------------------------------
# Fault point 5: manifest delete fails (backup restored to final)
# ---------------------------------------------------------------------------


def test_fault5_manifest_delete_fails_backup_restored(tmp_path: Path, monkeypatch) -> None:
    """Delete-partition flow: ``promote()`` moves final -> backup, then the
    manifest delete fails. ``rollback()`` restores backup -> final so the old
    data and manifest row survive."""

    store = _setup_store(tmp_path, n=1)
    master = _master(1)
    sid = "SH.600000"
    plan = _make_plan(store, master, force=True)

    # Pre-build so an old final + old manifest exist (to be deleted).
    counters0, _, _ = _run_build(
        store,
        master,
        plan,
        materialize_fn=_real_materialize_factory(NOW),
        run_id="prebuild",
        tmp_path=tmp_path,
    )
    assert counters0.committed == 1

    final = _final_dir(store, sid)
    old_bytes = (final / "data.parquet").read_bytes()
    marker = final / "OLD_MARKER"
    marker.write_text("old", encoding="utf-8")
    old_manifest = _read_manifest_row(store, sid)
    assert old_manifest is not None
    assert str(old_manifest["run_id"]) == "prebuild"

    # Inject: manifest delete always fails.
    def failing_delete(
        self: ManifestWriteSession,
        dataset_id: str,
        partition_column: str,
        partition_value: str,
    ) -> None:
        raise RuntimeError("injected manifest delete failure")

    monkeypatch.setattr(ManifestWriteSession, "delete_partition", failing_delete)

    # Re-plan and run with an empty materialize -> delete-partition path.
    plan2 = _make_plan(store, master, force=True)
    counters, journal, _ = _run_build(
        store,
        master,
        plan2,
        materialize_fn=_empty_materialize,
        run_id="fault5",
        tmp_path=tmp_path,
    )

    assert counters.failed == 1
    assert counters.committed == 0
    assert counters.deleted == 0
    assert not journal.is_completed(sid)
    assert sid in journal.failed

    # Final restored from backup with OLD content.
    assert final.exists(), "final must be restored by rollback"
    assert (final / "data.parquet").read_bytes() == old_bytes
    assert marker.exists(), "old marker must be restored"

    # Backup consumed by rollback.
    assert _backup_dirs(store, sid) == []

    # Manifest row still present (delete failed).
    manifest = _read_manifest_row(store, sid)
    assert manifest is not None
    assert str(manifest["run_id"]) == "prebuild"

    # Next run (no fault) can re-attempt the delete and succeed.
    monkeypatch.undo()
    plan3 = _make_plan(store, master, force=True)
    counters3, _, _ = _run_build(
        store,
        master,
        plan3,
        materialize_fn=_empty_materialize,
        run_id="rebuild5",
        tmp_path=tmp_path,
    )
    assert counters3.deleted == 1
    assert counters3.failed == 0
    assert not final.exists()
    assert _read_manifest_row(store, sid) is None


# ---------------------------------------------------------------------------
# Fault point 6: rollback rmtree(new final) fails (RecoveryError raised)
# ---------------------------------------------------------------------------


def test_fault6_rollback_rmtree_new_final_fails_recovery_error(tmp_path: Path, monkeypatch) -> None:
    """Replace path: ``promote()`` succeeds, manifest upsert fails, and the
    ``rmtree`` of the new final inside ``rollback()`` also fails.
    ``PartitionPromotionRecoveryError`` is raised; the coordinator records the
    partition as failed."""

    store = _setup_store(tmp_path, n=1)
    master = _master(1)
    sid = "SH.600000"
    plan = _make_plan(store, master, force=True)

    counters0, _, _ = _run_build(
        store,
        master,
        plan,
        materialize_fn=_real_materialize_factory(NOW),
        run_id="prebuild",
        tmp_path=tmp_path,
    )
    assert counters0.committed == 1

    final = _final_dir(store, sid)
    (final / "data.parquet").read_bytes()
    marker = final / "OLD_MARKER"
    marker.write_text("old", encoding="utf-8")

    # Inject TWO faults: manifest upsert fails (triggers rollback) AND
    # rmtree of the new final fails (makes rollback fail).
    def failing_upsert(self: ManifestWriteSession, row: dict) -> None:
        raise RuntimeError("injected manifest upsert failure")

    monkeypatch.setattr(ManifestWriteSession, "upsert_partition", failing_upsert)
    _patch_rmtree_to_fail(monkeypatch, lambda p: p.resolve() == final.resolve())

    plan2 = _make_plan(store, master, force=True)
    counters, journal, _ = _run_build(
        store,
        master,
        plan2,
        materialize_fn=_real_materialize_factory(NOW),
        run_id="fault6",
        tmp_path=tmp_path,
    )

    assert counters.failed == 1
    assert counters.committed == 0
    assert not journal.is_completed(sid)
    assert sid in journal.failed
    # The failure reason must mention the recovery error.
    assert "PartitionPromotionRecoveryError" in journal.failed[sid], journal.failed[sid]

    # rmtree failed -> the NEW final (promoted staging) is still present.
    assert final.exists(), "new final must still exist (rmtree failed)"
    # Old marker is gone: promote moved the old final to backup.
    assert not marker.exists(), "old final was moved to backup by promote()"

    # Backup still exists: rollback could not complete.
    assert len(_backup_dirs(store, sid)) >= 1

    # Manifest unchanged (upsert failed).
    manifest = _read_manifest_row(store, sid)
    assert manifest is not None
    assert str(manifest["run_id"]) == "prebuild"

    # Next run (no fault) can rebuild: promote moves the new final to a fresh
    # backup and installs a newer staging.
    monkeypatch.undo()
    plan3 = _make_plan(store, master, force=True)
    counters3, _, _ = _run_build(
        store,
        master,
        plan3,
        materialize_fn=_real_materialize_factory(NOW),
        run_id="rebuild6",
        tmp_path=tmp_path,
    )
    assert counters3.committed == 1
    assert counters3.failed == 0
    manifest3 = _read_manifest_row(store, sid)
    assert manifest3 is not None
    assert str(manifest3["run_id"]) == "rebuild6"


# ---------------------------------------------------------------------------
# Fault point 7: rollback backup -> final rename fails (RecoveryError raised)
# ---------------------------------------------------------------------------


def test_fault7_rollback_backup_to_final_rename_fails_recovery_error(tmp_path: Path, monkeypatch) -> None:
    """Replace path: ``promote()`` succeeds, manifest upsert fails, rollback
    rmtree(new final) succeeds, but the backup -> final rename fails.
    ``PartitionPromotionRecoveryError`` is raised; the partition is left with
    no final and a leftover backup (indeterminate state)."""

    store = _setup_store(tmp_path, n=1)
    master = _master(1)
    sid = "SH.600000"
    plan = _make_plan(store, master, force=True)

    counters0, _, _ = _run_build(
        store,
        master,
        plan,
        materialize_fn=_real_materialize_factory(NOW),
        run_id="prebuild",
        tmp_path=tmp_path,
    )
    assert counters0.committed == 1

    final = _final_dir(store, sid)
    marker = final / "OLD_MARKER"
    marker.write_text("old", encoding="utf-8")

    # Inject TWO faults: manifest upsert fails (triggers rollback) AND
    # backup -> final rename fails (source under .backup). The promote renames
    # (final->backup, staging->final) have sources NOT under .backup, so they
    # succeed; only the rollback restore rename is blocked.
    def failing_upsert(self: ManifestWriteSession, row: dict) -> None:
        raise RuntimeError("injected manifest upsert failure")

    monkeypatch.setattr(ManifestWriteSession, "upsert_partition", failing_upsert)
    _patch_rename_to_fail(monkeypatch, lambda src, dst: ".backup" in src.parts)

    plan2 = _make_plan(store, master, force=True)
    counters, journal, _ = _run_build(
        store,
        master,
        plan2,
        materialize_fn=_real_materialize_factory(NOW),
        run_id="fault7",
        tmp_path=tmp_path,
    )

    assert counters.failed == 1
    assert counters.committed == 0
    assert not journal.is_completed(sid)
    assert sid in journal.failed
    assert "PartitionPromotionRecoveryError" in journal.failed[sid], journal.failed[sid]

    # rmtree of the new final succeeded, but backup -> final rename failed:
    # there is NO final directory now.
    assert not final.exists(), "final must not exist (rmtree succeeded, rename failed)"

    # Backup still exists: the restore rename failed.
    assert len(_backup_dirs(store, sid)) >= 1

    # Manifest unchanged.
    manifest = _read_manifest_row(store, sid)
    assert manifest is not None
    assert str(manifest["run_id"]) == "prebuild"

    # Next run (no fault) can rebuild: no final exists, so promote does a
    # straight staging -> final insert.
    monkeypatch.undo()
    plan3 = _make_plan(store, master, force=True)
    counters3, _, _ = _run_build(
        store,
        master,
        plan3,
        materialize_fn=_real_materialize_factory(NOW),
        run_id="rebuild7",
        tmp_path=tmp_path,
    )
    assert counters3.committed == 1
    assert counters3.failed == 0
    assert final.exists()
    manifest3 = _read_manifest_row(store, sid)
    assert manifest3 is not None
    assert str(manifest3["run_id"]) == "rebuild7"


# ---------------------------------------------------------------------------
# Fault point 8: finalize backup cleanup fails (warning only, data consistent)
# ---------------------------------------------------------------------------


def test_fault8_finalize_backup_cleanup_fails_data_consistent(tmp_path: Path, monkeypatch) -> None:
    """``promote()`` and the manifest write both succeed, but
    ``finalize()`` cannot remove the backup directory. This is a warning only:
    the data (final + manifest) is already consistent, the partition is
    committed, and the journal is marked completed."""

    store = _setup_store(tmp_path, n=1)
    master = _master(1)
    sid = "SH.600000"
    plan = _make_plan(store, master, force=True)

    counters0, _, _ = _run_build(
        store,
        master,
        plan,
        materialize_fn=_real_materialize_factory(NOW),
        run_id="prebuild",
        tmp_path=tmp_path,
    )
    assert counters0.committed == 1

    final = _final_dir(store, sid)
    marker = final / "OLD_MARKER"
    marker.write_text("old", encoding="utf-8")

    # Inject: rmtree fails for paths under .backup (finalize backup cleanup).
    # The staging cleanup (under .staging) is NOT affected, so finalize still
    # removes staging and transitions to FINALIZED.
    _patch_rmtree_to_fail(monkeypatch, lambda p: ".backup" in p.parts)

    plan2 = _make_plan(store, master, force=True)
    counters, journal, _ = _run_build(
        store,
        master,
        plan2,
        materialize_fn=_real_materialize_factory(NOW),
        run_id="fault8",
        tmp_path=tmp_path,
    )

    # Committed (not failed): finalize failure is a warning only.
    assert counters.committed == 1
    assert counters.failed == 0

    # Journal marked completed.
    assert journal.is_completed(sid)
    assert sid not in journal.failed

    # Final exists with NEW content (promote succeeded).
    assert final.exists()
    # Old marker gone: the old final was moved to backup by promote.
    assert not marker.exists()

    # Backup still exists: finalize could not clean it up.
    assert len(_backup_dirs(store, sid)) >= 1

    # Manifest UPDATED (upsert succeeded) with the new run_id.
    manifest = _read_manifest_row(store, sid)
    assert manifest is not None
    assert str(manifest["run_id"]) == "fault8"

    # Data is consistent: the manifest output_path points at the current final.
    assert str(manifest["output_path"]) != ""
    assert (store.root / str(manifest["output_path"])).exists()

    # Next run (no fault) can rebuild; the leftover backup does not interfere.
    monkeypatch.undo()
    plan3 = _make_plan(store, master, force=True)
    counters3, _, _ = _run_build(
        store,
        master,
        plan3,
        materialize_fn=_real_materialize_factory(NOW),
        run_id="rebuild8",
        tmp_path=tmp_path,
    )
    assert counters3.committed == 1
    assert counters3.failed == 0
    manifest3 = _read_manifest_row(store, sid)
    assert manifest3 is not None
    assert str(manifest3["run_id"]) == "rebuild8"


# ---------------------------------------------------------------------------
# Direct PartitionPromotion unit tests for fault points 6 and 7.
#
# These verify that PartitionPromotionRecoveryError is raised directly by the
# promotion state machine (not just recorded as a failure reason by the
# coordinator), making the "RecoveryError raised" assertion explicit.
# ---------------------------------------------------------------------------


def _make_promotion_with_prior_final(store: ParquetStore, sid: str) -> tuple[PartitionPromotion, Path, Path]:
    """Create a PartitionPromotion with a prior final and a staging dir.

    Returns (promotion, final_dir, backup_dir_placeholder). The staging dir
    contains a 'new' parquet; the prior final contains an 'old' parquet.
    """

    staging = create_derived_partition_staging_area(store, DATASET_ID, sid)
    (staging.staging_partition_dir / "data.parquet").write_bytes(b"new-parquet")
    final_dir = staging.final_partition_dir
    final_dir.mkdir(parents=True, exist_ok=True)
    (final_dir / "data.parquet").write_bytes(b"old-parquet")
    promotion = PartitionPromotion(staging=staging, delete_partition=False)
    return promotion, final_dir, staging.backup_dir


def test_promotion_direct_fault6_rmtree_new_final_recovery_error(tmp_path: Path, monkeypatch) -> None:
    """Directly verify fault point 6: after a successful promote(), calling
    rollback() with rmtree of the new final patched to fail raises
    PartitionPromotionRecoveryError."""

    store = _setup_store(tmp_path, n=1)
    sid = "SH.600000"
    promotion, final_dir, _ = _make_promotion_with_prior_final(store, sid)

    # promote: old final -> backup, staging -> final.
    promotion.promote()
    # final now holds the 'new' parquet.
    assert (final_dir / "data.parquet").read_bytes() == b"new-parquet"

    # Inject: rmtree of the new final fails.
    _patch_rmtree_to_fail(monkeypatch, lambda p: p.resolve() == final_dir.resolve())

    with pytest.raises(PartitionPromotionRecoveryError):
        promotion.rollback()

    # New final still present (rmtree failed); backup still present.
    assert final_dir.exists()
    assert (final_dir / "data.parquet").read_bytes() == b"new-parquet"


def test_promotion_direct_fault7_backup_to_final_rename_recovery_error(tmp_path: Path, monkeypatch) -> None:
    """Directly verify fault point 7: after a successful promote(), calling
    rollback() with the backup -> final rename patched to fail raises
    PartitionPromotionRecoveryError. The new final is removed (rmtree
    succeeded) but the backup cannot be restored."""

    store = _setup_store(tmp_path, n=1)
    sid = "SH.600000"
    promotion, final_dir, _ = _make_promotion_with_prior_final(store, sid)

    # promote: old final -> backup, staging -> final.
    promotion.promote()
    assert (final_dir / "data.parquet").read_bytes() == b"new-parquet"

    # Inject: backup -> final rename fails (source under .backup).
    # rmtree of the new final is NOT patched, so it succeeds.
    _patch_rename_to_fail(monkeypatch, lambda src, dst: ".backup" in src.parts)

    with pytest.raises(PartitionPromotionRecoveryError):
        promotion.rollback()

    # New final was removed by rmtree; backup could not be restored.
    assert not final_dir.exists()
