"""Tests for the staged derived build pipeline.

Covers the algorithmic-correctness requirements from the task spec:

* manifest snapshot loads in O(#datasets) not O(#securities) — no N+1 query
* source signature computed exactly once per security
* concurrency 1 vs 4 produces identical output
* journal resume skips already-committed partitions
* stale staging directories are reclaimed
* cooperative cancellation stops the executor early
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from datetime import date, datetime, timedelta
from pathlib import Path
from threading import Event

import pandas as pd
import pytest

from src.sources.derived.common import cleanup_stale_derived_staging
from src.sources.derived.executor import PartitionExecutor, StreamingBuildCoordinator
from src.sources.derived.journal import (
    JOURNAL_STATUS_COMPLETED,
    BuildJournal,
    cleanup_old_journals,
    find_resumable_journal,
)
from src.sources.derived.plan import BuildPlanner
from src.sources.derived.stock_daily_bar import (
    AKSHARE_DAILY_SOURCES,
    BAOSTOCK_DAILY_SOURCES,
    build_cn_stock_daily_bar,
    materialize_security_daily_bar,
)
from src.storage.parquet_store import ParquetStore

NOW = datetime(2024, 1, 5, 12, 0)
DAILY_SOURCE_DATASETS = set(BAOSTOCK_DAILY_SOURCES) | set(AKSHARE_DAILY_SOURCES)


# ---------------------------------------------------------------------------
# Multi-security fixture
# ---------------------------------------------------------------------------


def _multi_master(n: int = 10) -> pd.DataFrame:
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


def _daily_for(code: str, close: float = 8.0) -> pd.DataFrame:
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


def _setup_multi_security_store(tmp_path: Path, n: int = 10) -> ParquetStore:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("cn_security_master", _multi_master(n))
    for i in range(n):
        code_num = 600000 + i
        code = f"sh.{code_num}"
        store.write_dataset(
            "baostock_cn_stock_daily_bar_unadjusted",
            _daily_for(code, close=8.0 + i * 0.1),
            {"code": code},
        )
    return store


# ---------------------------------------------------------------------------
# 1. Manifest N+1 query elimination
# ---------------------------------------------------------------------------


def test_planner_loads_source_manifests_in_batch_not_per_security(tmp_path, monkeypatch) -> None:
    """The planner must call ``read_dataset_partition_manifest_batch`` exactly
    once per plan, NOT ``read_dataset_partition_manifest`` once per security."""

    store = _setup_multi_security_store(tmp_path, n=10)
    batch_calls = {"count": 0}
    single_calls = {"count": 0}
    original_batch = ParquetStore.read_dataset_partition_manifest_batch
    original_single = ParquetStore.read_dataset_partition_manifest

    def spy_batch(self, dataset_ids):
        batch_calls["count"] += 1
        return original_batch(self, dataset_ids)

    def spy_single(self, dataset_id):
        # The planner is allowed to call this for the *target* dataset once,
        # but must NOT call it per source dataset / per security.
        if dataset_id in DAILY_SOURCE_DATASETS:
            single_calls["count"] += 1
        return original_single(self, dataset_id)

    monkeypatch.setattr(ParquetStore, "read_dataset_partition_manifest_batch", spy_batch)
    monkeypatch.setattr(ParquetStore, "read_dataset_partition_manifest", spy_single)

    planner = BuildPlanner(
        store=store,
        target="daily_bar",
        dataset_id="cn_stock_daily_bar",
        source_dataset_specs=tuple((dataset_id, "baostock_code") for dataset_id in BAOSTOCK_DAILY_SOURCES),
        master=_multi_master(10),
    )
    planner.plan()

    # Batch query is called at most twice (initial + post-preflight-repair).
    # Even with 10 securities x 6 source datasets, the query count must NOT
    # grow linearly with the number of securities.
    assert batch_calls["count"] <= 2, batch_calls["count"]
    assert single_calls["count"] == 0, single_calls["count"]


# ---------------------------------------------------------------------------
# 2. Source signature computed once
# ---------------------------------------------------------------------------


def test_source_signature_computed_once_per_security(tmp_path, monkeypatch) -> None:
    """The planner computes each security's source signature exactly once.
    The executor must NOT re-compute it."""

    _setup_multi_security_store(tmp_path, n=10)
    from src.storage import partition_manifest as pm_mod

    sig_calls: list[str] = []
    original_sig = pm_mod.source_signature

    def spy_sig(rows, master_hash):
        sig_calls.append(master_hash)
        return original_sig(rows, master_hash)

    monkeypatch.setattr(pm_mod, "source_signature", spy_sig)
    # Also patch the import in plan.py if it imported the symbol directly.
    monkeypatch.setattr("src.sources.derived.plan.source_signature", spy_sig)

    build_cn_stock_daily_bar(
        root=tmp_path,
        build_views=False,
        refresh_registry=False,
        now=lambda: NOW,
    )

    # 10 securities → at most 10 signature computations (one per security).
    # The old code computed it twice per security (planner + build loop).
    assert len(sig_calls) <= 10, len(sig_calls)


# ---------------------------------------------------------------------------
# 3. Concurrency 1 vs 4 produces identical output
# ---------------------------------------------------------------------------


def _build_and_snapshot(tmp_path: Path, max_workers: int) -> dict[str, pd.DataFrame]:
    store = _setup_multi_security_store(tmp_path, n=8)
    build_cn_stock_daily_bar(
        root=tmp_path,
        build_views=False,
        refresh_registry=False,
        now=lambda: NOW,
        max_workers=max_workers,
    )
    snapshot: dict[str, pd.DataFrame] = {}
    for i in range(8):
        sid = f"SH.{600000 + i}"
        snapshot[sid] = store.read_dataset("cn_stock_daily_bar", {"security_id": sid}).copy()
    return snapshot


def _snapshots_equal(a: dict[str, pd.DataFrame], b: dict[str, pd.DataFrame]) -> bool:
    if set(a.keys()) != set(b.keys()):
        return False
    for key in a:
        df_a, df_b = a[key], b[key]
        if list(df_a.columns) != list(df_b.columns):
            return False
        if len(df_a) != len(df_b):
            return False
        for col in df_a.columns:
            if not df_a[col].equals(df_b[col]):
                return False
    return True


def test_concurrency_1_and_4_produce_identical_output(tmp_path) -> None:
    """Building with 1 worker must produce byte-identical output to 4 workers."""

    # Build with 1 worker
    tmp_a = tmp_path / "a"
    tmp_a.mkdir()
    snap_a = _build_and_snapshot(tmp_a, max_workers=1)

    # Build with 4 workers
    tmp_b = tmp_path / "b"
    tmp_b.mkdir()
    snap_b = _build_and_snapshot(tmp_b, max_workers=4)

    assert _snapshots_equal(snap_a, snap_b), "Output differs between concurrency 1 and 4"


# ---------------------------------------------------------------------------
# 4. Journal resume skips already-committed partitions
# ---------------------------------------------------------------------------


def test_journal_resume_skips_completed_partitions(tmp_path) -> None:
    """After a crash, resuming a build must skip partitions already committed
    (and recorded in the journal), rebuilding only the remaining ones."""

    store = _setup_multi_security_store(tmp_path, n=6)
    # First, do a full build so all 6 partitions are committed.
    result1 = build_cn_stock_daily_bar(
        root=tmp_path,
        build_views=False,
        refresh_registry=False,
        now=lambda: NOW,
    )
    assert result1["partitions"] == 6

    # Now simulate a crash: create a fresh journal that marks 3 of 6 as completed,
    # then verify the executor skips them and only rebuilds the other 3.
    master = _multi_master(6)
    planner = BuildPlanner(
        store=store,
        target="daily_bar",
        dataset_id="cn_stock_daily_bar",
        source_dataset_specs=tuple((dataset_id, "baostock_code") for dataset_id in BAOSTOCK_DAILY_SOURCES),
        master=master,
    )
    plan = planner.plan()
    assert plan.total == 0  # All partitions are up-to-date → nothing to build.

    # Force a rebuild so the plan is non-empty, then test journal skip.
    planner = BuildPlanner(
        store=store,
        target="daily_bar",
        dataset_id="cn_stock_daily_bar",
        source_dataset_specs=tuple((dataset_id, "baostock_code") for dataset_id in BAOSTOCK_DAILY_SOURCES),
        master=master,
        force_rebuild=True,
    )
    plan = planner.plan()
    assert plan.total == 6

    # Create a journal marking the first 3 as completed.
    journal = BuildJournal.create(
        run_id="test-resume-journal",
        target="daily_bar",
        dataset_id="cn_stock_daily_bar",
        plan_hash=plan.plan_hash,
        schema_version="1",
        source_snapshot_hash=plan.source_snapshot_hash,
        total=plan.total,
        metadata_dir=store.metadata_dir,
    )
    for item in plan.partitions[:3]:
        journal.record_completed(item.security_id)
    journal._write_full(force=True)

    # Track which partitions the executor actually materializes.
    security_index = {row["security_id"]: row for _, row in master.iterrows()}
    executed_securities: list[str] = []
    original_materialize = materialize_security_daily_bar

    def tracking_materialize(security, source_frames):
        executed_securities.append(str(security["security_id"]))
        return original_materialize(security, source_frames, updated_at=NOW)

    executor = PartitionExecutor(
        store=store,
        dataset_id="cn_stock_daily_bar",
        materialize_fn=tracking_materialize,
        source_read_fn=lambda s, did, pv: s.read_dataset(did, {"code": pv}),
        security_lookup=lambda sid: security_index.get(sid),
        max_workers=2,
        updated_at=NOW,
    )
    coordinator = StreamingBuildCoordinator(
        executor=executor,
        store=store,
        dataset_id="cn_stock_daily_bar",
        journal=journal,
        heartbeat_interval_seconds=0.05,
    )
    with store._metadata_store.manifest_write_session() as session:
        coordinator.attach_manifest_session(session, run_id="test-resume-journal")
        counters = coordinator.run(plan)

    # The first 3 should be skipped (journal-completed), the last 3 committed.
    assert counters.committed == 3, f"Expected 3 committed, got {counters.committed}; executed={executed_securities}"
    skipped_ids = {item.security_id for item in plan.partitions[:3]}
    executed_ids = set(executed_securities)
    assert skipped_ids.isdisjoint(executed_ids), f"Skipped IDs were executed: {skipped_ids & executed_ids}"


def test_journal_rejects_mismatched_plan(tmp_path) -> None:
    """A journal whose plan_hash doesn't match must NOT be resumed."""

    store = _setup_multi_security_store(tmp_path, n=4)
    # Create a journal with a wrong plan_hash.
    BuildJournal.create(
        run_id="test-mismatch-journal",
        target="daily_bar",
        dataset_id="cn_stock_daily_bar",
        plan_hash="wrong-hash",
        schema_version="1",
        source_snapshot_hash="wrong-snapshot",
        total=4,
        metadata_dir=store.metadata_dir,
    )
    # find_resumable_journal should NOT return it because the hashes don't match.
    result = find_resumable_journal(
        metadata_dir=store.metadata_dir,
        target="daily_bar",
        dataset_id="cn_stock_daily_bar",
        schema_version="1",
        plan_hash="correct-hash",
        source_snapshot_hash="correct-snapshot",
    )
    assert result is None


# ---------------------------------------------------------------------------
# 5. Stale staging cleanup
# ---------------------------------------------------------------------------


def test_cleanup_stale_derived_staging_removes_old_dirs(tmp_path) -> None:
    """Stale staging directories older than 1 hour are removed."""

    store = _setup_multi_security_store(tmp_path, n=2)
    # Create a fake stale staging directory.
    staging_root = store.parquet_dir / ".staging"
    staging_root.mkdir(parents=True, exist_ok=True)
    stale_dir = staging_root / "cn_stock_daily_bar.SH.999999.deadbeef"
    stale_dir.mkdir()
    (stale_dir / "cn_stock_daily_bar" / "security_id=SH.999999").mkdir(parents=True)
    (stale_dir / "cn_stock_daily_bar" / "security_id=SH.999999" / "data.parquet").write_bytes(b"stale")

    # Backdate the mtime to 2 hours ago so it's older than the 1-hour threshold.
    old_time = time.time() - 7200
    import os

    os.utime(stale_dir, (old_time, old_time))

    removed = cleanup_stale_derived_staging(store, "cn_stock_daily_bar")
    assert removed == 1
    assert not stale_dir.exists()


def test_cleanup_stale_derived_staging_keeps_recent_dirs(tmp_path) -> None:
    """Recent staging directories (< 1 hour old) are NOT removed."""

    store = _setup_multi_security_store(tmp_path, n=2)
    staging_root = store.parquet_dir / ".staging"
    staging_root.mkdir(parents=True, exist_ok=True)
    recent_dir = staging_root / "cn_stock_daily_bar.SH.999999.recent"
    recent_dir.mkdir()
    (recent_dir / "cn_stock_daily_bar" / "security_id=SH.999999").mkdir(parents=True)

    # Don't backdate — it's brand new.
    removed = cleanup_stale_derived_staging(store, "cn_stock_daily_bar")
    assert removed == 0
    assert recent_dir.exists()


# ---------------------------------------------------------------------------
# 6. Cooperative cancellation
# ---------------------------------------------------------------------------


def test_executor_respects_cancel_event(tmp_path) -> None:
    """When the cancel_event is set, the executor stops submitting new work."""

    store = _setup_multi_security_store(tmp_path, n=10)
    master = _multi_master(10)
    planner = BuildPlanner(
        store=store,
        target="daily_bar",
        dataset_id="cn_stock_daily_bar",
        source_dataset_specs=tuple((dataset_id, "baostock_code") for dataset_id in BAOSTOCK_DAILY_SOURCES),
        master=master,
        force_rebuild=True,
    )
    plan = planner.plan()
    assert plan.total == 10

    cancel_event = Event()
    cancel_event.set()  # Pre-set: no work should be submitted.

    security_index = {row["security_id"]: row for _, row in master.iterrows()}

    def _wrap_materialize(security, source_frames):
        return materialize_security_daily_bar(security, source_frames, updated_at=NOW)

    executor = PartitionExecutor(
        store=store,
        dataset_id="cn_stock_daily_bar",
        materialize_fn=_wrap_materialize,
        source_read_fn=lambda s, did, pv: s.read_dataset(did, {"code": pv}),
        security_lookup=lambda sid: security_index.get(sid),
        max_workers=4,
        updated_at=NOW,
    )
    journal = BuildJournal.create(
        run_id="test-cancel",
        target="daily_bar",
        dataset_id="cn_stock_daily_bar",
        plan_hash=plan.plan_hash,
        schema_version="1",
        source_snapshot_hash=plan.source_snapshot_hash,
        total=plan.total,
        metadata_dir=store.metadata_dir,
    )
    coordinator = StreamingBuildCoordinator(
        executor=executor,
        store=store,
        dataset_id="cn_stock_daily_bar",
        journal=journal,
        cancel_event=cancel_event,
        heartbeat_interval_seconds=0.05,
    )
    with store._metadata_store.manifest_write_session() as session:
        coordinator.attach_manifest_session(session, run_id="test-cancel")
        counters = coordinator.run(plan)

    # With cancel pre-set, no futures are submitted → 0 committed.
    assert counters.committed == 0
    assert counters.failed == 0


# ---------------------------------------------------------------------------
# 7. Idempotency: re-running a successful build is a no-op
# ---------------------------------------------------------------------------


def test_idempotent_rerun_does_not_rebuild(tmp_path) -> None:
    """A second run with no source changes must produce 0 partitions."""

    _setup_multi_security_store(tmp_path, n=5)
    result1 = build_cn_stock_daily_bar(
        root=tmp_path,
        build_views=False,
        refresh_registry=False,
        now=lambda: NOW,
    )
    assert result1["partitions"] == 5

    result2 = build_cn_stock_daily_bar(
        root=tmp_path,
        build_views=False,
        refresh_registry=False,
        now=lambda: NOW,
    )
    assert result2["partitions"] == 0
    assert result2["status"] == "success"


# ---------------------------------------------------------------------------
# 8. Journal cleanup
# ---------------------------------------------------------------------------


def test_cleanup_old_journals_keeps_running_and_recent(tmp_path) -> None:
    """``cleanup_old_journals`` keeps active journals and the most recent ones."""

    store = _setup_multi_security_store(tmp_path, n=2)
    # Create a running journal (must be kept).
    running = BuildJournal.create(
        run_id="test-running",
        target="daily_bar",
        dataset_id="cn_stock_daily_bar",
        plan_hash="h1",
        schema_version="1",
        source_snapshot_hash="s1",
        total=2,
        metadata_dir=store.metadata_dir,
    )
    # Create 25 completed journals (only 20 should be kept).
    for i in range(25):
        j = BuildJournal.create(
            run_id=f"test-completed-{i:02d}",
            target="daily_bar",
            dataset_id="cn_stock_daily_bar",
            plan_hash=f"h-{i}",
            schema_version="1",
            source_snapshot_hash=f"s-{i}",
            total=2,
            metadata_dir=store.metadata_dir,
        )
        j.finalize(JOURNAL_STATUS_COMPLETED)

    deleted = cleanup_old_journals(store.metadata_dir, keep_recent=20)
    assert deleted >= 5  # At least 5 of the 25 completed journals were deleted.
    # The running journal must survive.
    assert running.path.exists()


# ---------------------------------------------------------------------------
# 9. Worker failure isolation: one partition failure doesn't corrupt others
# ---------------------------------------------------------------------------


def test_worker_failure_isolates_partition(tmp_path, monkeypatch) -> None:
    """If one partition's materialize fails, other partitions still commit."""

    _setup_multi_security_store(tmp_path, n=6)
    call_count = {"n": 0}
    original = materialize_security_daily_bar

    def fail_on_third(security, source_frames, *, updated_at):
        call_count["n"] += 1
        if call_count["n"] == 3:
            raise RuntimeError("simulated worker failure")
        return original(security, source_frames, updated_at=updated_at)

    monkeypatch.setattr(
        "src.sources.derived.stock_daily_bar.materialize_security_daily_bar",
        fail_on_third,
    )

    result = build_cn_stock_daily_bar(
        root=tmp_path,
        build_views=False,
        refresh_registry=False,
        now=lambda: NOW,
        max_workers=1,  # Serial to make the "third" call deterministic.
    )
    assert result["status"] == "partial"
    assert result["failed"] == 1
    assert result["partitions"] == 5  # 5 of 6 succeeded.

    # Verify no staging leak.
    staging_dir = tmp_path / "data" / "parquet" / ".staging"
    if staging_dir.exists():
        leaks = [p for p in staging_dir.iterdir() if p.is_dir() and p.name.startswith("cn_stock_daily_bar.")]
        assert leaks == [], f"Staging leak: {leaks}"


# ---------------------------------------------------------------------------
# 10. build_derived_datasets does NOT pre-filter daily_bar via N+1 query
# ---------------------------------------------------------------------------


def test_build_derived_datasets_skips_n_plus_one_prefilter_for_daily_bar(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``build_derived_datasets`` must NOT call ``_changed_security_ids_for_target``
    for ``daily_bar`` in incremental mode, because that would re-introduce the
    N+1 manifest query the BuildPlanner was designed to eliminate. The planner
    inside ``build_cn_stock_daily_bar`` handles change detection in batch.
    """

    from src.sources.derived import update as update_module

    store = _setup_multi_security_store(tmp_path, n=4)
    # Close the store's DuckDB connection so the real builder can re-open it.
    store.close()

    prefilter_calls: list[str] = []
    original_prefilter = update_module._changed_security_ids_for_target

    def spy_prefilter(s, m, target, changed_since):
        prefilter_calls.append(target)
        return original_prefilter(s, m, target, changed_since)

    monkeypatch.setattr(update_module, "_changed_security_ids_for_target", spy_prefilter)

    update_module.build_derived_datasets(
        root=tmp_path,
        targets=("daily_bar",),
        mode="incremental",
        build_views=False,
        refresh_registry=False,
        now=lambda: NOW,
    )

    # The pre-filter MUST NOT be called for daily_bar (the BuildPlanner handles it).
    # If valuation were also requested, it would still be called for valuation only.
    assert "daily_bar" not in prefilter_calls, (
        f"_changed_security_ids_for_target was called for daily_bar ({prefilter_calls}), "
        "which re-introduces the N+1 manifest query."
    )


# ---------------------------------------------------------------------------
# 11. Stall detection (consumed by the orchestrator)
# ---------------------------------------------------------------------------


def test_check_stall_returns_not_stalled_when_heartbeat_fresh(tmp_path) -> None:
    """A build with a fresh heartbeat must NOT be declared stalled."""

    from src.sources.derived.progress import STAGE_BUILDING_PARTITIONS, check_stall

    state_path = tmp_path / "progress.state.json"
    fresh = datetime.now()
    payload = {
        "stage": STAGE_BUILDING_PARTITIONS,
        "processed": 5,
        "total": 100,
        "heartbeat_at": fresh.isoformat(timespec="seconds"),
    }
    state_path.write_text(json.dumps(payload), encoding="utf-8")

    report = check_stall(state_path, stall_heartbeat_seconds=25 * 60)
    assert not report.stalled
    assert report.processed == 5


def test_check_stall_returns_stalled_when_heartbeat_stale_and_processed_unchanged(tmp_path) -> None:
    """A build whose heartbeat is stale AND processed is unchanged is stalled."""

    from src.sources.derived.progress import STAGE_BUILDING_PARTITIONS, check_stall

    state_path = tmp_path / "progress.state.json"
    stale_time = datetime(2024, 1, 1, 0, 0, 0)
    reference = stale_time + timedelta(minutes=30)  # 30 min later
    payload = {
        "stage": STAGE_BUILDING_PARTITIONS,
        "processed": 5,
        "total": 100,
        "heartbeat_at": stale_time.isoformat(timespec="seconds"),
    }
    state_path.write_text(json.dumps(payload), encoding="utf-8")

    # First poll: previous_processed=None → stalled (only heartbeat checked).
    report = check_stall(
        state_path,
        now=reference,
        stall_heartbeat_seconds=25 * 60,
        previous_processed=None,
    )
    assert report.stalled
    assert "heartbeat" in (report.reason or "")


def test_check_stall_not_stalled_when_processed_changed(tmp_path) -> None:
    """Even with a stale heartbeat, if processed changed it's NOT stalled."""

    from src.sources.derived.progress import STAGE_BUILDING_PARTITIONS, check_stall

    state_path = tmp_path / "progress.state.json"
    stale_time = datetime(2024, 1, 1, 0, 0, 0)
    reference = stale_time + timedelta(minutes=30)
    payload = {
        "stage": STAGE_BUILDING_PARTITIONS,
        "processed": 10,  # Changed from previous=5
        "total": 100,
        "heartbeat_at": stale_time.isoformat(timespec="seconds"),
    }
    state_path.write_text(json.dumps(payload), encoding="utf-8")

    report = check_stall(
        state_path,
        now=reference,
        stall_heartbeat_seconds=25 * 60,
        previous_processed=5,
    )
    assert not report.stalled
    assert "processed changed" in (report.reason or "")


def test_check_stall_not_stalled_for_terminal_stage(tmp_path) -> None:
    """A completed build is never stalled."""

    from src.sources.derived.progress import STAGE_COMPLETED, check_stall

    state_path = tmp_path / "progress.state.json"
    payload = {
        "stage": STAGE_COMPLETED,
        "processed": 100,
        "total": 100,
        "heartbeat_at": datetime(2020, 1, 1).isoformat(timespec="seconds"),
    }
    state_path.write_text(json.dumps(payload), encoding="utf-8")

    report = check_stall(state_path, now=datetime.now())
    assert not report.stalled


def test_check_stall_not_stalled_when_state_file_missing(tmp_path) -> None:
    """Missing state file means the build may not have started yet."""

    from src.sources.derived.progress import check_stall

    report = check_stall(tmp_path / "nonexistent.state.json")
    assert not report.stalled


# ---------------------------------------------------------------------------
# 12. build_cn_stock_daily_bar exposes cancelled status
# ---------------------------------------------------------------------------


def test_build_cn_stock_daily_bar_reports_cancelled_when_cancel_event_set(tmp_path, monkeypatch) -> None:
    """When the cancel_event is set during a build, the result status is ``cancelled``."""

    store = _setup_multi_security_store(tmp_path, n=4)
    # Close the store's DuckDB connection so the real builder can re-open it.
    store.close()

    # Force the cancel event to be set immediately by patching the signal
    # handler installer to return a pre-set event.
    from src.sources.derived import stock_daily_bar as sdb_mod

    pre_set_event = Event()
    pre_set_event.set()

    def fake_install() -> tuple[Event, Callable[[], None]]:
        return pre_set_event, lambda: None

    monkeypatch.setattr(sdb_mod, "_install_cancel_signal_handler", fake_install)

    result = build_cn_stock_daily_bar(
        root=tmp_path,
        build_views=False,
        refresh_registry=False,
        now=lambda: NOW,
    )
    assert result["status"] == "cancelled"
    assert result["cancelled"] is True
