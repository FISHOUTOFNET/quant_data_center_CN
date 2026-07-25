"""Targeted tests for the refactored derived pipeline.

These tests cover the requirements from the task spec that were not
already covered by ``test_derived_pipeline.py``:

* 18.1 Bounded streaming execution (in-flight cap, no full-result list)
* 18.2 Single-partition rollback (manifest upsert failure → file restored)
* 18.3 Source vs target manifest missing split
* 18.4 Journal resume validation (signature mismatch → rebuild)
* 18.7 Lock stale detection (live PID even if old → not reclaimed)
* 18.8 Log cleanup dangerous-path rejection + managed-root marker
* ManifestWriteSession single-connection invariant
"""

from __future__ import annotations

import json
import os
import shutil
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from threading import Event
from unittest.mock import patch

import pandas as pd
import pytest

from src.sources.derived.common import (
    create_derived_partition_staging_area,
)
from src.sources.derived.executor import (
    DEFAULT_MAX_IN_FLIGHT_MULTIPLIER,
    MAX_WORKERS_CAP,
    BuildCounters,
    PartitionExecutor,
    PartitionPromotion,
    PreparedPartitionManifest,
    StreamingBuildCoordinator,
    StagedPartitionResult,
)
from src.sources.derived.journal import (
    JOURNAL_STATUS_CANCELLED,
    JOURNAL_STATUS_COMPLETED,
    BuildJournal,
    resume_filter_completed,
    validate_completed_partition,
)
from src.sources.derived.plan import (
    BuildPlanner,
    ChangeReason,
    DerivedBuildPlan,
    DerivedPartitionPlan,
)
from src.sources.derived.progress import ProgressReporter
from src.sources.derived.run_context import build_run_context_paths, make_build_run_context
from src.storage.metadata_store import DuckDBMetadataStore, ManifestWriteSession
from src.storage.parquet_store import ParquetStore
from src.tools import log_cleanup
from src.utils import process_lock

NOW = datetime(2024, 1, 5, 12, 0)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _master(n: int = 4) -> pd.DataFrame:
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


def _setup_store(tmp_path: Path, n: int = 4) -> ParquetStore:
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


def _identity_materialize(security, source_frames):
    """Trivial materialize that returns the first source frame unchanged."""
    for df in source_frames.values():
        return df.copy()
    return pd.DataFrame()


def _real_materialize_factory(updated_at):
    """Wrap the real materialize_security_daily_bar for the executor signature."""
    from src.sources.derived.stock_daily_bar import materialize_security_daily_bar

    def _materialize(security, source_frames):
        return materialize_security_daily_bar(security, source_frames, updated_at=updated_at)

    return _materialize


def _read_source(store, dataset_id, partition_value):
    return store.read_dataset(dataset_id, {"code": partition_value})


def _security_lookup_factory(master):
    index = {row["security_id"]: row for _, row in master.iterrows()}
    return lambda sid: index.get(sid)


# ---------------------------------------------------------------------------
# 18.1 Bounded streaming execution
# ---------------------------------------------------------------------------


def _make_plan(store: ParquetStore, master: pd.DataFrame, *, force: bool = True) -> object:
    from src.sources.derived.stock_daily_bar import BAOSTOCK_DAILY_SOURCES

    planner = BuildPlanner(
        store=store,
        target="daily_bar",
        dataset_id="cn_stock_daily_bar",
        source_dataset_specs=tuple(
            (dataset_id, "baostock_code") for dataset_id in BAOSTOCK_DAILY_SOURCES
        ),
        master=master,
        force_rebuild=force,
    )
    return planner.plan()


def test_streaming_coordinator_caps_in_flight_at_max_workers_times_multiplier(
    tmp_path: Path,
) -> None:
    """``in_flight`` never exceeds ``max_workers * multiplier`` even with
    1000 partitions, and ``max_in_flight_observed`` is bounded."""

    store = _setup_store(tmp_path, n=4)
    master = _master(4)
    plan = _make_plan(store, master)
    assert plan.total == 4

    # Pad the plan to 1000 partitions by duplicating plan items with synthetic
    # security_ids. We re-use the same source data; the executor's
    # ``_submit_until_full`` is what we are testing, not the data correctness.
    padded_items = []
    for i in range(1000):
        src = plan.partitions[i % len(plan.partitions)]
        padded_items.append(
            DerivedPartitionPlan(
                security_id=f"SYNTH.{i:04d}",
                source_signature=src.source_signature,
                master_row_hash=src.master_row_hash,
                source_partitions=src.source_partitions,
                change_reason=ChangeReason.FORCE_REBUILD,
            )
        )
    # Build a synthetic plan object with the padded list.
    from src.sources.derived.plan import DerivedBuildPlan

    synthetic_plan = DerivedBuildPlan(
        target=plan.target,
        dataset_id=plan.dataset_id,
        partitions=tuple(padded_items),
        plan_hash=plan.plan_hash,
        source_snapshot_hash=plan.source_snapshot_hash,
        schema_version=plan.schema_version,
    )

    # The synth security_ids are not in the master → workers return None for
    # all of them. We only care about the in-flight bound here.
    executor = PartitionExecutor(
        store=store,
        dataset_id="cn_stock_daily_bar",
        materialize_fn=_identity_materialize,
        source_read_fn=_read_source,
        security_lookup=lambda sid: None,  # Always None → worker returns None.
        max_workers=4,
        updated_at=NOW,
    )
    coordinator = StreamingBuildCoordinator(
        executor=executor,
        store=store,
        dataset_id="cn_stock_daily_bar",
        max_in_flight_multiplier=DEFAULT_MAX_IN_FLIGHT_MULTIPLIER,
        heartbeat_interval_seconds=0.05,
    )
    counters = coordinator.run(synthetic_plan)

    # In-flight must never exceed 4 * 2 = 8.
    assert counters.max_in_flight_observed <= 4 * DEFAULT_MAX_IN_FLIGHT_MULTIPLIER
    # All 1000 partitions were processed (each failed because lookup → None).
    assert counters.failed == 1000
    # The coordinator returns counters, NOT a list of 1000 results.
    assert isinstance(counters, BuildCounters)


def test_streaming_coordinator_does_not_return_full_result_list(tmp_path: Path) -> None:
    """The coordinator returns BuildCounters (aggregate), not a list of
    per-partition results that would hold DataFrames in memory."""

    store = _setup_store(tmp_path, n=4)
    master = _master(4)
    plan = _make_plan(store, master)

    executor = PartitionExecutor(
        store=store,
        dataset_id="cn_stock_daily_bar",
        materialize_fn=_real_materialize_factory(NOW),
        source_read_fn=_read_source,
        security_lookup=_security_lookup_factory(master),
        max_workers=2,
        updated_at=NOW,
    )
    coordinator = StreamingBuildCoordinator(
        executor=executor,
        store=store,
        dataset_id="cn_stock_daily_bar",
        max_in_flight_multiplier=2,
        heartbeat_interval_seconds=0.05,
    )
    meta_store = DuckDBMetadataStore(root=tmp_path)
    with meta_store.manifest_write_session() as session:
        coordinator.attach_manifest_session(session, run_id="test-no-list")
        counters = coordinator.run(plan)

    assert isinstance(counters, BuildCounters)
    assert counters.committed == 4
    assert counters.failed == 0
    # No DataFrame attribute on the returned object.
    assert not hasattr(counters, "df")
    assert not hasattr(counters, "results")


# ---------------------------------------------------------------------------
# 18.2 Single-partition rollback (manifest write failure → file restored)
# ---------------------------------------------------------------------------


def test_promotion_rollback_restores_final_when_manifest_upsert_fails(
    tmp_path: Path,
) -> None:
    """If the manifest upsert fails after file promotion, ``rollback()``
    restores the original final directory and removes the promoted one."""

    store = _setup_store(tmp_path, n=2)
    sid = "SH.600000"
    # Manually create an existing final directory with a marker file.
    final_dir = store.parquet_dir / "cn_stock_daily_bar" / f"security_id={sid}"
    final_dir.mkdir(parents=True, exist_ok=True)
    marker = final_dir / "ORIGINAL_MARKER"
    marker.write_text("original", encoding="utf-8")
    (final_dir / "data.parquet").write_bytes(b"original-parquet")

    # Create a new staging area for the same partition.
    staging = create_derived_partition_staging_area(store, "cn_stock_daily_bar", sid)
    # Write a new parquet into the staging dir.
    (staging.staging_partition_dir / "data.parquet").write_bytes(b"new-parquet")

    promotion = PartitionPromotion(staging=staging, delete_partition=False)
    # Promote: original final → backup, staging → final.
    promotion.promote()
    assert not marker.exists()  # New final replaced the old one.
    assert staging.final_partition_dir.exists()
    # The new parquet is now in final.
    assert (staging.final_partition_dir / "data.parquet").read_bytes() == b"new-parquet"

    # Simulate manifest-write failure: rollback should restore the original.
    promotion.rollback()

    # Original final restored (with marker), new promoted final removed.
    assert marker.exists(), "rollback did not restore the original final directory"
    assert marker.read_text(encoding="utf-8") == "original"
    assert (staging.final_partition_dir / "data.parquet").read_bytes() == b"original-parquet"
    # Backup dir should be gone after rollback restores it.
    assert not staging.backup_dir.exists()


def test_streaming_coordinator_rollback_on_manifest_failure(tmp_path: Path) -> None:
    """End-to-end: when ``ManifestWriteSession.upsert_partition`` raises,
    the coordinator's ``_commit_partition`` rolls back the file promotion
    and records the partition as failed (not completed)."""

    store = _setup_store(tmp_path, n=3)
    master = _master(3)
    plan = _make_plan(store, master)

    executor = PartitionExecutor(
        store=store,
        dataset_id="cn_stock_daily_bar",
        materialize_fn=_real_materialize_factory(NOW),
        source_read_fn=_read_source,
        security_lookup=_security_lookup_factory(master),
        max_workers=1,
        updated_at=NOW,
    )
    progress = ProgressReporter()
    journal = BuildJournal.create(
        run_id="test-rollback",
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
        progress=progress,
        journal=journal,
        heartbeat_interval_seconds=0.05,
    )

    # Attach a session whose upsert_partition always fails.
    meta_store = DuckDBMetadataStore(root=tmp_path)
    with meta_store.manifest_write_session() as session:
        coordinator.attach_manifest_session(session, run_id="test-rollback")
        original_upsert = session.upsert_partition
        call_count = {"n": 0}

        def failing_upsert(row):
            call_count["n"] += 1
            raise RuntimeError("simulated manifest write failure")

        session.upsert_partition = failing_upsert  # type: ignore[method-assign]
        counters = coordinator.run(plan)

    # All 3 partitions failed; none committed.
    assert counters.committed == 0
    assert counters.failed == 3
    assert call_count["n"] == 3
    # Journal must NOT have marked any as completed.
    assert len(journal.completed) == 0
    # No final partition directories should exist (rollback removed them).
    for item in plan.partitions:
        final_path = store.dataset_path("cn_stock_daily_bar", {"security_id": item.security_id})
        assert not final_path.exists(), f"final unexpectedly exists for {item.security_id}"


# ---------------------------------------------------------------------------
# 18.3 Source vs target manifest missing
# ---------------------------------------------------------------------------


def test_source_manifest_missing_reason_is_unrecoverable() -> None:
    """``SOURCE_MANIFEST_MISSING`` (and the legacy ``MANIFEST_MISSING``) must
    be flagged as manifest-missing by the executor, which records a failure
    rather than building or skipping."""

    from src.sources.derived.executor import _is_manifest_missing_reason

    assert _is_manifest_missing_reason(ChangeReason.SOURCE_MANIFEST_MISSING)
    assert _is_manifest_missing_reason(ChangeReason.MANIFEST_MISSING)
    # TARGET_MANIFEST_MISSING must NOT be treated as a source-manifest failure;
    # it goes through the normal rebuild path.
    assert not _is_manifest_missing_reason(ChangeReason.TARGET_MANIFEST_MISSING)
    assert not _is_manifest_missing_reason(ChangeReason.TARGET_MISSING)
    assert not _is_manifest_missing_reason(ChangeReason.SOURCE_CHANGED)


def test_target_manifest_missing_triggers_rebuild_not_skip(tmp_path: Path) -> None:
    """When the target file exists but its manifest row is missing, the
    planner must emit a ``TARGET_MANIFEST_MISSING`` plan (rebuild path),
    NOT skip the partition."""

    from src.sources.derived.stock_daily_bar import BAOSTOCK_DAILY_SOURCES

    store = _setup_store(tmp_path, n=2)
    master = _master(2)
    # First build so the target exists.
    planner = BuildPlanner(
        store=store,
        target="daily_bar",
        dataset_id="cn_stock_daily_bar",
        source_dataset_specs=tuple(
            (dataset_id, "baostock_code") for dataset_id in BAOSTOCK_DAILY_SOURCES
        ),
        master=master,
        force_rebuild=True,
    )
    plan = planner.plan()
    # Manually commit partitions to create target files but DELETE the
    # manifest rows, simulating "file committed but manifest write failed".
    executor = PartitionExecutor(
        store=store,
        dataset_id="cn_stock_daily_bar",
        materialize_fn=_real_materialize_factory(NOW),
        source_read_fn=_read_source,
        security_lookup=_security_lookup_factory(master),
        max_workers=1,
        updated_at=NOW,
    )
    results = executor.execute(plan)
    # Promote files for the first partition only.
    built = next(r for r in results if r is not None)
    staging_area = create_derived_partition_staging_area(store, "cn_stock_daily_bar", built.security_id)
    # Re-use the existing final dir as the "committed" target.
    final_dir = built.final_partition_dir
    final_dir.mkdir(parents=True, exist_ok=True)
    # Move the staged parquet into final.
    for p in built.staging_partition_dir.iterdir():
        shutil.move(str(p), str(final_dir / p.name))
    # Delete the manifest row for this partition (simulate failed manifest write).
    store._metadata_store.delete_dataset_partition_manifest(
        "cn_stock_daily_bar", "security_id", built.security_id
    )
    store.close()

    # Re-plan without force_rebuild: TARGET_MANIFEST_MISSING must be emitted.
    planner2 = BuildPlanner(
        store=store,
        target="daily_bar",
        dataset_id="cn_stock_daily_bar",
        source_dataset_specs=tuple(
            (dataset_id, "baostock_code") for dataset_id in BAOSTOCK_DAILY_SOURCES
        ),
        master=master,
        force_rebuild=False,
    )
    plan2 = planner2.plan()
    matching = [p for p in plan2.partitions if p.security_id == built.security_id]
    assert matching, "TARGET_MANIFEST_MISSING partition not in plan"
    assert matching[0].change_reason == ChangeReason.TARGET_MANIFEST_MISSING


# ---------------------------------------------------------------------------
# 18.4 Journal resume validation
# ---------------------------------------------------------------------------


def test_resume_filter_rebuilds_when_target_file_missing(tmp_path: Path) -> None:
    """A journal-completed partition whose target file is missing must be
    removed from ``completed`` and rebuilt."""

    store = _setup_store(tmp_path, n=2)
    master = _master(2)
    plan = _make_plan(store, master)
    journal = BuildJournal.create(
        run_id="test-resume-file-missing",
        target="daily_bar",
        dataset_id="cn_stock_daily_bar",
        plan_hash=plan.plan_hash,
        schema_version="1",
        source_snapshot_hash=plan.source_snapshot_hash,
        total=plan.total,
        metadata_dir=store.metadata_dir,
    )
    # Mark the first partition completed in the journal.
    target_item = plan.partitions[0]
    journal.record_completed(target_item.security_id)
    # But its target file does NOT exist (we never built it).
    target_path = store.dataset_path("cn_stock_daily_bar", {"security_id": target_item.security_id})
    assert not target_path.exists()

    removed = resume_filter_completed(store, journal, plan, dataset_id="cn_stock_daily_bar")
    assert removed == 1
    assert not journal.is_completed(target_item.security_id)


def test_resume_filter_rebuilds_when_signature_mismatch(tmp_path: Path) -> None:
    """A journal-completed partition whose manifest source_signature no longer
    matches the plan must be removed and rebuilt."""

    store = _setup_store(tmp_path, n=2)
    master = _master(2)
    # First do a real build so files + manifests exist.
    from src.sources.derived.stock_daily_bar import build_cn_stock_daily_bar

    build_cn_stock_daily_bar(
        root=tmp_path,
        build_views=False,
        refresh_registry=False,
        now=lambda: NOW,
    )
    store.close()

    # Re-plan with force_rebuild to get a plan covering all partitions.
    plan = _make_plan(store, master)
    journal = BuildJournal.create(
        run_id="test-resume-sig-mismatch",
        target="daily_bar",
        dataset_id="cn_stock_daily_bar",
        plan_hash=plan.plan_hash,
        schema_version="1",
        source_snapshot_hash=plan.source_snapshot_hash,
        total=plan.total,
        metadata_dir=store.metadata_dir,
    )
    target_item = plan.partitions[0]
    journal.record_completed(target_item.security_id)

    # Tamper with the plan's source_signature so it no longer matches the
    # manifest's stored signature → validation must fail.
    tampered = DerivedPartitionPlan(
        security_id=target_item.security_id,
        source_signature="TAMPERED_DOES_NOT_MATCH",
        master_row_hash=target_item.master_row_hash,
        source_partitions=target_item.source_partitions,
        change_reason=target_item.change_reason,
    )
    tampered_plan = DerivedBuildPlan(
        target=plan.target,
        dataset_id=plan.dataset_id,
        partitions=(tampered, *plan.partitions[1:]),
        plan_hash=plan.plan_hash,
        source_snapshot_hash=plan.source_snapshot_hash,
        schema_version=plan.schema_version,
    )
    removed = resume_filter_completed(
        store, journal, tampered_plan, dataset_id="cn_stock_daily_bar"
    )
    assert removed == 1
    assert not journal.is_completed(target_item.security_id)


def test_resume_filter_keeps_valid_completed(tmp_path: Path) -> None:
    """A journal-completed partition with a valid file + manifest + matching
    signature is kept in ``completed`` (skipped on resume)."""

    from src.sources.derived.stock_daily_bar import build_cn_stock_daily_bar

    store = _setup_store(tmp_path, n=2)
    build_cn_stock_daily_bar(
        root=tmp_path,
        build_views=False,
        refresh_registry=False,
        now=lambda: NOW,
    )
    store.close()
    master = _master(2)
    plan = _make_plan(store, master, force=False)
    # No changes → plan is empty. Use force=True to get a plan covering all.
    plan = _make_plan(store, master, force=True)
    journal = BuildJournal.create(
        run_id="test-resume-valid",
        target="daily_bar",
        dataset_id="cn_stock_daily_bar",
        plan_hash=plan.plan_hash,
        schema_version="1",
        source_snapshot_hash=plan.source_snapshot_hash,
        total=plan.total,
        metadata_dir=store.metadata_dir,
    )
    for item in plan.partitions:
        journal.record_completed(item.security_id)

    removed = resume_filter_completed(store, journal, plan, dataset_id="cn_stock_daily_bar")
    assert removed == 0
    for item in plan.partitions:
        assert journal.is_completed(item.security_id)


# ---------------------------------------------------------------------------
# ManifestWriteSession: one connection per build
# ---------------------------------------------------------------------------


def test_manifest_write_session_uses_one_connection(tmp_path: Path) -> None:
    """``manifest_write_session`` opens exactly one DuckDB connection for the
    whole session lifetime (regardless of how many partitions are upserted)."""

    store = _setup_store(tmp_path, n=1)
    meta_store = DuckDBMetadataStore(root=tmp_path)
    connect_calls = {"n": 0}
    original_connect = duckdb_connect_spy = None

    import duckdb

    original_connect = duckdb.connect

    def spy_connect(*args, **kwargs):
        connect_calls["n"] += 1
        return original_connect(*args, **kwargs)

    with patch("src.storage.metadata_store.duckdb.connect", spy_connect):
        with meta_store.manifest_write_session() as session:
            assert isinstance(session, ManifestWriteSession)
            # Upsert several rows; they all reuse the same connection.
            for i in range(5):
                session.upsert_partition(
                    {
                        "dataset": "cn_stock_daily_bar",
                        "partition_column": "security_id",
                        "partition_value": f"SH.{600000 + i}",
                        "output_path": f"data/parquet/cn_stock_daily_bar/security_id=SH.{600000 + i}/data.parquet",
                        "row_count": 1,
                        "min_date": "2024-01-02",
                        "max_date": "2024-01-02",
                        "content_hash": f"hash-{i}",
                        "semantic_hash": f"sem-{i}",
                        "schema_hash": "schema-1",
                        "source_signature": f"sig-{i}",
                        "master_row_hash": f"master-{i}",
                        "file_size_bytes": 1024,
                        "file_mtime": datetime(2024, 1, 2),
                        "run_id": "test-session",
                        "writer_pid": os.getpid(),
                        "writer_thread": "coordinator",
                        "updated_at": datetime(2024, 1, 2),
                    }
                )
            # Exactly one connect() call for the whole session.
            assert connect_calls["n"] == 1, connect_calls["n"]
            assert session.upsert_count == 5

    # After exit, the connection is closed.


def test_manifest_write_session_partitioned_transactions(tmp_path: Path) -> None:
    """A failed upsert does not poison the connection for the next partition."""

    meta_store = DuckDBMetadataStore(root=tmp_path)
    with meta_store.manifest_write_session() as session:
        # First upsert succeeds.
        session.upsert_partition(
            {
                "dataset": "cn_stock_daily_bar",
                "partition_column": "security_id",
                "partition_value": "SH.600000",
                "output_path": "data/parquet/cn_stock_daily_bar/security_id=SH.600000/data.parquet",
                "row_count": 1,
                "min_date": "2024-01-02",
                "max_date": "2024-01-02",
                "content_hash": "h1",
                "semantic_hash": "s1",
                "schema_hash": "schema-1",
                "source_signature": "sig-1",
                "master_row_hash": "m1",
                "file_size_bytes": 1024,
                "file_mtime": datetime(2024, 1, 2),
                "run_id": "test-txn",
                "writer_pid": os.getpid(),
                "writer_thread": "coordinator",
                "updated_at": datetime(2024, 1, 2),
            }
        )
        # Second upsert fails (drop the table to force an error mid-transaction).
        session._conn.execute("DROP TABLE dataset_partition_manifest")
        with pytest.raises(Exception):
            session.upsert_partition(
                {
                    "dataset": "cn_stock_daily_bar",
                    "partition_column": "security_id",
                    "partition_value": "SH.600001",
                    "output_path": "x",
                    "row_count": 1,
                    "min_date": "",
                    "max_date": "",
                    "content_hash": "h2",
                    "semantic_hash": "s2",
                    "schema_hash": "schema-1",
                    "source_signature": "sig-2",
                    "master_row_hash": "m2",
                    "file_size_bytes": 0,
                    "file_mtime": datetime(2024, 1, 2),
                    "run_id": "test-txn",
                    "writer_pid": os.getpid(),
                    "writer_thread": "coordinator",
                    "updated_at": datetime(2024, 1, 2),
                }
            )
        # Recreate the table; the third upsert must succeed (connection not poisoned).
        session._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dataset_partition_manifest (
                dataset VARCHAR, partition_column VARCHAR, partition_value VARCHAR,
                output_path VARCHAR, row_count BIGINT, min_date VARCHAR, max_date VARCHAR,
                content_hash VARCHAR, semantic_hash VARCHAR, schema_hash VARCHAR,
                source_signature VARCHAR, master_row_hash VARCHAR,
                file_size_bytes BIGINT, file_mtime TIMESTAMP,
                run_id VARCHAR, writer_pid BIGINT, writer_thread VARCHAR, updated_at TIMESTAMP
            )
            """
        )
        session.upsert_partition(
            {
                "dataset": "cn_stock_daily_bar",
                "partition_column": "security_id",
                "partition_value": "SH.600002",
                "output_path": "y",
                "row_count": 1,
                "min_date": "",
                "max_date": "",
                "content_hash": "h3",
                "semantic_hash": "s3",
                "schema_hash": "schema-1",
                "source_signature": "sig-3",
                "master_row_hash": "m3",
                "file_size_bytes": 0,
                "file_mtime": datetime(2024, 1, 2),
                "run_id": "test-txn",
                "writer_pid": os.getpid(),
                "writer_thread": "coordinator",
                "updated_at": datetime(2024, 1, 2),
            }
        )
        assert session.upsert_count == 2  # First + third succeeded; second failed.


# ---------------------------------------------------------------------------
# 18.7 Lock stale detection: live PID wins over age
# ---------------------------------------------------------------------------


def test_lock_not_reclaimed_when_pid_alive_even_if_old(tmp_path: Path) -> None:
    """A lock whose owner PID is alive must NOT be reclaimed, no matter how
    old the lock is. This is the spec's "PID liveness wins over age" rule."""

    lock_dir = tmp_path / "build-derived.lock"
    lock_dir.mkdir()
    # Write an owner.json with THIS process's PID and a very old started_at.
    owner = {
        "pid": os.getpid(),
        "hostname": __import__("socket").gethostname(),
        "started_at": (datetime.now() - timedelta(hours=48)).isoformat(timespec="seconds"),
        "lock_name": "build-derived",
        "purpose": "build-derived",
        "stale_after_seconds": 12 * 60 * 60,
    }
    (lock_dir / "owner.json").write_text(
        json.dumps(owner, indent=2, sort_keys=True), encoding="utf-8"
    )

    # Try to acquire: should fail because the PID is alive (even though 48h old).
    with pytest.raises(process_lock.ProcessLockError):
        with process_lock.acquire_process_lock(
            lock_dir,
            lock_name="build-derived",
            purpose="build-derived",
            stale_after_seconds=12 * 60 * 60,
        ):
            pass


def test_lock_reclaimed_when_pid_dead(tmp_path: Path) -> None:
    """A lock whose owner PID is dead is reclaimed immediately."""

    lock_dir = tmp_path / "build-derived-dead.lock"
    lock_dir.mkdir()
    # Use a PID that is guaranteed to not exist (a very high number).
    owner = {
        "pid": 999999,
        "hostname": __import__("socket").gethostname(),
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "lock_name": "build-derived",
        "purpose": "build-derived",
        "stale_after_seconds": 12 * 60 * 60,
    }
    (lock_dir / "owner.json").write_text(
        json.dumps(owner, indent=2, sort_keys=True), encoding="utf-8"
    )

    # Should succeed: dead PID → reclaim.
    with process_lock.acquire_process_lock(
        lock_dir,
        lock_name="build-derived",
        purpose="build-derived",
        stale_after_seconds=12 * 60 * 60,
    ) as lock:
        assert lock.path == lock_dir.resolve()


# ---------------------------------------------------------------------------
# 18.8 Log cleanup: managed-root marker + dangerous-path rejection
# ---------------------------------------------------------------------------


def test_managed_root_marker_is_created_on_first_use(tmp_path: Path) -> None:
    """A fresh log root gets the ``.qdc-managed-log-root`` marker on first cleanup."""

    log_root = tmp_path / "logs"
    log_root.mkdir()
    # No marker yet.
    assert not (log_root / log_cleanup.MANAGED_ROOT_MARKER).exists()
    log_cleanup.cleanup_logs(log_root, retention_days=30)
    # Marker now exists.
    marker = log_root / log_cleanup.MANAGED_ROOT_MARKER
    assert marker.exists()
    payload = json.loads(marker.read_text(encoding="utf-8"))
    assert payload["application"] == "QuantDataCenter"
    assert payload["layout_version"] == log_cleanup.MANAGED_ROOT_LAYOUT_VERSION


def test_cleanup_rejects_drive_root(tmp_path: Path) -> None:
    """A bare Windows drive root (e.g. C:\\) must be rejected.

    On Windows, ``C:\\`` is both a drive root and a filesystem root (its
    parent is itself), so it is caught by the filesystem-root check first.
    Either rejection reason is acceptable."""

    if os.name != "nt":
        pytest.skip("drive-root rejection is Windows-specific")
    # C:\ resolves to a drive root.
    drive_root = Path("C:\\")
    with pytest.raises(log_cleanup.LogCleanupError, match="(drive root|filesystem root)"):
        log_cleanup.cleanup_logs(drive_root, retention_days=30)


def test_cleanup_rejects_user_home(tmp_path: Path, monkeypatch) -> None:
    """The user home directory must be rejected."""

    import pathlib

    home = pathlib.Path.home()
    with pytest.raises(log_cleanup.LogCleanupError, match="home"):
        log_cleanup.cleanup_logs(home, retention_days=30)


def test_cleanup_rejects_repo_root(tmp_path: Path, monkeypatch) -> None:
    """The repository root and any repo-internal path must be rejected."""

    from src.utils import paths

    repo_root = paths.ROOT
    with pytest.raises(log_cleanup.LogCleanupError, match="repositor"):
        log_cleanup.cleanup_logs(repo_root, retention_days=30)
    # A subdirectory inside the repo is also rejected.
    repo_internal = repo_root / "data" / "logs"
    with pytest.raises(log_cleanup.LogCleanupError, match="repositor"):
        log_cleanup.cleanup_logs(repo_internal, retention_days=30)


def test_cleanup_rejects_junction_root(tmp_path: Path) -> None:
    """A Windows junction/reparse root is rejected."""

    if os.name != "nt":
        pytest.skip("junction rejection is Windows-specific")
    if not hasattr(Path, "is_junction"):
        pytest.skip("Path.is_junction not available on this Python")
    # Create a junction target outside tmp_path.
    real_target = tmp_path / "real-logs"
    real_target.mkdir()
    junction = tmp_path / "junction-logs"
    # Use cmd.exe to create a junction.
    import subprocess

    ret = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(junction), str(real_target)],
        capture_output=True,
    )
    if ret.returncode != 0:
        pytest.skip("could not create junction")
    with pytest.raises(log_cleanup.LogCleanupError, match="junction"):
        log_cleanup.cleanup_logs(junction, retention_days=30)


# ---------------------------------------------------------------------------
# BuildRunContext: progress path bound to journal run_id
# ---------------------------------------------------------------------------


def test_build_run_context_progress_path_matches_journal_run_id(
    tmp_path: Path,
) -> None:
    """When resuming, the progress path uses the journal's old run_id —
    not a fresh new run id. This was the original orphaned-progress bug."""

    store = _setup_store(tmp_path, n=2)
    master = _master(2)
    plan = _make_plan(store, master)
    # Simulate a resumable journal from a previous run with run_id "OLD-RUN".
    journal = BuildJournal.create(
        run_id="OLD-RUN",
        target="daily_bar",
        dataset_id="cn_stock_daily_bar",
        plan_hash=plan.plan_hash,
        schema_version="1",
        source_snapshot_hash=plan.source_snapshot_hash,
        total=plan.total,
        metadata_dir=store.metadata_dir,
    )
    context = make_build_run_context(
        plan=plan,
        journal=journal,
        metadata_dir=store.metadata_dir,
    )
    # The context's run_id and progress path must reflect the OLD journal.
    assert context.run_id == "OLD-RUN"
    assert "OLD-RUN" in context.progress_path.name
    assert context.progress_path.suffix == ".json"
    # The progress path is derived from build_run_context_paths using the
    # same run_id — no directory scan.
    expected_journal, expected_progress = build_run_context_paths(
        metadata_dir=store.metadata_dir, run_id="OLD-RUN"
    )
    assert context.journal_path == expected_journal
    assert context.progress_path == expected_progress


# ---------------------------------------------------------------------------
# 18.5 / 18.6 : Cancellation during run (not just pre-set)
# ---------------------------------------------------------------------------


def test_cancellation_during_run_commits_completed_and_stops(tmp_path: Path) -> None:
    """When the cancel_event is set AFTER some partitions have started, the
    coordinator commits completed results, stops submitting new work, and
    reports ``cancelled=True``."""

    store = _setup_store(tmp_path, n=8)
    master = _master(8)
    plan = _make_plan(store, master)
    cancel_event = Event()
    call_count = {"n": 0}

    def slow_materialize(security, source_frames):
        call_count["n"] += 1
        # After 3 calls, set the cancel event (mid-run).
        if call_count["n"] == 3:
            cancel_event.set()
        # Small delay so the coordinator's wait loop can observe cancellation.
        time.sleep(0.05)
        from src.sources.derived.stock_daily_bar import materialize_security_daily_bar

        return materialize_security_daily_bar(security, source_frames, updated_at=NOW)

    executor = PartitionExecutor(
        store=store,
        dataset_id="cn_stock_daily_bar",
        materialize_fn=slow_materialize,
        source_read_fn=_read_source,
        security_lookup=_security_lookup_factory(master),
        max_workers=1,  # Serial → deterministic call ordering.
        updated_at=NOW,
    )
    journal = BuildJournal.create(
        run_id="test-cancel-mid",
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
    meta_store = DuckDBMetadataStore(root=tmp_path)
    with meta_store.manifest_write_session() as session:
        coordinator.attach_manifest_session(session, run_id="test-cancel-mid")
        counters = coordinator.run(plan)

    # Some partitions committed (>= 3 called before cancel), rest skipped.
    assert counters.cancelled is True
    assert counters.committed >= 1
    # The total processed + failed + cancelled cannot exceed the planned count.
    assert (counters.committed + counters.failed) <= plan.total
