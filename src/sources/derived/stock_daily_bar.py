"""Build the partitioned canonical daily bar dataset.

This module integrates the staged derived build pipeline:

    BuildPlanner → DerivedBuildPlan → PartitionExecutor → StreamingBuildCoordinator

with a lightweight BuildJournal for crash-safe resume and a ProgressReporter
for structured progress reporting.

The core transformation is split into two layers:

1. ``materialize_security_daily_bar(security, source_frames)`` — a PURE
   function that maps source DataFrames into the canonical daily-bar schema.
   It does NOT query manifests, acquire locks, update state, or decide
   whether to build incrementally. This makes it reusable by other derived
   targets and easy to unit-test.

2. ``build_cn_stock_daily_bar(...)`` — the orchestrator entrypoint that
   wires the planner, executor, and coordinator together.
"""

from __future__ import annotations

import os
import shutil
import signal
import uuid
from collections.abc import Callable, Mapping
from datetime import datetime
from pathlib import Path
from threading import Event
from typing import Any

import pandas as pd

from src.sources.derived.common import (
    cleanup_derived_dataset_staging,
    cleanup_derived_partition_staging,
    cleanup_stale_derived_staging,
    commit_derived_dataset_staging,
    commit_derived_partition_staging,
    create_derived_dataset_staging_area,
    create_derived_partition_staging_area,
    refresh_derived_registry,
)
from src.sources.derived.config import (
    DEFAULT_HEARTBEAT_SECONDS,
    DEFAULT_MAX_IN_FLIGHT_MULTIPLIER,
    DEFAULT_MAX_WORKERS,
    DerivedConfigError,
    load_derived_runtime_config,
)
from src.sources.derived.executor import (
    PartitionExecutor,
    StreamingBuildCoordinator,
)
from src.sources.derived.journal import (
    JOURNAL_STATUS_ABANDONED,
    JOURNAL_STATUS_CANCELLED,
    BuildJournal,
    JOURNAL_STATUS_COMPLETED,
    JOURNAL_STATUS_FAILED,
    cleanup_old_journals,
    find_resumable_journal,
    resume_filter_completed,
)
from src.sources.derived.manifest import (
    cleanup_stale_derived_manifests,
    current_source_signature_for_security,
    delete_derived_partition_manifest,
    source_partition_pairs_for_security,
    upsert_derived_partition_manifest,
)
from src.sources.derived.plan import (
    BuildPlanner,
    ChangeReason,
    DerivedBuildPlan,
    DerivedPartitionPlan,
)
from src.sources.derived.progress import (
    ProgressReporter,
    STAGE_BUILDING_PARTITIONS,
    STAGE_CANCELLED,
    STAGE_CANCELLING,
    STAGE_COMPLETED,
    STAGE_COMMITTING,
    STAGE_FAILED,
    STAGE_PLANNING,
    STAGE_REPAIRING_MANIFEST,
)
from src.sources.derived.run_context import make_build_run_context
from src.sources.derived.security_master import build_security_master
from src.storage.dataset_catalog import DATASET_CATALOG
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore
from src.storage.schema import CN_STOCK_DAILY_BAR_SCHEMA
from src.utils.logging import logger

BAOSTOCK_DAILY_SOURCES = {
    "baostock_cn_stock_daily_bar_unadjusted": "unadjusted",
    "baostock_cn_stock_daily_bar_qfq": "qfq",
    "baostock_cn_stock_daily_bar_hfq": "hfq",
}
AKSHARE_DAILY_SOURCES = {
    "akshare_cn_stock_daily_bar_unadjusted": "unadjusted",
    "akshare_cn_stock_daily_bar_qfq": "qfq",
    "akshare_cn_stock_daily_bar_hfq": "hfq",
}
CN_STOCK_DAILY_BAR_COLUMNS = tuple(field.name for field in CN_STOCK_DAILY_BAR_SCHEMA)
DAILY_BAR_SCHEMA_VERSION = "1"


def build_cn_stock_daily_bar(
    *,
    root: Path | None = None,
    security_ids: tuple[str, ...] | None = None,
    changed_since: datetime | None = None,
    build_views: bool = True,
    refresh_registry: bool = True,
    now: Callable[[], datetime] | None = None,
    max_workers: int | None = None,
) -> dict[str, object]:
    """Build the canonical daily-bar dataset using the staged pipeline.

    Args:
        root: Repository root. Defaults to :func:`paths.ROOT`.
        security_ids: Optional filter — only build these securities. When
            ``None``, the planner builds all securities whose source data has
            changed (or whose target partition is missing).
        changed_since: Optional cutoff — partitions whose source manifest
            ``updated_at`` is on or after this datetime are rebuilt.
        build_views: Whether to rebuild DuckDB views after the build.
        refresh_registry: Whether to refresh the data registry after the build.
        now: Clock override for deterministic tests.
        max_workers: Thread pool size for the partition executor. Defaults to
            :data:`DEFAULT_MAX_WORKERS` (4). Capped at 8.
    """

    store = ParquetStore(root=root)
    store.ensure_layout()
    run_id = f"build-daily-bar-{uuid.uuid4().hex[:12]}"
    timestamp = (now or datetime.now)()

    # Read/build the security master once — the planner needs it to compute
    # source partition pairs and master_row_hash for each security.
    master = _read_or_build_master(store, now)
    if master.empty:
        logger.warning("build_cn_stock_daily_bar: master is empty; nothing to build")
        return {
            "dataset": "cn_stock_daily_bar",
            "status": "success",
            "rows": 0,
            "partitions": 0,
            "run_id": run_id,
        }

    # Filter to explicit security_ids when provided.
    effective_master = master
    if security_ids:
        requested = {sid.upper() for sid in security_ids if sid}
        effective_master = master.loc[
            master["security_id"].astype("string").str.upper().isin(requested)
        ].reset_index(drop=True)
        if effective_master.empty:
            logger.warning(
                "build_cn_stock_daily_bar: no securities matched filter={}",
                security_ids,
            )
            return {
                "dataset": "cn_stock_daily_bar",
                "status": "success",
                "rows": 0,
                "partitions": 0,
                "run_id": run_id,
                "security_ids": tuple(sorted(requested)),
            }

    # ------------------------------------------------------------------
    # Stage 1: Plan (must happen before run id is decided)
    # ------------------------------------------------------------------
    # A throwaway reporter is used only for the planning stage; the real
    # progress reporter is created AFTER the run id is finalized so its state
    # path is bound to the journal's run id (not a fresh id that gets swapped).
    planning_reporter = ProgressReporter()
    planning_reporter.set_stage(STAGE_PLANNING)

    source_dataset_specs = (
        tuple((dataset_id, "baostock_code") for dataset_id in BAOSTOCK_DAILY_SOURCES)
        + tuple((dataset_id, "akshare_code") for dataset_id in AKSHARE_DAILY_SOURCES)
    )
    planner = BuildPlanner(
        store=store,
        target="daily_bar",
        dataset_id="cn_stock_daily_bar",
        source_dataset_specs=source_dataset_specs,
        master=effective_master,
        changed_since=changed_since,
        force_rebuild=bool(security_ids),  # Explicit filter → rebuild all matched.
    )
    plan = planner.plan()

    if not plan.partitions:
        logger.info("build_cn_stock_daily_bar: plan has 0 partitions; nothing to build")
        planning_reporter.final(STAGE_COMPLETED)
        _finalize_build(store, build_views, refresh_registry, ("cn_stock_daily_bar",))
        return {
            "dataset": "cn_stock_daily_bar",
            "status": "success",
            "rows": 0,
            "partitions": 0,
            "run_id": run_id,
        }

    # ------------------------------------------------------------------
    # Stage 2: Find resumable journal → decide final run id → create context
    # ------------------------------------------------------------------
    journal = find_resumable_journal(
        metadata_dir=store.metadata_dir,
        target="daily_bar",
        dataset_id="cn_stock_daily_bar",
        schema_version=DAILY_BAR_SCHEMA_VERSION,
        plan_hash=plan.plan_hash,
        source_snapshot_hash=plan.source_snapshot_hash,
    )
    if journal is not None:
        logger.info(
            "build_cn_stock_daily_bar: resuming from journal run_id={} completed={}/{}",
            journal.run_id,
            len(journal.completed),
            plan.total,
        )
        # Re-validate each journal-completed partition against the target file
        # and manifest. A partition whose file/manifest is missing or whose
        # signature no longer matches is removed from ``completed`` so it is
        # rebuilt on this resume (instead of being silently skipped).
        discarded = resume_filter_completed(
            store, journal, plan, dataset_id="cn_stock_daily_bar"
        )
        if discarded:
            logger.info(
                "build_cn_stock_daily_bar: resume validation discarded {} stale completed partition(s)",
                discarded,
            )
    else:
        journal = BuildJournal.create(
            run_id=run_id,
            target="daily_bar",
            dataset_id="cn_stock_daily_bar",
            plan_hash=plan.plan_hash,
            schema_version=DAILY_BAR_SCHEMA_VERSION,
            source_snapshot_hash=plan.source_snapshot_hash,
            total=plan.total,
            metadata_dir=store.metadata_dir,
            now=timestamp,
        )

    # The BuildRunContext binds the journal run id to the progress path so
    # the orchestrator's stall detector reads the correct file. When the
    # daily orchestrator pins a progress path via QDC_DERIVED_PROGRESS_PATH,
    # that path wins over the journal-derived default. This is the single
    # authoritative progress-path contract between orchestrator and child:
    # the orchestrator decides the path before spawn, passes it via env, and
    # reads only that path. The journal run id is unchanged so resume
    # semantics are preserved.
    cancel_event, restore_signals = _install_cancel_signal_handler()
    progress_path_override = _progress_path_override_from_env()
    context = make_build_run_context(
        plan=plan,
        journal=journal,
        metadata_dir=store.metadata_dir,
        cancel_event=cancel_event,
        progress_path_override=progress_path_override,
    )
    progress = ProgressReporter(state_path=context.progress_path)
    progress.set_stage(STAGE_REPAIRING_MANIFEST, total=plan.total)
    progress.set_stage(STAGE_BUILDING_PARTITIONS, total=plan.total)

    # ------------------------------------------------------------------
    # Stage 3+4: Bounded streaming execution + per-partition commit
    # ------------------------------------------------------------------
    # The streaming coordinator submits at most max_workers*multiplier futures
    # at a time, commits each partition immediately when its future completes,
    # and calls heartbeat every ``heartbeat_seconds`` independent of partition
    # completions. This replaces the previous "submit all → collect all →
    # batch commit" flow which held all DataFrames in memory and had a large
    # manifest-write failure window.
    #
    # Runtime tunables (max_workers, max_in_flight_multiplier,
    # heartbeat_seconds, stall_seconds) come from a single
    # :class:`DerivedRuntimeConfig` loaded from ``settings.yaml`` so there is
    # exactly one set of values in flight — no "daily-bar 30 min, orchestrator
    # 25 min, YAML 30 min" divergence (P0-9). CLI ``--max-workers`` overrides
    # only max_workers; the other three always come from settings.yaml (or code
    # default). Config load is fail-fast: an invalid settings.yaml raises
    # :class:`DerivedConfigError` instead of silently degrading.
    try:
        runtime_config = load_derived_runtime_config(
            root=store.root, max_workers_override=max_workers
        )
    except DerivedConfigError:
        # Re-raise so the CLI surfaces a clear error rather than silently
        # falling back to defaults. ``load_derived_runtime_config_or_default``
        # exists for callers (e.g. the orchestrator's stall detector) that
        # must keep running even when settings.yaml is missing.
        raise
    try:
        security_index = _build_security_index(effective_master)
        executor = PartitionExecutor(
            store=store,
            dataset_id="cn_stock_daily_bar",
            materialize_fn=_pure_materialize_wrapper(timestamp),
            source_read_fn=_read_source_partition,
            security_lookup=lambda sid: security_index.get(sid),
            max_workers=runtime_config.max_workers,
            updated_at=timestamp,
        )
        coordinator = StreamingBuildCoordinator(
            executor=executor,
            store=store,
            dataset_id="cn_stock_daily_bar",
            progress=progress,
            journal=journal,
            cancel_event=cancel_event,
            max_in_flight_multiplier=runtime_config.max_in_flight_multiplier,
            heartbeat_interval_seconds=runtime_config.heartbeat_seconds,
        )
        # Open one ManifestWriteSession (one DuckDB connection) for the entire
        # build and attach it to the coordinator. The coordinator is the sole
        # metadata writer.
        with store._metadata_store.manifest_write_session() as session:
            coordinator.attach_manifest_session(
                session,
                run_id=context.run_id,
                writer_thread="coordinator",
            )
            counters = coordinator.run(plan)
        cancelled = cancel_event.is_set()
        if cancelled:
            progress.set_stage(STAGE_CANCELLING, total=plan.total)
        committed = counters.committed
        failed = counters.failed
    finally:
        restore_signals()

    # When building for the entire master (no explicit security_ids filter),
    # remove target partitions whose security_id is no longer in the master.
    if not security_ids:
        _remove_stale_target_partitions(store, "cn_stock_daily_bar", effective_master)

    # Clean up stale manifests and staging leftovers.
    cleanup_stale_derived_manifests(store, "cn_stock_daily_bar")
    cleanup_stale_derived_staging(store, "cn_stock_daily_bar")

    # ------------------------------------------------------------------
    # Stage 5: Finalize
    # ------------------------------------------------------------------
    if cancelled:
        final_status = STAGE_CANCELLED
        journal_status = JOURNAL_STATUS_CANCELLED
        build_status = "cancelled"
    elif failed > 0:
        final_status = STAGE_FAILED
        journal_status = JOURNAL_STATUS_FAILED
        build_status = "partial"
    else:
        final_status = STAGE_COMPLETED
        journal_status = JOURNAL_STATUS_COMPLETED
        build_status = "success"
    journal.finalize(journal_status)
    progress.final(final_status)

    cleanup_old_journals(store.metadata_dir, keep_recent=20, now=timestamp)
    _finalize_build(store, build_views, refresh_registry, ("cn_stock_daily_bar",))

    return {
        "dataset": "cn_stock_daily_bar",
        "status": build_status,
        "rows": counters.rows,
        "partitions": committed,
        "failed": failed,
        "cancelled": cancelled,
        "run_id": context.run_id,
        "plan_hash": plan.plan_hash,
        "source_snapshot_hash": plan.source_snapshot_hash,
    }


# ---------------------------------------------------------------------------
# Pure materialize function (no manifest/lock/state dependencies)
# ---------------------------------------------------------------------------


def materialize_security_daily_bar(
    security: pd.Series,
    source_frames: Mapping[str, pd.DataFrame],
    *,
    updated_at: datetime,
) -> pd.DataFrame:
    """Pure transformation: map source DataFrames into the canonical daily-bar schema.

    This function does NOT:
    - Query manifests
    - Acquire process locks
    - Update state files
    - Update journals
    - Write metadata
    - Decide whether to build incrementally
    - Create global staging
    - Build DuckDB views

    It accepts pre-loaded source frames keyed by ``dataset_id`` and returns a
    single DataFrame conforming to ``CN_STOCK_DAILY_BAR_SCHEMA``.
    """

    frames: list[pd.DataFrame] = []
    baostock_code = _clean_string(security.get("baostock_code"))
    akshare_code = _clean_string(security.get("akshare_code"))

    if baostock_code:
        for dataset_id, adjustment in BAOSTOCK_DAILY_SOURCES.items():
            source = source_frames.get(dataset_id)
            if source is None or source.empty:
                continue
            frames.append(_map_baostock_daily(source, dataset_id, adjustment, security, updated_at))

    if akshare_code:
        for dataset_id, adjustment in AKSHARE_DAILY_SOURCES.items():
            source = source_frames.get(dataset_id)
            if source is None or source.empty:
                continue
            frames.append(_map_akshare_daily(source, dataset_id, adjustment, security, updated_at))

    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame()
    combined = pd.concat(non_empty, ignore_index=True)
    combined["_source_rank"] = _assign_source_rank(combined)
    combined = (
        combined.sort_values(["date", "security_id", "adjustment", "_source_rank"], kind="mergesort")
        .drop_duplicates(["date", "security_id", "adjustment"], keep="first")
        .drop(columns=["_source_rank"])
        .sort_values(["security_id", "adjustment", "date"])
        .reset_index(drop=True)
    )
    return combined


def _pure_materialize_wrapper(updated_at: datetime) -> Callable[[pd.Series, Mapping[str, pd.DataFrame]], pd.DataFrame]:
    """Return a closure matching the executor's ``MaterializeFn`` signature."""

    def _materialize(security: pd.Series, source_frames: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
        return materialize_security_daily_bar(security, source_frames, updated_at=updated_at)

    return _materialize


# ---------------------------------------------------------------------------
# Source partition reader (used by the executor)
# ---------------------------------------------------------------------------


def _read_source_partition(store: ParquetStore, dataset_id: str, partition_value: str) -> pd.DataFrame:
    """Read one source partition, returning an empty frame if missing."""

    definition = DATASET_CATALOG[dataset_id]
    partition_column = definition.partition_column
    if partition_column is None:
        return store.read_dataset(dataset_id)
    return store.read_dataset(dataset_id, {partition_column: partition_value})


def _progress_path_override_from_env() -> Path | None:
    """Read the orchestrator-pinned progress path from the environment.

    The daily orchestrator sets ``QDC_DERIVED_PROGRESS_PATH`` before spawning
    a derived build subprocess so its stall detector can read the exact file
    the child writes. When the variable is absent (manual CLI invocation),
    ``None`` is returned and the journal-derived default path is used.

    This is the ONLY place the derived build reads this env var, keeping the
    progress-path contract in one location instead of scattered across the
    business entrypoints.
    """

    raw = os.environ.get("QDC_DERIVED_PROGRESS_PATH")
    if not raw:
        return None
    return Path(raw).expanduser().resolve()


def _install_cancel_signal_handler() -> tuple[Event, Callable[[], None]]:
    """Install a SIGINT/SIGTERM handler that sets a cancel event.

    Returns a tuple of ``(cancel_event, restore_fn)``. The ``cancel_event`` is
    passed to the :class:`PartitionExecutor` so that a Ctrl+C (from the user
    or from the orchestrator's stall detector) triggers a cooperative shutdown:
    the executor stops submitting new work, running workers finish their
    current partition, and the coordinator commits whatever was completed.

    ``restore_fn`` must be called in a ``finally`` block to restore the
    previous signal handlers. This is important when the build runs in-process
    (e.g. during tests) so that subsequent signal handling is not affected.

    On platforms where ``signal.signal`` cannot be installed (e.g. inside a
    worker thread), this is a no-op and returns an unset event plus a no-op
    restore function.
    """

    cancel_event = Event()

    def _on_signal(signum: int, frame: Any) -> None:
        logger.warning(
            "Received signal {} during derived build; setting cancel event "
            "(running workers will finish, no new work will be submitted)",
            signum,
        )
        cancel_event.set()

    previous_int = signal.getsignal(signal.SIGINT)
    try:
        signal.signal(signal.SIGINT, _on_signal)
    except (ValueError, OSError):
        # ValueError: not in the main thread.
        # OSError: platform doesn't support this signal.
        return cancel_event, lambda: None

    previous_term = None
    if hasattr(signal, "SIGTERM"):
        previous_term = signal.getsignal(signal.SIGTERM)
        try:
            signal.signal(signal.SIGTERM, _on_signal)
        except (ValueError, OSError):
            previous_term = None

    def _restore() -> None:
        try:
            signal.signal(signal.SIGINT, previous_int)
        except (ValueError, OSError, TypeError):
            pass
        if previous_term is not None and hasattr(signal, "SIGTERM"):
            try:
                signal.signal(signal.SIGTERM, previous_term)
            except (ValueError, OSError, TypeError):
                pass

    return cancel_event, _restore


# ---------------------------------------------------------------------------
# Security master helpers
# ---------------------------------------------------------------------------


def _read_or_build_master(store: ParquetStore, now: Callable[[], datetime] | None) -> pd.DataFrame:
    master = store.read_dataset("cn_security_master")
    if not store.dataset_exists("cn_security_master") or master.empty:
        build_security_master(root=store.root, build_views=False, refresh_registry=False, now=now)
        master = store.read_dataset("cn_security_master")
    return master


def _build_security_index(master: pd.DataFrame) -> dict[str, pd.Series]:
    """Build a ``security_id → pd.Series`` lookup for the executor."""

    index: dict[str, pd.Series] = {}
    if master.empty:
        return index
    for _, row in master.iterrows():
        security_id = _clean_string(row.get("security_id"))
        if security_id:
            index[security_id] = row
    return index


def _remove_stale_target_partitions(
    store: ParquetStore,
    dataset_id: str,
    master: pd.DataFrame,
) -> int:
    """Remove target partitions whose ``security_id`` is not in ``master``.

    This replaces the stale-partition cleanup that the old full-dataset
    staging performed implicitly: when the entire master is rebuilt, any
    partition whose security has been delisted (or was never present) must be
    removed so the dataset does not accumulate orphan partitions. Returns the
    number of removed partitions.
    """

    if master.empty or "security_id" not in master.columns:
        return 0
    keep_ids = {_clean_string(value) for value in master["security_id"]}
    if not keep_ids:
        return 0
    existing_partitions = store.list_dataset_partitions(dataset_id)
    removed = 0
    for partition_value in existing_partitions:
        if partition_value in keep_ids:
            continue
        try:
            # ``dataset_path`` returns ``<dir>/security_id=<value>/data.parquet``;
            # we need to remove the parent directory (the partition folder).
            partition_file = store.dataset_path(dataset_id, {"security_id": partition_value})
            partition_dir = partition_file.parent
            if partition_dir.exists():
                shutil.rmtree(partition_dir, ignore_errors=True)
            delete_derived_partition_manifest(store, dataset_id, partition_value)
            removed += 1
        except Exception:
            logger.warning(
                "Failed to remove stale target partition {} from {}",
                partition_value,
                dataset_id,
                exc_info=True,
            )
    return removed


# ---------------------------------------------------------------------------
# Field mapping (pure helpers, kept for backwards compat with tests)
# ---------------------------------------------------------------------------


def _materialize_security_daily_bar(
    store: ParquetStore,
    security: pd.Series,
    updated_at: datetime,
    partition_cache: Mapping[str, set[str]],
) -> pd.DataFrame:
    """Legacy adapter: read source frames from ``store`` then call the pure function.

    Kept for backwards compatibility with existing tests that monkeypatch this
    name. The new pipeline calls :func:`materialize_security_daily_bar`
    directly via the executor.
    """

    frames: dict[str, pd.DataFrame] = {}
    for dataset_id in (*BAOSTOCK_DAILY_SOURCES, *AKSHARE_DAILY_SOURCES):
        code_field = "baostock_code" if dataset_id in BAOSTOCK_DAILY_SOURCES else "akshare_code"
        code = _clean_string(security.get(code_field))
        if not code:
            continue
        partition_column = DATASET_CATALOG[dataset_id].partition_column
        if partition_column is None:
            frames[dataset_id] = store.read_dataset(dataset_id)
            continue
        if code not in partition_cache.get(dataset_id, set()):
            continue
        frames[dataset_id] = store.read_dataset(dataset_id, {partition_column: code})
    return materialize_security_daily_bar(security, frames, updated_at=updated_at)


def _map_baostock_daily(
    df: pd.DataFrame,
    dataset_id: str,
    adjustment: str,
    security: pd.Series,
    updated_at: datetime,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=CN_STOCK_DAILY_BAR_COLUMNS)

    out = pd.DataFrame(index=df.index)
    out["date"] = _column_or_none(df, "date")
    out["security_id"] = security.get("security_id")
    out["code"] = security.get("code")
    out["exchange"] = security.get("exchange")
    out["name"] = security.get("name")
    out["adjustment"] = adjustment
    out["open"] = _column_or_none(df, "open")
    out["high"] = _column_or_none(df, "high")
    out["low"] = _column_or_none(df, "low")
    out["close"] = _column_or_none(df, "close")
    out["prev_close"] = _column_or_none(df, "prev_close")
    out["volume"] = _column_or_none(df, "volume")
    out["amount"] = _column_or_none(df, "amount")
    out["turnover_rate"] = _column_or_none(df, "turnover_rate")
    out["pct_change"] = _column_or_none(df, "pct_change")
    out["trade_status"] = _column_or_none(df, "trade_status")
    out["is_st"] = _column_or_none(df, "is_st")
    out["is_active"] = security.get("is_active")
    out["source_dataset"] = dataset_id
    out["source_endpoint"] = "query_history_k_data_plus"
    out["quality_status"] = "daily_bar_confirmed"
    out["updated_at"] = updated_at
    return pd.DataFrame(out, columns=CN_STOCK_DAILY_BAR_COLUMNS)


def _map_akshare_daily(
    df: pd.DataFrame,
    dataset_id: str,
    adjustment: str,
    security: pd.Series,
    updated_at: datetime,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=CN_STOCK_DAILY_BAR_COLUMNS)

    out = pd.DataFrame(index=df.index)
    out["date"] = _column_or_none(df, "date")
    out["security_id"] = security.get("security_id")
    out["code"] = security.get("code")
    out["exchange"] = security.get("exchange")
    out["name"] = security.get("name")
    out["adjustment"] = adjustment
    out["open"] = _column_or_none(df, "open")
    out["high"] = _column_or_none(df, "high")
    out["low"] = _column_or_none(df, "low")
    out["close"] = _column_or_none(df, "close")
    out["prev_close"] = None
    out["volume"] = _column_or_none(df, "volume")
    out["amount"] = _column_or_none(df, "amount")
    out["turnover_rate"] = _column_or_none(df, "turnover_rate")
    out["pct_change"] = _column_or_none(df, "pct_change")
    out["trade_status"] = None
    out["is_st"] = None
    out["is_active"] = security.get("is_active")
    out["source_dataset"] = dataset_id
    out["source_endpoint"] = _string_column_with_default(df, "source_endpoint", "stock_zh_a_hist")
    out["quality_status"] = _string_column_with_default(df, "quality_status", "daily_bar_confirmed")
    out["updated_at"] = updated_at
    return pd.DataFrame(out, columns=CN_STOCK_DAILY_BAR_COLUMNS)


def _assign_source_rank(combined: pd.DataFrame) -> pd.Series:
    source_dataset = _string_column_with_default(combined, "source_dataset", "")
    quality_status = _string_column_with_default(combined, "quality_status", "")
    rank = pd.Series(3, index=combined.index)
    rank.loc[quality_status == "spot_quote_close"] = 2
    rank.loc[quality_status == "daily_bar_confirmed"] = 1
    rank.loc[source_dataset.str.startswith("baostock_")] = 0
    return rank


def _column_or_none(df: pd.DataFrame, column: str) -> pd.Series:
    if column in df.columns:
        return df[column]
    return pd.Series([None] * len(df), index=df.index)


def _string_column_with_default(df: pd.DataFrame, column: str, default: str) -> pd.Series:
    if column in df.columns:
        series = df[column].astype("string").str.strip()
    else:
        series = pd.Series(pd.NA, index=df.index, dtype="string")
    return series.mask(series.isna() | (series == ""), default)


def _clean_string(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


# ---------------------------------------------------------------------------
# Legacy partition cache helpers (kept for backwards compat with tests)
# ---------------------------------------------------------------------------


def _build_daily_source_partition_cache(store: ParquetStore) -> dict[str, set[str]]:
    partition_cache: dict[str, set[str]] = {}
    for dataset_id in (*BAOSTOCK_DAILY_SOURCES, *AKSHARE_DAILY_SOURCES):
        if DATASET_CATALOG[dataset_id].partition_column is None:
            partition_cache[dataset_id] = set()
            continue
        partition_cache[dataset_id] = set(store.list_dataset_partitions(dataset_id))
    return partition_cache


def _filter_master_to_daily_source_candidates(
    master: pd.DataFrame,
    partition_cache: Mapping[str, set[str]],
) -> pd.DataFrame:
    if master.empty:
        return master

    baostock_partitions = set().union(
        *(partition_cache.get(dataset_id, set()) for dataset_id in BAOSTOCK_DAILY_SOURCES)
    )
    akshare_partitions = set().union(*(partition_cache.get(dataset_id, set()) for dataset_id in AKSHARE_DAILY_SOURCES))

    baostock_codes = [_clean_string(value) for value in _column_or_none(master, "baostock_code")]
    akshare_codes = [_clean_string(value) for value in _column_or_none(master, "akshare_code")]
    keep = [
        (bool(baostock_code) and baostock_code in baostock_partitions)
        or (bool(akshare_code) and akshare_code in akshare_partitions)
        for baostock_code, akshare_code in zip(baostock_codes, akshare_codes, strict=True)
    ]
    return master.loc[keep].reset_index(drop=True)


def _finalize_build(
    store: ParquetStore,
    build_views: bool,
    refresh_registry: bool,
    dataset_ids: tuple[str, ...],
) -> None:
    if refresh_registry:
        refresh_derived_registry(store, dataset_ids)
    if build_views:
        DuckDBStore(root=store.root).build_views()
