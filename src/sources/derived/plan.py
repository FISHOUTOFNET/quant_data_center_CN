"""Derived build planning: batch manifest snapshot and signature-once plan.

This module eliminates two algorithmic wastes in the previous derived build:

1. **Manifest N+1 query**: the old ``_changed_security_ids_for_target`` loop
   called ``current_source_signature_for_security`` per security, which in turn
   called ``store.read_dataset_partition_manifest`` per (dataset, partition)
   pair. With ~8000 securities and 6 source datasets this produced tens of
   thousands of DuckDB round-trips.

2. **Repeated source signature**: the old ``build_cn_stock_daily_bar`` called
   ``current_source_signature_for_security`` again inside the build loop, even
   though the planner had already computed the same signature to decide
   whether the partition needed rebuilding.

The :class:`BuildPlanner` loads all source manifests in a single batch query,
builds an in-memory index, computes each security's source signature exactly
once, and emits an immutable :class:`DerivedBuildPlan` that the executor
consumes directly.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

import pandas as pd

from src.sources.derived.manifest import SourcePartition
from src.storage.dataset_catalog import dataset_definition
from src.storage.manifest_rebuild import rebuild_one_partition_manifest
from src.storage.parquet_store import ParquetStore
from src.storage.partition_manifest import master_row_hash, source_signature
from src.utils.logging import logger


class ChangeReason(str, Enum):
    """Why a partition was selected for (re)build."""

    SOURCE_CHANGED = "source_changed"
    TARGET_MISSING = "target_missing"
    TARGET_MANIFEST_MISSING = "target_manifest_missing"
    SOURCE_MANIFEST_MISSING = "source_manifest_missing"
    MANIFEST_MISSING = "manifest_missing"  # Legacy alias; new code uses the specific reasons above.
    FORCE_REBUILD = "force_rebuild"
    SCHEMA_CHANGED = "schema_changed"


@dataclass(frozen=True)
class PartitionManifestSnapshot:
    """In-memory index of manifest rows for a set of datasets.

    The index key is ``(dataset_id, partition_column, partition_value)`` so the
    planner can look up any source partition in O(1) without re-querying DuckDB.
    """

    rows: pd.DataFrame
    _index: dict[tuple[str, str, str], pd.Series] | None = None

    def index(self) -> dict[tuple[str, str, str], pd.Series]:
        """Lazily build and cache the lookup index."""

        if self._index is not None:
            return self._index
        index: dict[tuple[str, str, str], pd.Series] = {}
        if not self.rows.empty:
            for _, row in self.rows.iterrows():
                key = (
                    _clean_string(row.get("dataset")),
                    _clean_string(row.get("partition_column")),
                    _clean_string(row.get("partition_value")),
                )
                index[key] = row
        object.__setattr__(self, "_index", index)
        return index

    def lookup(
        self,
        dataset_id: str,
        partition_column: str,
        partition_value: str,
    ) -> pd.Series | None:
        """Return the manifest row for the given key, or ``None``."""

        return self.index().get((dataset_id, partition_column, partition_value))

    def restrict(self, dataset_id: str) -> PartitionManifestSnapshot:
        """Return a new snapshot containing only rows for ``dataset_id``.

        Used to extract the target dataset's manifest rows from a combined
        source+target batch query without issuing a second single-dataset
        query.
        """

        if self.rows.empty:
            return self
        mask = self.rows["dataset"].astype("string") == dataset_id
        filtered = self.rows.loc[mask].reset_index(drop=True)
        return PartitionManifestSnapshot(rows=filtered)

    @classmethod
    def empty(cls) -> PartitionManifestSnapshot:
        return cls(rows=pd.DataFrame())


@dataclass(frozen=True)
class DerivedPartitionPlan:
    """One partition's build plan, with source signature pre-computed."""

    security_id: str
    source_signature: str
    master_row_hash: str
    source_partitions: tuple[SourcePartition, ...]
    change_reason: ChangeReason
    source_rows: pd.DataFrame | None = None


@dataclass(frozen=True)
class DerivedBuildPlan:
    """Immutable output of :class:`BuildPlanner`."""

    target: str
    dataset_id: str
    partitions: tuple[DerivedPartitionPlan, ...]
    source_snapshot_hash: str
    plan_hash: str
    schema_version: str = "1"

    @property
    def total(self) -> int:
        return len(self.partitions)

    @property
    def security_ids(self) -> tuple[str, ...]:
        return tuple(item.security_id for item in self.partitions)


class BuildPlanner:
    """Plan a derived build by loading source manifests in batch and computing
    each security's source signature exactly once.
    """

    def __init__(
        self,
        store: ParquetStore,
        target: str,
        *,
        dataset_id: str,
        source_dataset_specs: tuple[tuple[str, str], ...],
        master: pd.DataFrame,
        changed_since: datetime | None = None,
        force_rebuild: bool = False,
    ) -> None:
        """``source_dataset_specs`` is a tuple of ``(dataset_id, code_field)``
        pairs describing where to find each source partition for a security.
        """

        self.store = store
        self.target = target
        self.dataset_id = dataset_id
        self.source_dataset_specs = source_dataset_specs
        self.master = master
        self.changed_since = changed_since
        self.force_rebuild = force_rebuild

    def plan(self) -> DerivedBuildPlan:
        source_dataset_ids = tuple(spec[0] for spec in self.source_dataset_specs)
        # Load source AND target manifests in a single batch query to avoid
        # any per-dataset single queries in the planner. The target snapshot
        # is extracted from the same batch result.
        all_dataset_ids = (*source_dataset_ids, self.dataset_id)
        combined_snapshot = self._load_snapshot(all_dataset_ids)
        target_snapshot = combined_snapshot.restrict(self.dataset_id)
        missing = self._find_missing_source_manifests(combined_snapshot, source_dataset_ids)
        if missing:
            self._repair_missing_manifests(missing)
            combined_snapshot = self._load_snapshot(all_dataset_ids)
            target_snapshot = combined_snapshot.restrict(self.dataset_id)

        partitions = self._compute_partition_plans(combined_snapshot, target_snapshot)
        source_snapshot_hash = _hash_source_snapshot(combined_snapshot, self.dataset_id)
        plan_hash = _hash_plan(self.target, self.dataset_id, partitions, source_snapshot_hash)
        return DerivedBuildPlan(
            target=self.target,
            dataset_id=self.dataset_id,
            partitions=tuple(partitions),
            source_snapshot_hash=source_snapshot_hash,
            plan_hash=plan_hash,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_snapshot(
        self,
        dataset_ids: Collection[str],
    ) -> PartitionManifestSnapshot:
        rows = self.store.read_dataset_partition_manifest_batch(dataset_ids)
        return PartitionManifestSnapshot(rows=rows)

    def _find_missing_source_manifests(
        self,
        snapshot: PartitionManifestSnapshot,
        dataset_ids: Collection[str],
    ) -> list[tuple[str, str, str]]:
        """Find source partitions that exist on disk but have no manifest row."""

        missing: list[tuple[str, str, str]] = []
        for dataset_id in dataset_ids:
            definition = dataset_definition(dataset_id)
            if definition.partition_column is None:
                continue
            existing_partitions = set(self.store.list_dataset_partitions(dataset_id))
            for partition_value in existing_partitions:
                if snapshot.lookup(dataset_id, definition.partition_column, partition_value) is None:
                    missing.append((dataset_id, definition.partition_column, partition_value))
        return missing

    def _repair_missing_manifests(
        self,
        missing: list[tuple[str, str, str]],
    ) -> None:
        """Batch-repair missing source manifests (preflight, not in the hot loop)."""

        logger.info(
            "Derived build preflight: repairing {} missing source manifest(s) for target={}",
            len(missing),
            self.target,
        )
        for dataset_id, partition_column, partition_value in missing:
            try:
                rebuild_one_partition_manifest(
                    store=self.store,
                    dataset_id=dataset_id,
                    partition_value=partition_value,
                    force=True,
                )
            except Exception:
                logger.exception(
                    "Derived build preflight: failed to rebuild manifest for {} partition={}",
                    dataset_id,
                    partition_value,
                )

    def _compute_partition_plans(
        self,
        source_snapshot: PartitionManifestSnapshot,
        target_snapshot: PartitionManifestSnapshot,
    ) -> list[DerivedPartitionPlan]:
        plans: list[DerivedPartitionPlan] = []
        if self.master.empty:
            return plans
        for _, security in self.master.iterrows():
            security_id = _clean_string(security.get("security_id"))
            if not security_id:
                continue
            source_pairs = self._source_pairs_for_security(security)
            target_path = self.store.dataset_path(self.dataset_id, {"security_id": security_id})
            target_exists = target_path.exists()
            if not source_pairs:
                # No source partitions exist for this security. If the target
                # partition also doesn't exist, there's nothing to do. If the
                # target exists (e.g. upstream source was deleted after a
                # previous build), schedule a deletion by emitting a plan with
                # empty source partitions: the executor will produce an empty
                # DataFrame and the coordinator will remove the target.
                if not target_exists:
                    continue
                plans.append(
                    DerivedPartitionPlan(
                        security_id=security_id,
                        source_signature="",
                        master_row_hash=master_row_hash(security),
                        source_partitions=(),
                        change_reason=ChangeReason.SOURCE_CHANGED,
                    )
                )
                continue
            source_rows = self._collect_source_rows(source_snapshot, source_pairs)
            if source_rows is None:
                # Source manifest missing even after preflight repair. This is
                # an unrecoverable data-integrity error for this partition: we
                # must NOT build (we cannot compute a source signature), and we
                # must NOT report success. Emit a plan flagged
                # SOURCE_MANIFEST_MISSING so the executor records a failure.
                plans.append(
                    DerivedPartitionPlan(
                        security_id=security_id,
                        source_signature="",
                        master_row_hash="",
                        source_partitions=tuple(source_pairs),
                        change_reason=ChangeReason.SOURCE_MANIFEST_MISSING,
                    )
                )
                continue
            current_signature = source_signature(source_rows, master_row_hash(security))
            current_master_hash = master_row_hash(security)
            if not target_exists:
                reason = ChangeReason.TARGET_MISSING
            elif self.force_rebuild:
                reason = ChangeReason.FORCE_REBUILD
            else:
                target_manifest = _target_manifest_row(target_snapshot, security_id)
                if target_manifest is None:
                    # Target parquet exists but its manifest row is missing.
                    # This is the recovery window for "file committed but
                    # manifest write failed". We rebuild the partition through
                    # the normal path so the manifest is restored. This is
                    # distinct from SOURCE_MANIFEST_MISSING (which fails).
                    reason = ChangeReason.TARGET_MANIFEST_MISSING
                else:
                    target_signature = _clean_string(target_manifest.get("source_signature"))
                    target_master_hash = _clean_string(target_manifest.get("master_row_hash"))
                    if (
                        not target_signature
                        or current_signature != target_signature
                        or current_master_hash != target_master_hash
                        or _source_updated_since(source_rows, self.changed_since)
                    ):
                        reason = ChangeReason.SOURCE_CHANGED
                    else:
                        continue  # Unchanged — skip entirely.
            plans.append(
                DerivedPartitionPlan(
                    security_id=security_id,
                    source_signature=current_signature,
                    master_row_hash=current_master_hash,
                    source_partitions=tuple(source_pairs),
                    change_reason=reason,
                    source_rows=source_rows,
                )
            )
        return plans

    def _source_pairs_for_security(self, security: pd.Series) -> list[SourcePartition]:
        pairs: list[SourcePartition] = []
        for dataset_id, code_field in self.source_dataset_specs:
            definition = dataset_definition(dataset_id)
            if definition.partition_column is None:
                continue
            code = _clean_string(security.get(code_field))
            if code and self.store.dataset_exists(dataset_id, {definition.partition_column: code}):
                pairs.append((dataset_id, code))
        return pairs

    def _collect_source_rows(
        self,
        snapshot: PartitionManifestSnapshot,
        pairs: list[SourcePartition],
    ) -> pd.DataFrame | None:
        rows: list[pd.Series] = []
        for dataset_id, partition_value in pairs:
            definition = dataset_definition(dataset_id)
            partition_column = definition.partition_column or ""
            row = snapshot.lookup(dataset_id, partition_column, partition_value)
            if row is None:
                return None
            rows.append(row)
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows).reset_index(drop=True)


def _target_manifest_row(snapshot: PartitionManifestSnapshot, security_id: str) -> pd.Series | None:
    if snapshot.rows.empty:
        return None
    matched = snapshot.rows.loc[
        (snapshot.rows["partition_column"].astype("string") == "security_id")
        & (snapshot.rows["partition_value"].astype("string") == security_id)
    ]
    if matched.empty:
        return None
    return matched.iloc[-1]


def _source_updated_since(source_rows: pd.DataFrame, changed_since: datetime | None) -> bool:
    if changed_since is None or source_rows.empty or "updated_at" not in source_rows.columns:
        return False
    updated_at = pd.to_datetime(source_rows["updated_at"], errors="coerce")
    return bool((updated_at >= pd.Timestamp(changed_since)).any())


def _hash_source_snapshot(snapshot: PartitionManifestSnapshot, dataset_id: str) -> str:
    """Stable hash of the source manifest rows that feed into ``dataset_id``."""

    if snapshot.rows.empty:
        return hashlib.sha256(f"{dataset_id}:empty".encode("utf-8")).hexdigest()
    payload = snapshot.rows.sort_values(
        by=["dataset", "partition_column", "partition_value"]
    )[["dataset", "partition_column", "partition_value", "semantic_hash", "source_signature"]]
    return hashlib.sha256(
        json.dumps(
            {
                "dataset": dataset_id,
                "rows": payload.to_dict(orient="records"),
            },
            sort_keys=True,
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _hash_plan(
    target: str,
    dataset_id: str,
    partitions: list[DerivedPartitionPlan],
    source_snapshot_hash: str,
) -> str:
    payload = {
        "target": target,
        "dataset_id": dataset_id,
        "source_snapshot_hash": source_snapshot_hash,
        "security_ids": [p.security_id for p in partitions],
        "signatures": [p.source_signature for p in partitions],
        "reasons": [p.change_reason.value for p in partitions],
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def _clean_string(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()
