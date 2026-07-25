"""Lightweight build progress tracking for derived builds.

Reports structured progress to the run log and an optional state file so that
external monitors (and the daily orchestrator) can see:

* current stage (PLANNING, BUILDING_PARTITIONS, COMMITTING, …)
* processed / total / failed counts
* current security_id
* throughput (partitions per minute)
* heartbeat timestamp

The reporter is intentionally throttled: it writes at most once every
``min_interval_seconds`` or every ``flush_every`` partitions, whichever comes
first. This avoids I/O overhead in the hot loop.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Any

from src.utils.logging import logger

STAGE_PLANNING = "PLANNING"
STAGE_REPAIRING_MANIFEST = "REPAIRING_MANIFEST"
STAGE_BUILDING_PARTITIONS = "BUILDING_PARTITIONS"
STAGE_COMMITTING = "COMMITTING"
STAGE_REFRESHING_VIEWS = "REFRESHING_VIEWS"
STAGE_COMPLETED = "COMPLETED"
STAGE_FAILED = "FAILED"
STAGE_CANCELLING = "CANCELLING"
STAGE_CANCELLED = "CANCELLED"

DEFAULT_FLUSH_INTERVAL_SECONDS = 30.0
DEFAULT_FLUSH_EVERY = 50


@dataclass
class ProgressState:
    """Mutable progress state. Reads are snapshot-consistent under the lock."""

    stage: str = STAGE_PLANNING
    processed: int = 0
    total: int = 0
    failed: int = 0
    current_security_id: str | None = None
    started_at: datetime = field(default_factory=datetime.now)
    heartbeat_at: datetime = field(default_factory=datetime.now)
    last_flush_at: float = field(default_factory=time.monotonic)
    failed_security_ids: list[str] = field(default_factory=list)

    def snapshot(self) -> dict[str, Any]:
        elapsed = max((datetime.now() - self.started_at).total_seconds(), 0.1)
        throughput = (self.processed / elapsed) * 60.0 if self.processed else 0.0
        return {
            "stage": self.stage,
            "processed": self.processed,
            "total": self.total,
            "failed": self.failed,
            "current_security_id": self.current_security_id,
            "throughput_per_minute": round(throughput, 2),
            "heartbeat_at": datetime.now().isoformat(timespec="seconds"),
            "started_at": self.started_at.isoformat(timespec="seconds"),
            "failed_security_ids": list(self.failed_security_ids),
        }


class ProgressReporter:
    """Thread-safe progress reporter with throttled state-file writes."""

    def __init__(
        self,
        *,
        state_path: Path | None = None,
        run_log_path: Path | None = None,
        flush_interval_seconds: float = DEFAULT_FLUSH_INTERVAL_SECONDS,
        flush_every: int = DEFAULT_FLUSH_EVERY,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._state_path = state_path
        self._run_log_path = run_log_path
        self._flush_interval = flush_interval_seconds
        self._flush_every = flush_every
        self._clock = clock or time.monotonic
        self._lock = RLock()
        self._state = ProgressState()

    @property
    def state(self) -> ProgressState:
        return self._state

    def set_stage(self, stage: str, *, total: int | None = None) -> None:
        with self._lock:
            self._state.stage = stage
            if total is not None:
                self._state.total = total
            self._state.heartbeat_at = datetime.now()
        self._maybe_flush(force=True)

    def record_processed(self, security_id: str | None = None) -> None:
        with self._lock:
            self._state.processed += 1
            self._state.current_security_id = security_id
            self._state.heartbeat_at = datetime.now()
        self._maybe_flush()

    def record_failed(self, security_id: str) -> None:
        with self._lock:
            self._state.failed += 1
            self._state.failed_security_ids.append(security_id)
            self._state.heartbeat_at = datetime.now()
        self._maybe_flush()

    def heartbeat(self) -> None:
        with self._lock:
            self._state.heartbeat_at = datetime.now()
        self._maybe_flush(force=True)

    def final(self, stage: str = STAGE_COMPLETED) -> dict[str, Any]:
        with self._lock:
            self._state.stage = stage
            self._state.heartbeat_at = datetime.now()
            snapshot = self._state.snapshot()
        self._write_state(snapshot)
        self._write_log(snapshot)
        return snapshot

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return self._state.snapshot()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _maybe_flush(self, *, force: bool = False) -> None:
        with self._lock:
            now = self._clock()
            time_due = (now - self._state.last_flush_at) >= self._flush_interval
            count_due = self._state.processed > 0 and self._state.processed % self._flush_every == 0
            if not force and not time_due and not count_due:
                return
            self._state.last_flush_at = now
            snapshot = self._state.snapshot()
        self._write_state(snapshot)
        if count_due or force:
            self._write_log(snapshot)

    def _write_state(self, snapshot: dict[str, Any]) -> None:
        if self._state_path is None:
            return
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._state_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(self._state_path)
        except OSError:
            logger.warning("Failed to write derived progress state to {}", self._state_path)

    def _write_log(self, snapshot: dict[str, Any]) -> None:
        line = (
            f"[derived-progress] stage={snapshot['stage']} "
            f"processed={snapshot['processed']}/{snapshot['total']} "
            f"failed={snapshot['failed']} "
            f"current={snapshot['current_security_id']} "
            f"throughput={snapshot['throughput_per_minute']}/min"
        )
        if self._run_log_path is not None:
            try:
                with self._run_log_path.open("a", encoding="utf-8") as handle:
                    handle.write(line + "\n")
            except OSError:
                pass
        logger.info(line)


# ---------------------------------------------------------------------------
# Stall detection (consumed by the orchestrator)
# ---------------------------------------------------------------------------


DEFAULT_STALL_HEARTBEAT_SECONDS = 25 * 60  # 25 minutes
DEFAULT_SAFETY_TIMEOUT_SECONDS = 18 * 60 * 60  # 18 hours


@dataclass(frozen=True)
class StallReport:
    """Result of a stall check on a derived build's progress state file."""

    stalled: bool
    reason: str | None
    heartbeat_age_seconds: float
    processed: int
    total: int


def read_progress_state(state_path: Path) -> dict[str, Any] | None:
    """Read the most recent progress state snapshot, or ``None`` if missing."""

    if not state_path.exists():
        return None
    try:
        data = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def check_stall(
    state_path: Path,
    *,
    now: datetime | None = None,
    stall_heartbeat_seconds: float = DEFAULT_STALL_HEARTBEAT_SECONDS,
    previous_processed: int | None = None,
) -> StallReport:
    """Check whether a derived build is stalled.

    A build is stalled when **all** of the following are true:

    1. The progress state file exists (build has started).
    2. The stage is not terminal (``COMPLETED`` / ``FAILED`` / ``CANCELLED``).
    3. The heartbeat is older than ``stall_heartbeat_seconds``.
    4. ``processed`` has not changed since the previous check.

    Condition 4 requires the caller to pass the previous ``processed`` value
    from the prior poll. On the first poll (``previous_processed=None``), only
    conditions 1-3 are checked.

    The caller (orchestrator) is expected to poll this every 60 seconds and
    terminate the child process when ``stalled`` is ``True``.
    """

    reference = now or datetime.now()
    snapshot = read_progress_state(state_path)
    if snapshot is None:
        return StallReport(
            stalled=False,
            reason="state file not found (build may not have started yet)",
            heartbeat_age_seconds=0.0,
            processed=0,
            total=0,
        )
    stage = str(snapshot.get("stage", ""))
    if stage in {STAGE_COMPLETED, STAGE_FAILED, STAGE_CANCELLED}:
        return StallReport(
            stalled=False,
            reason=f"stage is terminal: {stage}",
            heartbeat_age_seconds=0.0,
            processed=int(snapshot.get("processed", 0)),
            total=int(snapshot.get("total", 0)),
        )
    heartbeat_str = snapshot.get("heartbeat_at")
    heartbeat_at = _parse_iso_timestamp(heartbeat_str)
    if heartbeat_at is None:
        return StallReport(
            stalled=False,
            reason="heartbeat_at missing or unparseable",
            heartbeat_age_seconds=0.0,
            processed=int(snapshot.get("processed", 0)),
            total=int(snapshot.get("total", 0)),
        )
    age = (reference - heartbeat_at).total_seconds()
    processed = int(snapshot.get("processed", 0))
    total = int(snapshot.get("total", 0))
    if age < stall_heartbeat_seconds:
        return StallReport(
            stalled=False,
            reason=f"heartbeat is fresh ({age:.0f}s old)",
            heartbeat_age_seconds=age,
            processed=processed,
            total=total,
        )
    if previous_processed is not None and processed != previous_processed:
        return StallReport(
            stalled=False,
            reason=f"processed changed ({previous_processed} → {processed})",
            heartbeat_age_seconds=age,
            processed=processed,
            total=total,
        )
    return StallReport(
        stalled=True,
        reason=(
            f"heartbeat {age:.0f}s old (threshold {stall_heartbeat_seconds:.0f}s) "
            f"and processed unchanged at {processed}/{total}"
        ),
        heartbeat_age_seconds=age,
        processed=processed,
        total=total,
    )


def latest_progress_state_path(metadata_dir: Path, target: str) -> Path | None:
    """Find the most recent progress state file for the given target.

    Progress state files are named ``<run_id>.state.json`` under
    ``metadata_dir/derived-runs/``. We pick the most recently modified one
    whose journal (``<run_id>.json`` alongside it) has ``status=running``.
    """

    runs_dir = metadata_dir / "derived-runs"
    if not runs_dir.exists():
        return None
    candidates: list[tuple[Path, float]] = []
    for state_path in runs_dir.glob("*.state.json"):
        journal_path = state_path.with_suffix("")  # strip .state.json → .json
        # state path is <run_id>.state.json; journal is <run_id>.json
        journal_path = state_path.parent / state_path.name.replace(".state.json", ".json")
        if not journal_path.exists():
            continue
        try:
            journal_data = json.loads(journal_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(journal_data, dict):
            continue
        if str(journal_data.get("target")) != target:
            continue
        if str(journal_data.get("status")) != "running":
            continue
        try:
            mtime = state_path.stat().st_mtime
        except OSError:
            continue
        candidates.append((state_path, mtime))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[1], reverse=True)
    return candidates[0][0]


def _parse_iso_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None
