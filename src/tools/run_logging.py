"""Per-run log context for the daily update orchestrator.

The orchestrator owns a single :class:`RunLogContext` per invocation and passes
its ``path`` to every subprocess via ``--run-log`` / ``QDC_RUN_LOG_PATH``. This
replaces the previous ad-hoc scheme where:

* the BAT script created a log file before invoking Python;
* the orchestrator created another log file under ``<repo>/logs``;
* the state file referenced that path without ever validating it.

With :class:`RunLogContext`, the orchestrator is the single owner: it creates
the file, the BAT script only forwards the path if the user supplied one
explicitly, and the state file tracks the run-id / creation time / last-write
status so that crash recovery can tell whether the log is still alive.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from src.utils import paths
from src.utils.logging import logger


class RunLogContextError(RuntimeError):
    """Raised when a run log context cannot be created or validated."""


@dataclass(frozen=True)
class RunLogContext:
    """Single per-run log context shared by the orchestrator and subprocesses.

    Attributes:
        run_id: Short, sortable identifier (``run-<timestamp>-<nonce>``).
        path: Filesystem path to the per-run log file.
        created_at: Wall-clock time when the log file was created.
    """

    run_id: str
    path: Path
    created_at: datetime

    def touch(self, now: datetime | None = None) -> None:
        """Update ``log_last_write_at`` by appending a no-op marker line."""

        marker_time = (now or datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
        try:
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(f"[{marker_time}] run-log heartbeat\n")
        except OSError as exc:  # pragma: no cover - defensive, logged below
            logger.warning("Failed to update run log heartbeat: {}", exc)

    def status_payload(self, *, now: datetime | None = None) -> dict[str, Any]:
        """Return the dict written into the daily-update state file."""

        reference_time = now or datetime.now()
        path_exists = self.path.exists()
        last_write_at: str | None = None
        if path_exists:
            try:
                stat = self.path.stat()
                last_write_at = datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds")
            except OSError:
                last_write_at = None
        return {
            "run_id": self.run_id,
            "log_path": str(self.path),
            "log_created_at": self.created_at.isoformat(timespec="seconds"),
            "log_last_write_at": last_write_at,
            "log_status": "available" if path_exists else "missing",
            "log_checked_at": reference_time.isoformat(timespec="seconds"),
        }


def create_run_log_context(
    *,
    runtime_paths: paths.RuntimePaths | None = None,
    run_id: str | None = None,
    now: datetime | None = None,
    explicit_path: str | Path | None = None,
) -> RunLogContext:
    """Create a fresh :class:`RunLogContext` and the underlying log file.

    Args:
        runtime_paths: Resolved runtime paths. When omitted the function calls
            :func:`paths.resolve_runtime_paths` with no overrides so the
            default priority (CLI/env/config/OS-default) applies.
        run_id: Optional run id. Auto-generated as ``run-<stamp>-<nonce>``.
        now: Optional clock for deterministic tests.
        explicit_path: Optional explicit run-log path. When provided it wins
            over the runtime path's ``run_logs_dir``; the parent directory
            must already exist or be creatable.

    The log file is created (truncated) immediately so that crash recovery can
    detect ``log_status=missing`` later. Failure to create the file raises
    :class:`RunLogContextError` (fail-fast) instead of silently continuing.
    """

    resolved_runtime = runtime_paths or paths.resolve_runtime_paths()
    timestamp = (now or datetime.now())
    resolved_run_id = run_id or _format_run_id(timestamp)
    if explicit_path:
        log_path = Path(explicit_path).expanduser().resolve()
    else:
        stamp = timestamp.strftime("%Y%m%d_%H%M%S")
        safe_nonce = uuid.uuid4().hex[:6]
        log_path = resolved_runtime.run_logs_dir / f"{stamp}_{resolved_run_id}.log"
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        # Ensure the managed-root marker exists so that log_cleanup can later
        # authorize this directory. ``ensure_managed_log_root`` is the single
        # authority that creates the marker; cleanup only validates (P0-7).
        paths.ensure_managed_log_root(resolved_runtime.logs_dir)
        # Create/truncate the file so the orchestrator is the sole owner.
        log_path.touch(exist_ok=False)
    except FileExistsError as exc:
        raise RunLogContextError(
            f"Run log already exists; refusing to overwrite: {log_path}. "
            "Pass a unique --run-log path or remove the existing file."
        ) from exc
    except OSError as exc:
        raise RunLogContextError(f"Failed to create run log at {log_path}: {exc}") from exc
    return RunLogContext(run_id=resolved_run_id, path=log_path, created_at=timestamp)


def adopt_run_log_context(
    *,
    path: str | Path,
    run_id: str | None = None,
    now: datetime | None = None,
) -> RunLogContext:
    """Adopt an existing run log file (created by the BAT entrypoint).

    Used when the BAT script has already created the run-log file with a
    deterministic name. The orchestrator reuses the file rather than
    creating a parallel one. If the file does not exist a new one is created
    so that the orchestrator remains the owner of the log lifecycle.
    """

    log_path = Path(path).expanduser().resolve()
    timestamp = now or datetime.now()
    resolved_run_id = run_id or _format_run_id(timestamp)
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        if not log_path.exists():
            log_path.touch(exist_ok=False)
        else:
            # The BAT script pre-wrote a header line; preserve it.
            pass
    except OSError as exc:
        raise RunLogContextError(f"Failed to adopt run log at {log_path}: {exc}") from exc
    return RunLogContext(run_id=resolved_run_id, path=log_path, created_at=timestamp)


def _format_run_id(timestamp: datetime) -> str:
    return f"run-{timestamp.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"


def disable_file_log_env_for_subprocess() -> dict[str, str]:
    """Return env overrides so subprocesses only echo to the run log.

    ``QDC_DISABLE_FILE_LOG=1`` is preserved for backwards compatibility. The
    comment below documents the actual semantics so future contributors do
    not assume the flag silences the application log entirely (it does not:
    it only prevents subprocesses from re-opening ``application/qdc.log``,
    which is desirable because the orchestrator already captures their
    stdout/stderr into the per-run log file).
    """

    return {"QDC_DISABLE_FILE_LOG": "1"}


def current_orchestrator_pid() -> int:
    return int(os.getpid())
