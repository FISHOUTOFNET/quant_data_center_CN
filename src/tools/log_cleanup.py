"""Clean expired runtime log files under the managed log root.

Policy:
* Only delete files inside the resolved managed log root.
* The root must carry a valid ``.qdc-managed-log-root`` marker. Cleanup MUST
  NOT auto-create the marker — that would let cleanup claim any external
  directory. The marker is created by ``paths.ensure_managed_log_root``,
  which is called from application logging init and ``create_run_log_context``.
* Never follow symlinks or junctions that escape the log root.
* Reject dangerous roots: filesystem root, drive root, home, repo root,
  repo-internal paths, symlinks, and Windows junctions/reparse points.
* Keep the most recent ``keep_recent_runs`` per-run logs regardless of age.
* Never delete logs belonging to an active run (``active_run_ids``).
* Delete files older than ``retention_days`` (except the protected ones).
* If total size exceeds ``max_bytes``, evict the oldest non-active run logs
  first until the cap is satisfied.
* Cleanup failures are recorded as warnings and never raise.
"""

from __future__ import annotations

import json
import os
from collections.abc import Collection
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import click

from src.utils import paths
from src.utils.paths import (
    MANAGED_ROOT_LAYOUT_VERSION,
    MANAGED_ROOT_MARKER,
    LogRootAuthorizationError,
    validate_managed_log_root,
)

DEFAULT_RETENTION_DAYS = 30
DEFAULT_KEEP_RECENT_RUNS = 10
DEFAULT_MAX_BYTES = 2 * 1024 * 1024 * 1024  # 2 GiB
LOG_SUFFIXES = {".log", ".out", ".err"}
RUNS_SUBDIR = "runs"
KEEP_REASON_WITHIN_RETENTION = "within_retention"
KEEP_REASON_RECENT_RUN = "recent_run"
KEEP_REASON_ACTIVE_RUN = "active_run"
KEEP_REASON_NOT_A_LOG = "not_a_log_file"
KEEP_REASON_SYMLINK_ESCAPE = "symlink_escapes_root"
KEEP_REASON_OUTSIDE_ROOT = "outside_root"


class LogCleanupError(RuntimeError):
    """Raised when the requested log directory is unsafe to clean."""


@dataclass(frozen=True)
class CleanupFailure:
    """A file that could not be removed."""

    path: Path
    error: str


@dataclass(frozen=True)
class CleanupResult:
    """Summary of a log cleanup run."""

    deleted_count: int
    deleted_bytes: int
    kept_count: int
    failures: list[CleanupFailure]
    kept_reasons: dict[str, int] = field(default_factory=dict)


def cleanup_logs(
    log_dir: str | Path,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    *,
    dry_run: bool = False,
    now: datetime | None = None,
    keep_recent_runs: int = DEFAULT_KEEP_RECENT_RUNS,
    max_bytes: int | None = DEFAULT_MAX_BYTES,
    active_run_ids: Collection[str] | None = None,
) -> CleanupResult:
    """Delete expired log files under ``log_dir`` according to the cleanup policy.

    The function walks ``log_dir`` recursively but never follows symlinks or
    junctions that point outside ``log_dir``. Files directly in ``log_dir`` and
    in the ``runs/`` subdirectory are candidates for deletion; other files are
    left untouched.
    """

    if retention_days < 0:
        raise ValueError("retention_days must be >= 0")
    if keep_recent_runs < 0:
        raise ValueError("keep_recent_runs must be >= 0")

    root = Path(log_dir).expanduser()
    # Validate the managed-root marker BEFORE any deletion. Cleanup must NOT
    # auto-create the marker (P0-7): a missing/invalid/wrong marker means the
    # directory was never authorized, and cleanup must refuse with
    # ``LogCleanupError`` (surfaced as a CLI non-zero exit). The marker is
    # created by ``paths.ensure_managed_log_root`` from application logging
    # init / ``create_run_log_context`` — never by cleanup.
    try:
        validate_managed_log_root(root)
    except LogRootAuthorizationError as exc:
        raise LogCleanupError(str(exc)) from exc
    root = root.resolve()
    if not root.exists():
        return CleanupResult(
            deleted_count=0,
            deleted_bytes=0,
            kept_count=0,
            failures=[],
            kept_reasons={},
        )

    reference_time = now or datetime.now(timezone.utc)
    cutoff_timestamp = reference_time.timestamp() - retention_days * 24 * 60 * 60
    active_ids = {str(value) for value in (active_run_ids or ()) if value}

    discovered = _discover_log_files(root)
    run_logs = [item for item in discovered if item.is_run_log]

    # Protect the most recent N run logs by mtime (descending).
    protected_run_paths: set[Path] = set()
    if keep_recent_runs > 0:
        sorted_run_logs = sorted(run_logs, key=lambda item: item.mtime, reverse=True)
        for item in sorted_run_logs[:keep_recent_runs]:
            protected_run_paths.add(item.path)

    deleted_count = 0
    deleted_bytes = 0
    failures: list[CleanupFailure] = []
    kept_reasons: dict[str, int] = {}
    deleted_paths: set[Path] = set()

    def _delete(item: _LogFile) -> bool:
        """Attempt to delete ``item``. Return True on success."""
        nonlocal deleted_count, deleted_bytes
        if dry_run:
            deleted_count += 1
            deleted_bytes += item.size
            deleted_paths.add(item.path)
            return True
        try:
            item.path.unlink()
        except OSError as exc:
            failures.append(CleanupFailure(path=item.path, error=str(exc)))
            return False
        deleted_count += 1
        deleted_bytes += item.size
        deleted_paths.add(item.path)
        return True

    # Pass 1: retention + active_run + recent_run protection.
    for item in discovered:
        if not item.is_log_file:
            kept_reasons[KEEP_REASON_NOT_A_LOG] = kept_reasons.get(KEEP_REASON_NOT_A_LOG, 0) + 1
            continue
        if item.active_run_id and item.active_run_id in active_ids:
            kept_reasons[KEEP_REASON_ACTIVE_RUN] = kept_reasons.get(KEEP_REASON_ACTIVE_RUN, 0) + 1
            continue
        if item.path in protected_run_paths:
            kept_reasons[KEEP_REASON_RECENT_RUN] = kept_reasons.get(KEEP_REASON_RECENT_RUN, 0) + 1
            continue
        if item.mtime_timestamp >= cutoff_timestamp:
            kept_reasons[KEEP_REASON_WITHIN_RETENTION] = kept_reasons.get(KEEP_REASON_WITHIN_RETENTION, 0) + 1
            continue
        _delete(item)

    # Pass 2: capacity cap. Evict oldest non-active, non-recent run logs first.
    # Only log files count towards ``max_bytes`` — the managed-root marker and
    # other non-log infrastructure files are excluded from the size total so a
    # tiny marker cannot trigger spurious evictions.
    if max_bytes is not None and max_bytes > 0:
        remaining_size = sum(item.size for item in discovered if item.path not in deleted_paths and item.is_log_file)
        if remaining_size > max_bytes:
            survivors = sorted(
                (
                    item
                    for item in run_logs
                    if item.path not in deleted_paths
                    and item.path not in protected_run_paths
                    and not (item.active_run_id and item.active_run_id in active_ids)
                ),
                key=lambda item: item.mtime,
            )
            for item in survivors:
                if remaining_size <= max_bytes:
                    break
                size_before = item.size
                if _delete(item):
                    remaining_size -= size_before

    # kept_count reports only log files; the managed-root marker is
    # infrastructure and should not appear in user-facing counts.
    kept_count = sum(1 for item in discovered if item.path not in deleted_paths and item.is_log_file)

    return CleanupResult(
        deleted_count=deleted_count,
        deleted_bytes=deleted_bytes,
        kept_count=kept_count,
        failures=failures,
        kept_reasons=kept_reasons,
    )


def default_log_dir() -> Path:
    """Return the managed log root resolved via :func:`paths.resolve_runtime_paths`."""

    return paths.resolve_runtime_paths().logs_dir


def _reject_dangerous_root(root: Path) -> None:
    """Reject log roots that are unsafe to clean.

    Evaluated on the ORIGINAL (pre-resolve) path so symlinks/junctions are
    still detectable. The managed-root marker check is performed separately
    in :func:`cleanup_logs` after resolution.
    """

    if root.is_symlink():
        target = root.resolve()
        if not paths.is_path_inside(target, root.parent):
            raise LogCleanupError(f"Refusing to clean symlink root that escapes its parent: {root} -> {target}")

    raw = root
    # Detect Windows junctions/reparse points (is_junction is 3.12+).
    is_junction = getattr(raw, "is_junction", lambda: False)()
    if is_junction:
        raise LogCleanupError(f"Refusing to clean Windows junction/reparse root: {raw}")

    resolved = raw.resolve()
    resolved_parent = resolved.parent

    # Reject filesystem root: a path whose parent is itself.
    if resolved == resolved_parent:
        raise LogCleanupError(f"Refusing to clean filesystem root: {raw}")

    # Reject a bare Windows drive root (e.g. C:\).
    drive = resolved.anchor
    if drive and resolved == Path(drive):
        raise LogCleanupError(f"Refusing to clean drive root: {raw}")

    # Reject the user home directory.
    import pathlib

    home = pathlib.Path.home().resolve()
    if resolved == home:
        raise LogCleanupError(f"Refusing to clean user home directory: {raw}")

    # Reject the repository root and any path inside the repository.
    repo_root = paths.ROOT.resolve()
    if resolved == repo_root or paths.is_path_inside(resolved, repo_root):
        raise LogCleanupError(f"Refusing to clean repository or repo-internal path: {raw}")


def _ensure_managed_root_marker(root: Path) -> None:
    """Create the managed-root marker if it does not exist."""

    marker = root / MANAGED_ROOT_MARKER
    if marker.exists():
        return
    payload = {
        "application": "QuantDataCenter",
        "layout_version": MANAGED_ROOT_LAYOUT_VERSION,
    }
    marker.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


@dataclass(frozen=True)
class _LogFile:
    path: Path
    size: int
    mtime: datetime
    mtime_timestamp: float
    is_log_file: bool
    is_run_log: bool
    active_run_id: str | None


def _discover_log_files(root: Path) -> list[_LogFile]:
    """Walk ``root`` without following symlinks that escape it."""

    discovered: list[_LogFile] = []
    root_resolved = root.resolve()
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        current_dir = Path(dirpath)
        # Filter subdirectories: skip symlinked dirs that point outside root.
        kept_dirnames: list[str] = []
        for entry in dirnames:
            candidate = current_dir / entry
            if candidate.is_symlink():
                target = candidate.resolve()
                if not paths.is_path_inside(target, root_resolved):
                    # Skip this directory entirely; do not recurse into it.
                    continue
            kept_dirnames.append(entry)
        dirnames[:] = kept_dirnames

        for name in filenames:
            file_path = current_dir / name
            try:
                stat = file_path.stat()
            except OSError:
                continue
            # Skip symlinks pointing outside root.
            if file_path.is_symlink():
                target = file_path.resolve()
                if not paths.is_path_inside(target, root_resolved):
                    continue
            is_log = file_path.suffix.lower() in LOG_SUFFIXES and file_path.is_file() and not file_path.is_dir()
            is_run_log = is_log and current_dir.name == RUNS_SUBDIR
            run_id = _extract_run_id(file_path.name) if is_run_log else None
            mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
            discovered.append(
                _LogFile(
                    path=file_path,
                    size=int(stat.st_size),
                    mtime=mtime,
                    mtime_timestamp=float(stat.st_mtime),
                    is_log_file=is_log,
                    is_run_log=is_run_log,
                    active_run_id=run_id,
                )
            )
    return discovered


def _extract_run_id(filename: str) -> str | None:
    """Best-effort extraction of a run-id from a run-log filename.

    Run logs created by :mod:`src.tools.run_logging` are named
    ``<YYYYMMDD_HHMMSS>_run-<YYYYMMDD-HHMMSS>-<nonce>.log``. We return the
    ``run-...`` portion when present, otherwise ``None``.
    """

    stem = filename
    for suffix in LOG_SUFFIXES:
        if stem.lower().endswith(suffix):
            stem = stem[: -len(suffix)]
            break
    idx = stem.find("run-")
    if idx >= 0:
        return stem[idx:]
    return None


@click.command()
@click.option("--log-dir", type=click.Path(path_type=Path, file_okay=False), default=None)
@click.option("--retention-days", type=click.IntRange(min=0), default=DEFAULT_RETENTION_DAYS, show_default=True)
@click.option(
    "--keep-recent-runs",
    type=click.IntRange(min=0),
    default=DEFAULT_KEEP_RECENT_RUNS,
    show_default=True,
    help="Always keep at least this many most-recent per-run logs.",
)
@click.option(
    "--max-bytes",
    type=click.IntRange(min=0),
    default=DEFAULT_MAX_BYTES,
    show_default=True,
    help="Total capacity cap in bytes. 0 disables the cap.",
)
@click.option(
    "--active-run-id",
    "active_run_ids",
    multiple=True,
    help="Run-id whose run log must never be deleted. May be repeated. "
    "Also read from QDC_ACTIVE_RUN_ID (comma-separated).",
)
@click.option("--dry-run", is_flag=True, help="Report expired logs without deleting files.")
def main(
    log_dir: Path | None,
    retention_days: int,
    keep_recent_runs: int,
    max_bytes: int,
    active_run_ids: tuple[str, ...],
    dry_run: bool,
) -> None:
    """Clean expired log files under the managed log root."""

    target_dir = log_dir.resolve() if log_dir else default_log_dir()
    effective_max = max_bytes if max_bytes > 0 else None
    env_active = os.environ.get("QDC_ACTIVE_RUN_ID", "")
    env_ids = tuple(value for value in env_active.split(",") if value)
    all_active_ids = (*active_run_ids, *env_ids)
    try:
        result = cleanup_logs(
            target_dir,
            retention_days=retention_days,
            dry_run=dry_run,
            keep_recent_runs=keep_recent_runs,
            max_bytes=effective_max,
            active_run_ids=all_active_ids,
        )
    except LogCleanupError as exc:
        # Safety violations are always errors.
        raise click.ClickException(str(exc)) from exc

    kept_summary = ", ".join(f"{reason}={count}" for reason, count in sorted(result.kept_reasons.items())) or "none"
    click.echo(
        "log cleanup "
        f"dir={target_dir} retention_days={retention_days} dry_run={dry_run} "
        f"keep_recent_runs={keep_recent_runs} max_bytes={effective_max} "
        f"deleted={result.deleted_count} bytes={result.deleted_bytes} "
        f"kept={result.kept_count} kept_reasons={kept_summary} "
        f"failures={len(result.failures)}"
    )
    for failure in result.failures:
        click.echo(f"warning: failed to delete path={failure.path} error={failure.error}", err=True)
    # Cleanup failures must NOT fail the daily workflow; only safety violations
    # (raised above as ClickException) exit non-zero.


if __name__ == "__main__":
    main()
