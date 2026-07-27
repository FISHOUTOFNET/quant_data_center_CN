"""Clean expired runtime log files under the managed log root.

Policy:
* Only delete files inside the resolved managed log root.
* The root must carry a valid ``.qdc-managed-log-root`` marker. Cleanup MUST
  NOT auto-create the marker — that would let cleanup claim any external
  directory. The marker is created by ``paths.ensure_managed_log_root`` (the
  single authority), which is called from application logging init,
  ``create_run_log_context``, ``adopt_run_log_context``, and the
  ``--initialize-managed-root`` CLI flag of this module.
* Root safety rules (symlink/junction/home/root/repo rejection) live in
  ``paths._reject_unsafe_log_root`` — the single authority. This module does
  NOT maintain a duplicate set of root safety rules.
* Never follow symlinks or junctions that escape the log root.
* Keep the most recent ``keep_recent_runs`` per-run logs regardless of age.
* Never delete logs belonging to an active run (``active_run_ids``).
* Delete files older than ``retention_days`` (except the protected ones).
* If total size exceeds ``max_bytes``, evict the oldest non-active run logs
  first until the cap is satisfied.
* Cleanup failures are recorded as warnings and never raise.
"""

from __future__ import annotations

import os
from collections.abc import Collection, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import click

from src.utils import filesystem_safety, paths
from src.utils.logging import logger
from src.utils.paths import (
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
KEEP_REASON_LINK_LIKE = "link_like_or_reparse"


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


def _normalize_active_run_ids(values: Iterable[str]) -> tuple[str, ...]:
    """Normalize a sequence of active-run IDs into a stable, de-duplicated tuple.

    P1 fix: the CLI ``--active-run-id`` option and the ``QDC_ACTIVE_RUN_ID``
    env var (comma-separated) previously fed raw strings into the active-id
    set. A value like ``" id-a , id-b , ,id-a"`` would produce distinct
    entries ``" id-a "`` and ``"id-a"`` — so a run log tagged ``id-a`` was
    NOT protected even though the user thought it was. This helper closes
    that gap and is shared by the CLI and ``cleanup_logs`` (defensive
    normalization at the entry point, not just at the CLI).

    Rules:
    * ``str(value).strip()`` — remove leading/trailing whitespace.
    * Filter out empty strings (so trailing commas / doubled commas are
      ignored).
    * Exact de-duplication (case-sensitive — run IDs are case-sensitive).
    * Preserve first-seen order for stable output.
    * NO substring matching: ``id-a`` does NOT protect ``id-ab``.
    """

    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        normalized = str(value).strip()
        if not normalized:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return tuple(result)


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
    #
    # P0: ``validate_managed_log_root`` runs ``_reject_unsafe_log_root`` on
    # the RAW (expanduser-only) path BEFORE resolving internally, so a symlink
    # or junction managed root is rejected here. We must NOT call
    # ``root.resolve()`` before this check or the symlink would be washed
    # away.
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
    # Defensive normalization at the cleanup_logs() entry too — do NOT rely
    # solely on the CLI. Active run IDs are stripped, de-duplicated (exact
    # match, case-sensitive, no substring), and order-preserved so a run-id
    # with surrounding whitespace cannot slip past retention/capacity
    # protection.
    active_ids = set(_normalize_active_run_ids(active_run_ids or ()))

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
        """Attempt to delete ``item``. Return True on success.

        Pre-delete boundary verification (P0): re-verify the candidate is safe
        to delete right before unlinking. This catches TOCTOU where a file was
        swapped for a symlink/junction/reparse point between discovery and
        deletion, and where a file was moved outside the canonical managed
        root. Unsafe candidates are skipped (not deleted, not counted as
        failure) and recorded under ``KEEP_REASON_LINK_LIKE``.
        """
        nonlocal deleted_count, deleted_bytes
        if not _is_safe_delete_candidate(item.path, root):
            kept_reasons[KEEP_REASON_LINK_LIKE] = kept_reasons.get(KEEP_REASON_LINK_LIKE, 0) + 1
            logger.debug(
                "Skipping delete of unsafe candidate: path={} reason=link_like_or_outside_root",
                item.path,
            )
            return False
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


@dataclass(frozen=True)
class _LogFile:
    path: Path
    size: int
    mtime: datetime
    mtime_timestamp: float
    is_log_file: bool
    is_run_log: bool
    active_run_id: str | None


def _is_safe_delete_candidate(path: Path, root_resolved: Path) -> bool:
    """Re-verify a file is safe to delete right before unlinking.

    Pre-delete boundary verification (P0). Catches TOCTOU where a file was
    swapped for a symlink/junction/reparse point, or moved outside the
    canonical managed root, between discovery and deletion.

    Checks:
    1. The candidate is NOT a symlink/junction/reparse point.
    2. The candidate still lives inside the canonical managed root.
    3. The candidate is still a regular log file (not a dir, not a non-log).
    """

    if filesystem_safety.is_link_like_or_reparse(path):
        return False
    try:
        resolved = path.resolve()
    except (OSError, RuntimeError):
        return False
    if not (paths.is_path_inside(resolved, root_resolved) or resolved == root_resolved):
        return False
    if path.suffix.lower() not in LOG_SUFFIXES:
        return False
    return path.is_file() and not path.is_dir()


def _discover_log_files(root: Path) -> list[_LogFile]:
    """Walk ``root`` pruning link-like/reparse nodes (symlinks, junctions, etc.).

    ``os.walk(followlinks=False)`` already avoids following symlink dirs on
    POSIX, but on Windows it does NOT skip junctions or other reparse points.
    We conservatively prune ALL link-like dirs (symlinks, junctions, mount
    points, other reparse tags) from ``dirnames[:]`` IN PLACE — not just
    those that escape the root. The marker binds to a directory identity;
    recursing into a link-like dir would reach the link target's tree, which
    may live outside the managed root.

    Log file candidates that are link-like/reparse are also rejected. The
    deletion boundary must never touch a link target's content.
    """

    discovered: list[_LogFile] = []
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        current_dir = Path(dirpath)
        # Prune link-like/reparse directories from dirnames[:] IN PLACE.
        # This is the primary defense; followlinks=False is a backstop on
        # POSIX but does not catch junctions on Windows.
        kept_dirnames: list[str] = []
        for entry in dirnames:
            candidate = current_dir / entry
            if filesystem_safety.is_link_like_or_reparse(candidate):
                logger.debug(
                    "Pruning link-like directory from cleanup traversal: path={} kind={}",
                    candidate,
                    filesystem_safety.describe_link_like(candidate),
                )
                continue
            kept_dirnames.append(entry)
        dirnames[:] = kept_dirnames

        for name in filenames:
            file_path = current_dir / name
            # Reject link-like/reparse file candidates (symlinks, junctions,
            # other reparse points). The deletion boundary must never touch a
            # link target's content, even if the link points inside the root.
            if filesystem_safety.is_link_like_or_reparse(file_path):
                logger.debug(
                    "Skipping link-like file in cleanup discovery: path={} kind={}",
                    file_path,
                    filesystem_safety.describe_link_like(file_path),
                )
                continue
            try:
                stat = file_path.stat()
            except OSError:
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
@click.option(
    "--initialize-managed-root",
    "initialize_managed_root",
    is_flag=True,
    default=False,
    help="Only create the managed-root marker in --log-dir and exit. "
    "Does NOT run cleanup in the same call. Idempotent for a valid marker; "
    "refuses to overwrite a corrupt/wrong-application marker. "
    "Root symlink/junction/home/root/repo paths are rejected.",
)
def main(
    log_dir: Path | None,
    retention_days: int,
    keep_recent_runs: int,
    max_bytes: int,
    active_run_ids: tuple[str, ...],
    dry_run: bool,
    initialize_managed_root: bool,
) -> None:
    """Clean expired log files under the managed log root."""

    # P0 fix: validate the RAW (expanduser-only) log dir BEFORE resolution.
    # ``paths.ensure_managed_log_root`` and ``validate_managed_log_root``
    # both run ``_reject_unsafe_log_root`` on the raw path (checking
    # ``is_symlink`` / ``is_junction``) and only then resolve internally. If
    # we called ``.resolve()`` here, a symlink or junction managed root
    # would be washed away to its real target and the safety check would be
    # bypassed.
    target_dir = log_dir.expanduser() if log_dir else default_log_dir()

    # P0-3: --initialize-managed-root only authorizes the directory and exits.
    # It never runs cleanup in the same call, so a user cannot accidentally
    # authorize and clean in one step. The underlying authority is
    # ``paths.ensure_managed_log_root`` (single source of truth). It is
    # idempotent for a valid marker, refuses to overwrite a corrupt/wrong
    # marker, and rejects root symlink/junction/home/root/repo paths.
    if initialize_managed_root:
        try:
            paths.ensure_managed_log_root(target_dir)
        except LogRootAuthorizationError as exc:
            raise click.ClickException(str(exc)) from exc
        click.echo(f"managed log root authorized: {target_dir}")
        return

    effective_max = max_bytes if max_bytes > 0 else None
    # P1: normalize active run IDs (strip whitespace, de-duplicate, preserve
    # order). Both the CLI ``--active-run-id`` option and the
    # ``QDC_ACTIVE_RUN_ID`` env var (comma-separated) flow through the same
    # helper so a run-id with surrounding whitespace cannot slip past
    # retention/capacity protection. ``cleanup_logs`` also normalizes
    # defensively, but normalizing here keeps the echo'd summary accurate.
    env_active = os.environ.get("QDC_ACTIVE_RUN_ID", "")
    env_ids = tuple(value for value in env_active.split(","))
    all_active_ids = _normalize_active_run_ids((*active_run_ids, *env_ids))
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
