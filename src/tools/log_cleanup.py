"""Clean expired runtime log files."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import click

from src.utils import paths

DEFAULT_RETENTION_DAYS = 30
LOG_SUFFIXES = {".log", ".out", ".err"}


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


def cleanup_logs(
    log_dir: str | Path,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    dry_run: bool = False,
    now: datetime | None = None,
) -> CleanupResult:
    """Delete log files older than the retention window from one directory."""

    directory = Path(log_dir).expanduser().resolve()
    if retention_days < 0:
        raise ValueError("retention_days must be >= 0")
    if not directory.exists():
        return CleanupResult(deleted_count=0, deleted_bytes=0, kept_count=0, failures=[])

    reference_time = now or datetime.now(timezone.utc)
    cutoff = reference_time.timestamp() - retention_days * 24 * 60 * 60
    deleted_count = 0
    deleted_bytes = 0
    kept_count = 0
    failures: list[CleanupFailure] = []

    for candidate in directory.iterdir():
        if not _is_cleanup_candidate(candidate):
            continue

        try:
            stat = candidate.stat()
        except OSError as exc:
            failures.append(CleanupFailure(path=candidate, error=str(exc)))
            continue

        if stat.st_mtime >= cutoff:
            kept_count += 1
            continue

        if dry_run:
            deleted_count += 1
            deleted_bytes += stat.st_size
            continue

        try:
            candidate.unlink()
        except OSError as exc:
            failures.append(CleanupFailure(path=candidate, error=str(exc)))
            continue

        deleted_count += 1
        deleted_bytes += stat.st_size

    return CleanupResult(
        deleted_count=deleted_count,
        deleted_bytes=deleted_bytes,
        kept_count=kept_count,
        failures=failures,
    )


def default_log_dir() -> Path:
    """Return the configured log directory."""

    configured = os.environ.get("QDC_LOG_DIR")
    if configured:
        return Path(configured).expanduser().resolve()
    return paths.LOGS_DIR


def _is_cleanup_candidate(path: Path) -> bool:
    return path.suffix.lower() in LOG_SUFFIXES and path.is_file() and not path.is_symlink()


@click.command()
@click.option("--log-dir", type=click.Path(path_type=Path, file_okay=False), default=None)
@click.option("--retention-days", type=click.IntRange(min=0), default=DEFAULT_RETENTION_DAYS, show_default=True)
@click.option("--dry-run", is_flag=True, help="Report expired logs without deleting files.")
def main(log_dir: Path | None, retention_days: int, dry_run: bool) -> None:
    """Clean expired log files."""

    target_dir = log_dir.resolve() if log_dir else default_log_dir()
    result = cleanup_logs(target_dir, retention_days=retention_days, dry_run=dry_run)
    click.echo(
        "log cleanup "
        f"dir={target_dir} retention_days={retention_days} dry_run={dry_run} "
        f"deleted={result.deleted_count} bytes={result.deleted_bytes} "
        f"kept={result.kept_count} failures={len(result.failures)}"
    )
    for failure in result.failures:
        click.echo(f"failed path={failure.path} error={failure.error}", err=True)
    if result.failures:
        raise click.ClickException(f"failed to delete {len(result.failures)} log file(s)")


if __name__ == "__main__":
    main()
