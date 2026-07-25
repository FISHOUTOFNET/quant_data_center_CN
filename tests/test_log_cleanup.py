from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from click.testing import CliRunner

from src.tools import log_cleanup
from src.utils.paths import ensure_managed_log_root


def _touch(path: Path, mtime: datetime, content: bytes = b"log") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    timestamp = mtime.timestamp()
    os.utime(path, (timestamp, timestamp))
    return path


def _authorize(root: Path) -> Path:
    """Authorize ``root`` via the official managed-root authority."""
    ensure_managed_log_root(root)
    return root


def test_cleanup_logs_deletes_log_files_older_than_retention(tmp_path: Path) -> None:
    now = datetime(2026, 6, 4, 12, 0, tzinfo=timezone.utc)
    old_log = _touch(tmp_path / "old.log", now - timedelta(days=31))
    old_out = _touch(tmp_path / "old.out", now - timedelta(days=31), b"stdout")
    recent_log = _touch(tmp_path / "recent.log", now - timedelta(days=1))
    boundary_log = _touch(tmp_path / "boundary.log", now - timedelta(days=30))

    _authorize(tmp_path)
    result = log_cleanup.cleanup_logs(tmp_path, retention_days=30, now=now)

    assert result.deleted_count == 2
    assert result.deleted_bytes == len(b"log") + len(b"stdout")
    assert result.kept_count == 2
    assert result.failures == []
    assert not old_log.exists()
    assert not old_out.exists()
    assert recent_log.exists()
    assert boundary_log.exists()


def test_cleanup_logs_dry_run_reports_without_deleting(tmp_path: Path) -> None:
    now = datetime(2026, 6, 4, 12, 0, tzinfo=timezone.utc)
    old_log = _touch(tmp_path / "old.log", now - timedelta(days=31))

    _authorize(tmp_path)
    result = log_cleanup.cleanup_logs(tmp_path, retention_days=30, dry_run=True, now=now)

    assert result.deleted_count == 1
    assert result.deleted_bytes == len(b"log")
    assert result.failures == []
    assert old_log.exists()


def test_cleanup_logs_reports_failed_delete_without_counting_it(tmp_path: Path, monkeypatch) -> None:
    now = datetime(2026, 6, 4, 12, 0, tzinfo=timezone.utc)
    old_log = _touch(tmp_path / "old.log", now - timedelta(days=31))
    original_unlink = Path.unlink

    def fail_unlink(self: Path) -> None:
        if self == old_log:
            raise OSError("locked")
        original_unlink(self)

    monkeypatch.setattr(Path, "unlink", fail_unlink)

    _authorize(tmp_path)
    result = log_cleanup.cleanup_logs(tmp_path, retention_days=30, now=now)

    assert result.deleted_count == 0
    assert result.deleted_bytes == 0
    assert len(result.failures) == 1
    assert result.failures[0].path == old_log
    assert "locked" in result.failures[0].error
    assert old_log.exists()


def test_cleanup_logs_skips_non_log_files_directories_and_symlinks(tmp_path: Path) -> None:
    now = datetime(2026, 6, 4, 12, 0, tzinfo=timezone.utc)
    _touch(tmp_path / "old.txt", now - timedelta(days=31))
    nested_dir = tmp_path / "old-dir.log"
    nested_dir.mkdir()
    symlink = tmp_path / "link.log"
    target = _touch(tmp_path / "target.log", now - timedelta(days=1))
    try:
        symlink.symlink_to(target)
    except OSError:
        symlink = None

    _authorize(tmp_path)
    result = log_cleanup.cleanup_logs(tmp_path, retention_days=30, now=now)

    assert result.deleted_count == 0
    assert (tmp_path / "old.txt").exists()
    assert nested_dir.exists()
    if symlink is not None:
        assert symlink.exists()


def test_cli_uses_qdc_log_dir_by_default(tmp_path: Path, monkeypatch) -> None:
    now = datetime.now(timezone.utc)
    log_dir = tmp_path / "env-logs"
    old_log = _touch(log_dir / "old.log", now - timedelta(days=31))
    _authorize(log_dir)
    monkeypatch.setenv("QDC_LOG_DIR", str(log_dir))

    result = CliRunner().invoke(log_cleanup.main, ["--retention-days", "30"])

    assert result.exit_code == 0
    assert "deleted=1" in result.output
    assert not old_log.exists()


# ---------------------------------------------------------------------------
# Enhanced policy: keep_recent_runs, active_run_ids, max_bytes, symlink safety
# ---------------------------------------------------------------------------


def _touch_run_log(path: Path, mtime: datetime, run_id: str, content: bytes = b"log") -> Path:
    """Create a run log with a run-id embedded in the filename."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    timestamp = mtime.timestamp()
    os.utime(path, (timestamp, timestamp))
    return path


def test_keep_recent_runs_protects_old_run_logs(tmp_path: Path) -> None:
    """Even if run logs are older than retention, the N most recent are kept."""

    now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
    runs_dir = tmp_path / "runs"
    # Create 15 run logs, all 100 days old.
    for i in range(15):
        stamp = (now - timedelta(days=100, hours=i)).strftime("%Y%m%d_%H%M%S")
        run_id = f"run-{stamp}-abc{i:03d}"
        _touch_run_log(
            runs_dir / f"{stamp}_{run_id}.log",
            now - timedelta(days=100, hours=i),
            run_id,
        )

    _authorize(tmp_path)
    result = log_cleanup.cleanup_logs(
        tmp_path,
        retention_days=30,
        now=now,
        keep_recent_runs=10,
        max_bytes=None,
    )

    # 5 oldest run logs deleted, 10 most recent kept.
    assert result.deleted_count == 5
    remaining = list(runs_dir.iterdir())
    assert len(remaining) == 10


def test_active_run_logs_are_never_deleted(tmp_path: Path) -> None:
    """Run logs matching an active run_id are protected even if ancient."""

    now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
    runs_dir = tmp_path / "runs"
    active_id = "run-20250101-000000-active1"
    ancient = _touch_run_log(
        runs_dir / "20250101_000000_run-20250101-000000-active1.log",
        now - timedelta(days=365),
        active_id,
    )

    _authorize(tmp_path)
    result = log_cleanup.cleanup_logs(
        tmp_path,
        retention_days=30,
        now=now,
        keep_recent_runs=0,
        active_run_ids={active_id},
    )

    assert result.deleted_count == 0
    assert ancient.exists()
    assert result.kept_reasons.get(log_cleanup.KEEP_REASON_ACTIVE_RUN) == 1


def test_max_bytes_evicts_oldest_run_logs_first(tmp_path: Path) -> None:
    """When total size exceeds the cap, oldest non-active run logs go first."""

    now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
    runs_dir = tmp_path / "runs"
    # 3 run logs: 1KB each, total 3KB. Cap = 2KB → oldest 1 deleted.
    for i in range(3):
        stamp = (now - timedelta(days=i)).strftime("%Y%m%d_%H%M%S")
        run_id = f"run-{stamp}-nonce{i}"
        _touch_run_log(
            runs_dir / f"{stamp}_{run_id}.log",
            now - timedelta(days=i),
            run_id,
            content=b"x" * 1024,
        )

    _authorize(tmp_path)
    result = log_cleanup.cleanup_logs(
        tmp_path,
        retention_days=30,
        now=now,
        keep_recent_runs=0,
        max_bytes=2048,
    )

    assert result.deleted_count == 1
    remaining = sorted(runs_dir.iterdir(), key=lambda p: p.stat().st_mtime)
    assert len(remaining) == 2


def test_symlink_escaping_root_is_not_followed(tmp_path: Path) -> None:
    """A symlink pointing outside the log root must not be deleted or followed."""

    now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
    outside_dir = tmp_path / "outside"
    outside_dir.mkdir()
    outside_target = _touch(outside_dir / "secret.log", now - timedelta(days=365))
    log_dir = tmp_path / "managed-logs"
    log_dir.mkdir()
    link = log_dir / "escape.log"
    try:
        link.symlink_to(outside_target)
    except OSError:
        pytest.skip("symlink not supported on this platform")

    _authorize(log_dir)
    result = log_cleanup.cleanup_logs(log_dir, retention_days=30, now=now)

    assert result.deleted_count == 0
    assert outside_target.exists()
    assert link.exists()


def test_cleanup_failures_do_not_raise_cli(tmp_path: Path, monkeypatch) -> None:
    """The CLI must exit 0 even when individual file deletes fail."""

    now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
    old_log = _touch(tmp_path / "old.log", now - timedelta(days=31))
    original_unlink = Path.unlink

    def fail_unlink(self: Path) -> None:
        if self == old_log:
            raise OSError("locked")
        original_unlink(self)

    monkeypatch.setattr(Path, "unlink", fail_unlink)
    monkeypatch.setenv("QDC_LOG_DIR", str(tmp_path))

    _authorize(tmp_path)
    result = CliRunner().invoke(log_cleanup.main, ["--retention-days", "30"])

    assert result.exit_code == 0
    assert "failures=1" in result.output


def test_run_logs_in_runs_subdir_are_identified(tmp_path: Path) -> None:
    """Only files in the ``runs/`` subdir are treated as run logs for recent-run protection."""

    now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
    runs_dir = tmp_path / "runs"
    # An old run log in runs/ that should be protected by keep_recent_runs.
    old_run = _touch_run_log(
        runs_dir / "20250101_000000_run-20250101-000000-abc.log",
        now - timedelta(days=365),
        "run-20250101-000000-abc",
    )
    # An old log file directly in tmp_path (not a run log) — should be deleted
    # by retention regardless of keep_recent_runs.
    old_flat = _touch(tmp_path / "flat.log", now - timedelta(days=365))

    _authorize(tmp_path)
    log_cleanup.cleanup_logs(
        tmp_path,
        retention_days=30,
        now=now,
        keep_recent_runs=10,
        max_bytes=None,
    )

    assert old_run.exists()  # protected by keep_recent_runs
    assert not old_flat.exists()  # deleted by retention


def test_git_clean_does_not_remove_logs_outside_repo(tmp_path: Path, monkeypatch) -> None:
    """Simulate git clean: the repo dir is wiped, but logs outside survive."""

    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / ".git").mkdir()
    log_root = tmp_path / "AppData" / "Local" / "QuantDataCenter" / "logs"
    run_log = _touch(log_root / "runs" / "20260725_100000_run-20260725-100000-abc.log", datetime.now(timezone.utc))

    # Simulate git clean removing everything in the repo.
    import shutil

    shutil.rmtree(repo)

    assert run_log.exists()
