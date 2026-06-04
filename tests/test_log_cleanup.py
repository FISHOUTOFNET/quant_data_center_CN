from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from click.testing import CliRunner

from src.tools import log_cleanup


def _touch(path: Path, mtime: datetime, content: bytes = b"log") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    timestamp = mtime.timestamp()
    os.utime(path, (timestamp, timestamp))
    return path


def test_cleanup_logs_deletes_log_files_older_than_retention(tmp_path: Path) -> None:
    now = datetime(2026, 6, 4, 12, 0, tzinfo=timezone.utc)
    old_log = _touch(tmp_path / "old.log", now - timedelta(days=31))
    old_out = _touch(tmp_path / "old.out", now - timedelta(days=31), b"stdout")
    recent_log = _touch(tmp_path / "recent.log", now - timedelta(days=1))
    boundary_log = _touch(tmp_path / "boundary.log", now - timedelta(days=30))

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
    monkeypatch.setenv("QDC_LOG_DIR", str(log_dir))

    result = CliRunner().invoke(log_cleanup.main, ["--retention-days", "30"])

    assert result.exit_code == 0
    assert "deleted=1" in result.output
    assert not old_log.exists()
