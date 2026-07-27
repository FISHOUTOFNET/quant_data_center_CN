from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from click.testing import CliRunner

from src.tools import log_cleanup
from src.utils import filesystem_safety
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


# ---------------------------------------------------------------------------
# P0: Cleanup traversal prunes link-like/reparse directories and files
#
# ``_discover_log_files`` must prune symlink/junction/reparse directories from
# ``dirnames[:]`` IN PLACE (not just rely on ``followlinks=False``) and must
# reject link-like file candidates. These tests use real symlinks where
# possible and mock ``is_link_like_or_reparse`` for Windows reparse points.
# ---------------------------------------------------------------------------


def _make_symlink_or_skip(parent: Path, name: str, target: Path) -> Path:
    """Create a symlink or skip the test if symlinks are unsupported."""

    link = parent / name
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlink not supported on this platform")
    return link


class TestCleanupPrunesLinkLikeDirectories:
    """Symlink/junction/reparse directories are pruned from traversal."""

    def test_symlink_directory_not_recursed(self, tmp_path: Path) -> None:
        """A symlink directory inside the log root is NOT recursed into.

        Files under the symlink target must NOT appear in the discovered set,
        even if they match the log suffix.
        """

        now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
        # Create an external directory with an old .log file.
        external = tmp_path / "external"
        external.mkdir()
        external_log = _touch(external / "secret.log", now - timedelta(days=365))

        # Create the managed log root and a symlink dir inside it.
        log_root = tmp_path / "managed-logs"
        log_root.mkdir()
        _make_symlink_or_skip(log_root, "link-dir", external)

        _authorize(log_root)
        discovered = log_cleanup._discover_log_files(log_root)

        # The symlink dir itself is not a file, so it won't be in discovered.
        # The key assertion: the external .log file is NOT discovered.
        discovered_paths = {item.path for item in discovered}
        assert external_log not in discovered_paths
        # The symlink directory was pruned — its target content was not reached.
        assert not any(item.path == external_log for item in discovered)

    def test_reparse_directory_not_recursed_via_mock(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """A directory flagged as reparse point (mocked) is NOT recursed into.

        This simulates Windows junction/mount-point behavior on any OS by
        mocking ``is_link_like_or_reparse`` to return True for a specific
        directory. ``followlinks=False`` alone does NOT catch junctions on
        Windows, so the explicit pruning is the primary defense.
        """

        now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
        log_root = tmp_path / "managed-logs"
        log_root.mkdir()
        # Create a real subdirectory with an old .log file.
        sub = log_root / "subdir"
        sub.mkdir()
        sub_log = _touch(sub / "old.log", now - timedelta(days=365))

        # Mock: treat ``sub`` as a reparse point.
        real_check = filesystem_safety.is_link_like_or_reparse

        def mock_check(path: Path) -> bool:
            if Path(path) == sub:
                return True
            return real_check(path)

        monkeypatch.setattr(filesystem_safety, "is_link_like_or_reparse", mock_check)
        # Also patch the reference in log_cleanup since it imported the name.
        monkeypatch.setattr(log_cleanup.filesystem_safety, "is_link_like_or_reparse", mock_check)

        _authorize(log_root)
        discovered = log_cleanup._discover_log_files(log_root)

        # The sub_log must NOT be discovered because its parent was pruned.
        discovered_paths = {item.path for item in discovered}
        assert sub_log not in discovered_paths

    def test_normal_subdirectory_is_recursed(self, tmp_path: Path) -> None:
        """A normal (non-link) subdirectory IS recursed into."""

        now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
        log_root = tmp_path / "managed-logs"
        log_root.mkdir()
        sub = log_root / "subdir"
        sub.mkdir()
        sub_log = _touch(sub / "old.log", now - timedelta(days=365))

        _authorize(log_root)
        discovered = log_cleanup._discover_log_files(log_root)

        discovered_paths = {item.path for item in discovered}
        assert sub_log in discovered_paths


class TestCleanupRejectsLinkLikeFiles:
    """Link-like/reparse file candidates are not discovered or deleted."""

    def test_symlink_log_file_not_discovered(self, tmp_path: Path) -> None:
        """A symlink .log file is NOT added to the discovered set."""

        now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
        log_root = tmp_path / "managed-logs"
        log_root.mkdir()
        target = _touch(log_root / "target.log", now - timedelta(days=1))
        link = _make_symlink_or_skip(log_root, "link.log", target)

        _authorize(log_root)
        discovered = log_cleanup._discover_log_files(log_root)

        discovered_paths = {item.path for item in discovered}
        # The real target IS discovered; the symlink is NOT.
        assert target in discovered_paths
        assert link not in discovered_paths

    def test_reparse_log_file_not_discovered_via_mock(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """A file flagged as reparse point (mocked) is NOT discovered."""

        now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
        log_root = tmp_path / "managed-logs"
        log_root.mkdir()
        old_log = _touch(log_root / "old.log", now - timedelta(days=365))

        real_check = filesystem_safety.is_link_like_or_reparse

        def mock_check(path: Path) -> bool:
            if Path(path) == old_log:
                return True
            return real_check(path)

        monkeypatch.setattr(filesystem_safety, "is_link_like_or_reparse", mock_check)
        monkeypatch.setattr(log_cleanup.filesystem_safety, "is_link_like_or_reparse", mock_check)

        _authorize(log_root)
        discovered = log_cleanup._discover_log_files(log_root)

        discovered_paths = {item.path for item in discovered}
        assert old_log not in discovered_paths


class TestCleanupPreDeleteVerification:
    """Pre-delete boundary verification catches TOCTOU swaps."""

    def test_symlink_swapped_before_delete_is_not_deleted(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A file that becomes a symlink between discovery and deletion is
        NOT deleted. This is the TOCTOU defense in ``_is_safe_delete_candidate``.
        """

        now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
        log_root = tmp_path / "managed-logs"
        log_root.mkdir()
        old_log = _touch(log_root / "old.log", now - timedelta(days=365))

        _authorize(log_root)

        # Discover first (old_log is a regular file at this point).
        discovered = log_cleanup._discover_log_files(log_root)
        assert any(item.path == old_log for item in discovered)

        # Now simulate TOCTOU: replace the file with a symlink.
        external = tmp_path / "external-target"
        external.mkdir()
        external_target = _touch(external / "secret.log", now - timedelta(days=1))
        old_log.unlink()
        try:
            old_log.symlink_to(external_target)
        except OSError:
            pytest.skip("symlink not supported on this platform")

        # Run cleanup — the symlink must NOT be deleted.
        result = log_cleanup.cleanup_logs(log_root, retention_days=30, now=now)
        assert result.deleted_count == 0
        assert external_target.exists()
        assert old_log.exists()  # the symlink itself still exists

    def test_file_moved_outside_root_before_delete_is_not_deleted(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A file that is moved outside the managed root between discovery
        and deletion is NOT deleted (the resolved path check catches this).
        """

        now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
        log_root = tmp_path / "managed-logs"
        log_root.mkdir()
        old_log = _touch(log_root / "old.log", now - timedelta(days=365))

        _authorize(log_root)

        # Mock _is_safe_delete_candidate to simulate the file being outside root.
        # This tests the boundary check without needing to actually move the file
        # (which would be racy). We patch path.resolve() for the candidate.
        original_resolve = Path.resolve

        def mock_resolve(self: Path) -> Path:
            if self == old_log:
                # Simulate the file resolving to a path outside the root.
                return tmp_path / "escaped.log"
            return original_resolve(self)

        monkeypatch.setattr(Path, "resolve", mock_resolve)

        result = log_cleanup.cleanup_logs(log_root, retention_days=30, now=now)
        assert result.deleted_count == 0
        assert old_log.exists()


class TestCleanupRetentionCapacityConsistency:
    """Retention and capacity share the same safe discovery set."""

    def test_retention_and_capacity_use_same_discovered_set(self, tmp_path: Path) -> None:
        """Both retention and capacity passes operate on the same ``discovered``
        list, so a symlink file rejected by discovery is invisible to BOTH
        passes. There is no second traversal with a different policy.
        """

        now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
        log_root = tmp_path / "managed-logs"
        log_root.mkdir()
        runs_dir = log_root / "runs"
        runs_dir.mkdir()

        # A real old run log that WILL be deleted by retention.
        old_run = _touch_run_log(
            runs_dir / "20250101_000000_run-20250101-000000-abc.log",
            now - timedelta(days=365),
            "run-20250101-000000-abc",
            content=b"x" * 2048,
        )

        # A symlink "run log" pointing at an external file — must NOT be
        # discovered by either retention or capacity.
        external = tmp_path / "external"
        external.mkdir()
        external_target = _touch(external / "external.log", now - timedelta(days=365), b"external")
        link_log = _make_symlink_or_skip(runs_dir, "20250102_000000_run-20250102-000000-link.log", external_target)

        _authorize(log_root)
        # Retention=30 days, cap=1KB. The real run log is 2KB and 365 days old.
        # Both passes should only see the real run log; the symlink is invisible.
        result = log_cleanup.cleanup_logs(
            log_root,
            retention_days=30,
            now=now,
            keep_recent_runs=0,
            max_bytes=1024,
        )

        # The real run log was deleted.
        assert not old_run.exists()
        # The external target and the symlink are untouched.
        assert external_target.exists()
        assert link_log.exists()
        # Only 1 file was deleted (the real run log), not 2.
        assert result.deleted_count == 1
