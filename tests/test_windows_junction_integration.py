"""Real Windows junction integration tests for log cleanup safety.

These tests create REAL Windows junctions (via ``cmd /d /c mklink /J``) and
verify that ``cleanup_logs`` does NOT recurse into them or delete files
reachable through them. They run ONLY on Windows (skipped elsewhere).

GitHub Windows runners support junction creation, so these tests MUST execute
there. If junction creation fails for a genuine OS reason (e.g. insufficient
privilege), the test skips with the specific OS error — never a blanket
``except Exception``.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.tools import log_cleanup
from src.utils.paths import ensure_managed_log_root

pytestmark = pytest.mark.skipif(sys.platform != "win32", reason="Windows junction tests require Windows")


def _touch(path: Path, mtime: datetime, content: bytes = b"log") -> Path:
    """Create a file with a specific mtime."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    timestamp = mtime.timestamp()
    os.utime(path, (timestamp, timestamp))
    return path


def _create_junction(junction_path: Path, target: Path) -> None:
    """Create a Windows junction ``junction_path`` → ``target`` via mklink /J.

    Raises the specific ``OSError`` if the OS rejects the operation. Does NOT
    use a blanket ``except Exception`` — callers can distinguish "junctions
    not supported" from "test bug".
    """

    junction_path.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        ["cmd", "/d", "/c", "mklink", "/J", str(junction_path), str(target)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise OSError(f"mklink /J failed (exit {result.returncode}): stdout={result.stdout!r} stderr={result.stderr!r}")


def _is_junction(path: Path) -> bool:
    """Check if ``path`` is a junction/reparse point using the shared helper."""

    from src.utils import filesystem_safety

    return filesystem_safety.is_link_like_or_reparse(path)


class TestWindowsJunctionCleanupSafety:
    """Real junction tests: cleanup must not recurse into or delete through junctions."""

    def test_junction_directory_not_recursed_by_cleanup(self, tmp_path: Path) -> None:
        """A junction inside the managed log root is NOT recursed into.

        An old .log file reachable through the junction (living in the
        junction target's tree) must survive cleanup.
        """

        now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)

        # External directory with an old .log file.
        external = tmp_path / "external"
        external.mkdir()
        external_log = _touch(external / "old.log", now - timedelta(days=365))

        # Managed log root.
        log_root = tmp_path / "managed-logs"
        log_root.mkdir()
        # Create a junction inside the log root pointing at the external dir.
        junction = log_root / "junction-dir"
        try:
            _create_junction(junction, external)
        except OSError as exc:
            pytest.skip(f"Cannot create junction on this system: {exc}")

        # Verify the junction is detected as link-like.
        assert _is_junction(junction), f"junction not detected as reparse point: {junction}"

        ensure_managed_log_root(log_root)

        # Run retention cleanup — the external log must survive.
        result = log_cleanup.cleanup_logs(log_root, retention_days=30, now=now)
        assert result.deleted_count == 0
        assert external_log.exists()

    def test_junction_survives_capacity_pass(self, tmp_path: Path) -> None:
        """A very small capacity limit must NOT evict files reachable through
        a junction. The junction is pruned from discovery, so its content is
        invisible to the capacity pass.
        """

        now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)

        # External directory with a large old .log file.
        external = tmp_path / "external"
        external.mkdir()
        external_log = _touch(external / "big.log", now - timedelta(days=365), b"x" * 10240)

        # Managed log root with a real run log (small).
        log_root = tmp_path / "managed-logs"
        log_root.mkdir()
        runs_dir = log_root / "runs"
        runs_dir.mkdir()
        _touch(
            runs_dir / "20250101_000000_run-20250101-000000-abc.log",
            now - timedelta(days=365),
            b"real",
        )

        # Junction inside runs/ pointing at the external directory.
        junction = runs_dir / "junction-link"
        try:
            _create_junction(junction, external)
        except OSError as exc:
            pytest.skip(f"Cannot create junction on this system: {exc}")

        assert _is_junction(junction)

        ensure_managed_log_root(log_root)

        # Capacity = 100 bytes. The real run log (4 bytes) fits, but the
        # external big.log (10KB) would blow the cap IF it were discovered.
        # It must NOT be discovered because the junction is pruned.
        log_cleanup.cleanup_logs(
            log_root,
            retention_days=30,
            now=now,
            keep_recent_runs=0,
            max_bytes=100,
        )

        # The external big.log survives.
        assert external_log.exists()
        # The real run log may or may not be evicted by capacity (it's 4 bytes,
        # under the 100-byte cap), but it's 365 days old so retention deletes it.
        # The key assertion is that the external file is untouched.

    def test_normal_logs_inside_managed_root_still_deleted(self, tmp_path: Path) -> None:
        """A junction inside the log root does NOT prevent normal logs in the
        root from being deleted by retention. This proves the pruning is
        surgical — only the junction is skipped, not the entire root.
        """

        now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)

        # External directory with an old .log file.
        external = tmp_path / "external"
        external.mkdir()
        external_log = _touch(external / "external.log", now - timedelta(days=365))

        # Managed log root with a real old .log file.
        log_root = tmp_path / "managed-logs"
        log_root.mkdir()
        real_old_log = _touch(log_root / "real-old.log", now - timedelta(days=365))

        # Junction inside the log root.
        junction = log_root / "junction-dir"
        try:
            _create_junction(junction, external)
        except OSError as exc:
            pytest.skip(f"Cannot create junction on this system: {exc}")

        ensure_managed_log_root(log_root)

        result = log_cleanup.cleanup_logs(log_root, retention_days=30, now=now)

        # The real old log IS deleted (normal behavior preserved).
        assert not real_old_log.exists()
        # The external log through the junction is NOT deleted.
        assert external_log.exists()
        assert result.deleted_count == 1

    def test_junction_log_file_candidate_rejected(self, tmp_path: Path) -> None:
        """A junction named ``*.log`` is NOT treated as a log file candidate.

        Even though the junction name ends in ``.log``, it is a reparse point
        and must be rejected by the file-candidate safety check.
        """

        now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)

        # External target with an old .log file.
        external = tmp_path / "external"
        external.mkdir()
        external_log = _touch(external / "inside.log", now - timedelta(days=365))

        # Managed log root with a junction named ``fake.log``.
        log_root = tmp_path / "managed-logs"
        log_root.mkdir()
        junction_log = log_root / "fake.log"
        try:
            _create_junction(junction_log, external)
        except OSError as exc:
            pytest.skip(f"Cannot create junction on this system: {exc}")

        assert _is_junction(junction_log)

        ensure_managed_log_root(log_root)

        # Run cleanup. The junction ``fake.log`` must NOT be deleted, and the
        # external ``inside.log`` must NOT be deleted.
        result = log_cleanup.cleanup_logs(log_root, retention_days=30, now=now)
        assert result.deleted_count == 0
        assert external_log.exists()
        assert junction_log.exists()

    def test_junction_cleanup_is_reliable(self, tmp_path: Path) -> None:
        """Test cleanup: remove the junction reliably (rmdir, not rmtree).

        A junction must be removed with ``os.rmdir`` (which removes the
        reparse point, not the target). ``shutil.rmtree`` would follow the
        junction and delete the target tree.
        """

        now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
        external = tmp_path / "external"
        external.mkdir()
        external_log = _touch(external / "survivor.log", now - timedelta(days=1))

        log_root = tmp_path / "managed-logs"
        log_root.mkdir()
        junction = log_root / "junction-dir"
        try:
            _create_junction(junction, external)
        except OSError as exc:
            pytest.skip(f"Cannot create junction on this system: {exc}")

        # Remove the junction with rmdir (NOT rmtree).
        os.rmdir(junction)
        assert not junction.exists()
        # The external target must survive.
        assert external.exists()
        assert external_log.exists()
