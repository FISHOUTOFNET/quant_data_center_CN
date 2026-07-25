"""Tests for P0-7 (log cleanup marker as real auth boundary) and P0-8 (active run log protection).

P0-7: The managed-root marker (``.qdc-managed-log-root``) is the single
authorization boundary for log cleanup. ``cleanup_logs`` must refuse to delete
files from any directory that does not carry a valid marker. The marker is
created ONLY by ``paths.ensure_managed_log_root`` (called from
``create_run_log_context`` and application logging init) and validated by
``paths.validate_managed_log_root`` (called from ``cleanup_logs`` before any
deletion). Cleanup must NOT auto-create the marker — otherwise any external
directory could be claimed.

P0-8: The orchestrator propagates its run-id to the cleanup subprocess via
``QDC_ACTIVE_RUN_ID`` so that the in-flight run log is never deleted. The
``cleanup_logs`` function accepts an ``active_run_ids`` parameter; any run log
whose extracted run-id appears in that set is protected from deletion
regardless of age.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from src.tools import log_cleanup, run_logging, run_update_daily
from src.tools.run_logging import RunLogContext
from src.utils import paths
from src.utils.paths import (
    MANAGED_ROOT_APPLICATION,
    MANAGED_ROOT_LAYOUT_VERSION,
    MANAGED_ROOT_MARKER,
    LogRootAuthorizationError,
    ensure_managed_log_root,
    validate_managed_log_root,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _touch(path: Path, mtime: datetime, content: bytes = b"log") -> Path:
    """Create a file with a specific mtime."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    timestamp = mtime.timestamp()
    os.utime(path, (timestamp, timestamp))
    return path


def _touch_run_log(
    path: Path, mtime: datetime, run_id: str, content: bytes = b"log"
) -> Path:
    """Create a run-log file whose name embeds ``run_id``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    timestamp = mtime.timestamp()
    os.utime(path, (timestamp, timestamp))
    return path


def _write_marker(
    root: Path,
    *,
    application: str = MANAGED_ROOT_APPLICATION,
    layout_version: int = MANAGED_ROOT_LAYOUT_VERSION,
) -> Path:
    """Write a marker file with the requested payload (bypasses ensure)."""
    root.mkdir(parents=True, exist_ok=True)
    marker = root / MANAGED_ROOT_MARKER
    marker.write_text(
        json.dumps(
            {"application": application, "layout_version": layout_version},
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return marker


def _read_marker(root: Path) -> dict[str, Any]:
    """Read and parse the marker JSON payload."""
    return json.loads((root / MANAGED_ROOT_MARKER).read_text(encoding="utf-8"))


def _authorize(root: Path) -> Path:
    """Authorize ``root`` via the official authority (ensure_managed_log_root)."""
    ensure_managed_log_root(root)
    return root


# ---------------------------------------------------------------------------
# P0-7: Marker Safety — validate_managed_log_root
# ---------------------------------------------------------------------------


class TestP07MarkerValidation:
    """validate_managed_log_root is the fail-closed gate called before deletion."""

    def test_validate_raises_when_marker_missing(self, tmp_path: Path) -> None:
        """1. A missing marker must be rejected (cleanup must NOT auto-create)."""
        tmp_path.mkdir(parents=True, exist_ok=True)
        with pytest.raises(LogRootAuthorizationError, match="marker"):
            validate_managed_log_root(tmp_path)

    def test_validate_raises_when_marker_corrupt(self, tmp_path: Path) -> None:
        """2. Corrupt/invalid JSON in the marker must be rejected."""
        tmp_path.mkdir(parents=True, exist_ok=True)
        (tmp_path / MANAGED_ROOT_MARKER).write_text(
            "not valid json {{{", encoding="utf-8"
        )
        with pytest.raises(LogRootAuthorizationError, match="corrupt|unreadable"):
            validate_managed_log_root(tmp_path)

    def test_validate_raises_when_application_wrong(self, tmp_path: Path) -> None:
        """3. A marker belonging to a different application must be rejected."""
        _write_marker(tmp_path, application="OtherApp")
        with pytest.raises(LogRootAuthorizationError, match="application"):
            validate_managed_log_root(tmp_path)

    def test_validate_raises_when_layout_version_unsupported(self, tmp_path: Path) -> None:
        """4. An unsupported layout_version must be rejected."""
        _write_marker(tmp_path, layout_version=999)
        with pytest.raises(LogRootAuthorizationError, match="layout_version"):
            validate_managed_log_root(tmp_path)

    def test_validate_passes_with_correct_marker(self, tmp_path: Path) -> None:
        """5. A correct marker (right application + version) passes without error."""
        _authorize(tmp_path)
        # Must not raise.
        validate_managed_log_root(tmp_path)


# ---------------------------------------------------------------------------
# P0-7: Marker Safety — ensure_managed_log_root
# ---------------------------------------------------------------------------


class TestP07EnsureManagedLogRoot:
    """ensure_managed_log_root is the ONLY authority that creates the marker."""

    def test_ensure_creates_marker_with_correct_payload(self, tmp_path: Path) -> None:
        """6. The created marker has the right application and layout_version."""
        tmp_path.mkdir(parents=True, exist_ok=True)
        ensure_managed_log_root(tmp_path)

        marker = tmp_path / MANAGED_ROOT_MARKER
        assert marker.exists()
        payload = _read_marker(tmp_path)
        assert payload["application"] == MANAGED_ROOT_APPLICATION
        assert payload["layout_version"] == MANAGED_ROOT_LAYOUT_VERSION

    def test_ensure_is_idempotent_does_not_overwrite(self, tmp_path: Path) -> None:
        """7. An existing marker is never overwritten, even with different content."""
        tmp_path.mkdir(parents=True, exist_ok=True)
        # Pre-write a marker with a different application.
        _write_marker(tmp_path, application="PreExisting")

        ensure_managed_log_root(tmp_path)

        # The marker must be untouched.
        payload = _read_marker(tmp_path)
        assert payload["application"] == "PreExisting"


# ---------------------------------------------------------------------------
# P0-7: Marker Safety — symlink marker
# ---------------------------------------------------------------------------


class TestP07SymlinkMarker:
    """A symlink marker could be redirected at an attacker-controlled file."""

    def test_validate_rejects_symlink_marker(self, tmp_path: Path) -> None:
        """8. A symlink marker must be rejected."""
        tmp_path.mkdir(parents=True, exist_ok=True)
        # Create a real marker file elsewhere.
        real_marker = tmp_path / "real-marker"
        real_marker.write_text(
            json.dumps(
                {
                    "application": MANAGED_ROOT_APPLICATION,
                    "layout_version": MANAGED_ROOT_LAYOUT_VERSION,
                }
            ),
            encoding="utf-8",
        )
        # Create a log root with a symlink marker.
        log_root = tmp_path / "logs"
        log_root.mkdir(parents=True, exist_ok=True)
        link = log_root / MANAGED_ROOT_MARKER
        try:
            link.symlink_to(real_marker)
        except (OSError, NotImplementedError):
            pytest.skip("symlink not supported on this platform")

        with pytest.raises(LogRootAuthorizationError, match="symlink"):
            validate_managed_log_root(log_root)


# ---------------------------------------------------------------------------
# P0-7: Marker Safety — RuntimePaths and RunLogContext
# ---------------------------------------------------------------------------


class TestP07RuntimePathsAndRunLogContext:
    """The marker is created at the right point in the lifecycle."""

    def test_resolve_runtime_paths_does_not_create_marker(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """9. resolve_runtime_paths is a pure resolver — no marker side effects.

        The docstring explicitly states: "The directory is *not* created here;
        callers (configure_logging and RunLogContext) create it lazily so that
        read-only commands do not produce side effects."
        """
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        monkeypatch.setenv("QDC_LOG_DIR", str(log_dir))

        calls: list[Path] = []
        original = paths.ensure_managed_log_root

        def spy(root: Path) -> None:
            calls.append(root)
            original(root)

        monkeypatch.setattr(paths, "ensure_managed_log_root", spy)

        runtime = paths.resolve_runtime_paths()

        assert calls == []  # ensure_managed_log_root was NOT called
        assert not (log_dir / MANAGED_ROOT_MARKER).exists()
        assert runtime.logs_dir == log_dir.resolve()

    def test_create_run_log_context_creates_marker(self, tmp_path: Path) -> None:
        """10. create_run_log_context creates the marker in logs_dir."""
        runtime = paths.RuntimePaths(
            app_data_dir=tmp_path,
            logs_dir=tmp_path,
            application_log_path=tmp_path / "application" / "qdc.log",
            run_logs_dir=tmp_path / "runs",
        )
        now = datetime(2026, 7, 25, 10, 0, 0)

        run_logging.create_run_log_context(runtime_paths=runtime, now=now)

        marker = tmp_path / MANAGED_ROOT_MARKER
        assert marker.exists()
        payload = _read_marker(tmp_path)
        assert payload["application"] == MANAGED_ROOT_APPLICATION
        assert payload["layout_version"] == MANAGED_ROOT_LAYOUT_VERSION


# ---------------------------------------------------------------------------
# P0-7: Marker Safety — cleanup cannot auto-claim external directories
# ---------------------------------------------------------------------------


class TestP07CleanupAuthBoundary:
    """cleanup_logs must refuse directories without a valid marker."""

    def test_cleanup_refuses_dir_without_marker(self, tmp_path: Path) -> None:
        """11. A random directory with .log files but no marker is refused."""
        now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
        old_log = _touch(tmp_path / "old.log", now - timedelta(days=60))

        with pytest.raises(log_cleanup.LogCleanupError, match="marker"):
            log_cleanup.cleanup_logs(tmp_path, retention_days=30, now=now)

        # The log file must survive — cleanup was rejected before any deletion.
        assert old_log.exists()

    def test_cleanup_succeeds_after_ensure_managed_log_root(self, tmp_path: Path) -> None:
        """After ensure_managed_log_root, cleanup is authorized and runs."""
        now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
        old_log = _touch(tmp_path / "old.log", now - timedelta(days=60))
        recent_log = _touch(tmp_path / "recent.log", now - timedelta(days=1))

        _authorize(tmp_path)
        result = log_cleanup.cleanup_logs(tmp_path, retention_days=30, now=now)

        assert result.deleted_count == 1
        assert not old_log.exists()
        assert recent_log.exists()

    def test_ensure_managed_log_root_authorizes_cleanup(self, tmp_path: Path) -> None:
        """13. ensure_managed_log_root is the CLI --initialize-managed-root equivalent.

        The CLI does not currently expose --initialize-managed-root, but the
        error message in validate_managed_log_root points users at it. The
        underlying function (ensure_managed_log_root) is the authority that
        authorizes cleanup.
        """
        now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
        _touch(tmp_path / "expired.log", now - timedelta(days=90))

        # Before authorization: cleanup refuses.
        with pytest.raises(log_cleanup.LogCleanupError):
            log_cleanup.cleanup_logs(tmp_path, retention_days=30, now=now)

        # Authorize via the official authority.
        ensure_managed_log_root(tmp_path)

        # After authorization: cleanup succeeds.
        result = log_cleanup.cleanup_logs(tmp_path, retention_days=30, now=now)
        assert result.deleted_count == 1


# ---------------------------------------------------------------------------
# P0-7: Unsafe Root Rejection — _reject_unsafe_log_root
# ---------------------------------------------------------------------------


class TestP07UnsafeRootRejection:
    """_reject_unsafe_log_root rejects inherently dangerous roots."""

    def test_rejects_filesystem_root(self) -> None:
        """12a. The filesystem root (parent is itself) is rejected."""
        if os.name == "nt":
            root = Path("C:\\")
        else:
            root = Path("/")
        with pytest.raises(LogRootAuthorizationError, match="filesystem root|drive root"):
            paths._reject_unsafe_log_root(root)

    def test_rejects_drive_root_windows(self) -> None:
        """12b. A bare Windows drive root (C:\\) is rejected."""
        if os.name != "nt":
            pytest.skip("drive-root rejection is Windows-specific")
        with pytest.raises(LogRootAuthorizationError, match="drive root|filesystem root"):
            paths._reject_unsafe_log_root(Path("C:\\"))

    def test_rejects_user_home(self) -> None:
        """12c. The user home directory is rejected."""
        import pathlib

        home = pathlib.Path.home()
        with pytest.raises(LogRootAuthorizationError, match="home"):
            paths._reject_unsafe_log_root(home)

    def test_rejects_repo_root(self) -> None:
        """12d. The repository root is rejected."""
        with pytest.raises(LogRootAuthorizationError, match="repositor"):
            paths._reject_unsafe_log_root(paths.ROOT)

    def test_rejects_repo_internal_path(self) -> None:
        """12e. A path inside the repository is rejected."""
        repo_internal = paths.ROOT / "data" / "logs"
        with pytest.raises(LogRootAuthorizationError, match="repositor"):
            paths._reject_unsafe_log_root(repo_internal)

    def test_rejects_windows_junction(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """12f. A Windows junction/reparse point is rejected."""
        junction_dir = tmp_path / "junction-logs"
        junction_dir.mkdir(parents=True, exist_ok=True)

        # Simulate a junction by patching is_junction. The production code
        # uses getattr(root, "is_junction", lambda: False)() so patching the
        # class attribute is sufficient regardless of Python version.
        original = getattr(Path, "is_junction", None)

        def fake_is_junction(self: Path) -> bool:
            if self == junction_dir:
                return True
            if original is not None:
                return original(self)
            return False

        monkeypatch.setattr(Path, "is_junction", fake_is_junction, raising=False)

        with pytest.raises(LogRootAuthorizationError, match="junction"):
            paths._reject_unsafe_log_root(junction_dir)

    def test_unsafe_root_rejected_by_ensure(self, tmp_path: Path) -> None:
        """ensure_managed_log_root also rejects unsafe roots (shared check)."""
        with pytest.raises(LogRootAuthorizationError, match="repositor"):
            ensure_managed_log_root(paths.ROOT)

    def test_unsafe_root_rejected_by_validate(self, tmp_path: Path) -> None:
        """validate_managed_log_root also rejects unsafe roots (shared check)."""
        with pytest.raises(LogRootAuthorizationError, match="repositor"):
            validate_managed_log_root(paths.ROOT)


# ---------------------------------------------------------------------------
# P0-8: Active Run Protection — cleanup_logs(active_run_ids=...)
# ---------------------------------------------------------------------------


class TestP08ActiveRunProtection:
    """Active run logs are protected from cleanup deletion regardless of age."""

    def test_active_run_log_is_protected_from_deletion(self, tmp_path: Path) -> None:
        """1. A run log matching an active run_id is NOT deleted even if ancient."""
        now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
        runs_dir = tmp_path / "runs"
        active_id = "run-20250101-000000-active1"
        ancient_active = _touch_run_log(
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
        assert ancient_active.exists()
        assert result.kept_reasons.get(log_cleanup.KEEP_REASON_ACTIVE_RUN) == 1

    def test_old_logs_not_in_active_run_ids_are_deleted(self, tmp_path: Path) -> None:
        """2. Old run logs that are NOT in active_run_ids are deleted."""
        now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
        runs_dir = tmp_path / "runs"
        active_id = "run-20260725-100000-active1"
        active_log = _touch_run_log(
            runs_dir / "20260725_100000_run-20260725-100000-active1.log",
            now - timedelta(days=365),  # ancient but active
            active_id,
        )
        old_inactive = _touch_run_log(
            runs_dir / "20250101_000000_run-20250101-000000-inactive.log",
            now - timedelta(days=365),  # ancient and NOT active
            "run-20250101-000000-inactive",
        )

        _authorize(tmp_path)
        result = log_cleanup.cleanup_logs(
            tmp_path,
            retention_days=30,
            now=now,
            keep_recent_runs=0,
            active_run_ids={active_id},
        )

        assert result.deleted_count == 1
        assert active_log.exists()
        assert not old_inactive.exists()

    def test_run_log_with_active_id_in_filename_protected(self, tmp_path: Path) -> None:
        """4. A run log file with the active run ID embedded in its name is protected."""
        now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
        runs_dir = tmp_path / "runs"
        active_id = "run-20260725-120000-abcdef"
        # Filename format: {stamp}_{run_id}.log
        protected = _touch_run_log(
            runs_dir / "20260725_120000_run-20260725-120000-abcdef.log",
            now - timedelta(days=200),
            active_id,
        )
        # An old flat log (not a run log) that should be deleted.
        old_flat = _touch(tmp_path / "flat-old.log", now - timedelta(days=200))

        _authorize(tmp_path)
        result = log_cleanup.cleanup_logs(
            tmp_path,
            retention_days=30,
            now=now,
            keep_recent_runs=0,
            active_run_ids={active_id},
        )

        assert protected.exists()
        assert not old_flat.exists()
        assert result.kept_reasons.get(log_cleanup.KEEP_REASON_ACTIVE_RUN) == 1

    def test_multiple_active_run_ids_all_protected(self, tmp_path: Path) -> None:
        """5. Multiple active run IDs are all protected from deletion."""
        now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
        runs_dir = tmp_path / "runs"
        active_id_a = "run-20260725-100000-aaaaaa"
        active_id_b = "run-20260725-110000-bbbbbb"
        active_id_c = "run-20260725-120000-cccccc"

        protected_a = _touch_run_log(
            runs_dir / "20260725_100000_run-20260725-100000-aaaaaa.log",
            now - timedelta(days=365),
            active_id_a,
        )
        protected_b = _touch_run_log(
            runs_dir / "20260725_110000_run-20260725-110000-bbbbbb.log",
            now - timedelta(days=365),
            active_id_b,
        )
        protected_c = _touch_run_log(
            runs_dir / "20260725_120000_run-20260725-120000-cccccc.log",
            now - timedelta(days=365),
            active_id_c,
        )
        # An old inactive run log that should be deleted.
        old_inactive = _touch_run_log(
            runs_dir / "20250101_000000_run-20250101-000000-inactive.log",
            now - timedelta(days=365),
            "run-20250101-000000-inactive",
        )

        _authorize(tmp_path)
        result = log_cleanup.cleanup_logs(
            tmp_path,
            retention_days=30,
            now=now,
            keep_recent_runs=0,
            active_run_ids={active_id_a, active_id_b, active_id_c},
        )

        assert result.deleted_count == 1
        assert protected_a.exists()
        assert protected_b.exists()
        assert protected_c.exists()
        assert not old_inactive.exists()
        assert result.kept_reasons.get(log_cleanup.KEEP_REASON_ACTIVE_RUN) == 3

    def test_active_run_protection_works_with_keep_recent_runs(
        self, tmp_path: Path
    ) -> None:
        """Active run protection takes priority over both retention and recent-run."""
        now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
        runs_dir = tmp_path / "runs"
        active_id = "run-20260725-100000-active"
        # The active log is the OLDEST by mtime, but must still be protected.
        active_log = _touch_run_log(
            runs_dir / "20260725_100000_run-20260725-100000-active.log",
            now - timedelta(days=365),
            active_id,
        )
        # Create some newer logs that are NOT active — these are protected by
        # keep_recent_runs (they're the most recent).
        for i in range(3):
            stamp = (now - timedelta(days=i)).strftime("%Y%m%d_%H%M%S")
            rid = f"run-{stamp}-nonce{i}"
            _touch_run_log(
                runs_dir / f"{stamp}_{rid}.log",
                now - timedelta(days=i),
                rid,
            )

        _authorize(tmp_path)
        result = log_cleanup.cleanup_logs(
            tmp_path,
            retention_days=30,
            now=now,
            keep_recent_runs=10,  # keep all recent
            active_run_ids={active_id},
        )

        assert active_log.exists()
        assert result.kept_reasons.get(log_cleanup.KEEP_REASON_ACTIVE_RUN) == 1


# ---------------------------------------------------------------------------
# P0-8: Active Run Protection — orchestrator env var propagation
# ---------------------------------------------------------------------------


class _CapturingPopen:
    """Popen stand-in that captures spawn kwargs (esp. env) for assertions."""

    pid = 1234

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.captured_kwargs = kwargs

    def wait(self, timeout: int | None = None) -> int:
        return 0

    def poll(self) -> int | None:
        return 0


class TestP08OrchestratorEnvPropagation:
    """The orchestrator sets QDC_ACTIVE_RUN_ID when a run_log_context is provided.

    This is a focused re-test here so the active-run-protection story is
    self-contained in this file. The full coverage lives in
    test_progress_path_and_stall.py.
    """

    def test_run_subprocess_sets_active_run_id_when_run_log_context_provided(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """3. _run_subprocess sets QDC_ACTIVE_RUN_ID from run_log_context.run_id."""
        log_file = tmp_path / "run.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)
        # Use a non-derived step so _run_subprocess calls proc.wait() directly
        # (no stall-detection path).
        step = run_update_daily.DailyStep(
            id="akshare-daily-bar",
            name="akshare update daily_bar",
            command=("qdc", "akshare", "update", "--target", "daily_bar"),
        )
        captured: dict[str, Any] = {}

        class _Capturing(_CapturingPopen):
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                super().__init__(*args, **kwargs)
                captured["env"] = kwargs.get("env")

        monkeypatch.setattr(run_update_daily.subprocess, "Popen", _Capturing)

        run_log_context = RunLogContext(
            run_id="run-20260725-120000-abcdef",
            path=log_file,
            created_at=datetime(2026, 7, 25, 12, 0, 0),
        )
        run_update_daily._run_subprocess(
            step,
            log_file,
            tmp_path,
            run_log_context=run_log_context,
        )

        env = captured["env"]
        assert env is not None
        assert env[run_update_daily.QDC_ACTIVE_RUN_ID_ENV] == "run-20260725-120000-abcdef"

    def test_run_subprocess_omits_active_run_id_when_context_absent(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When no run_log_context is provided, QDC_ACTIVE_RUN_ID is NOT set."""
        log_file = tmp_path / "run.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)
        step = run_update_daily.DailyStep(
            id="akshare-daily-bar",
            name="akshare update daily_bar",
            command=("qdc", "akshare", "update", "--target", "daily_bar"),
        )
        captured: dict[str, Any] = {}

        class _Capturing(_CapturingPopen):
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                super().__init__(*args, **kwargs)
                captured["env"] = kwargs.get("env")

        monkeypatch.setattr(run_update_daily.subprocess, "Popen", _Capturing)

        run_update_daily._run_subprocess(step, log_file, tmp_path)

        env = captured["env"]
        assert env is not None
        assert run_update_daily.QDC_ACTIVE_RUN_ID_ENV not in env
