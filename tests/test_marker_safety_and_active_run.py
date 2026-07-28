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
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from src.tools import log_cleanup, run_logging, run_update_daily
from src.tools.run_logging import RunLogContext
from src.utils import filesystem_safety, paths
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


def _touch_run_log(path: Path, mtime: datetime, run_id: str, content: bytes = b"log") -> Path:
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
        (tmp_path / MANAGED_ROOT_MARKER).write_text("not valid json {{{", encoding="utf-8")
        with pytest.raises(LogRootAuthorizationError, match=r"corrupt|unreadable"):
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

    def test_ensure_is_idempotent_for_valid_marker(self, tmp_path: Path) -> None:
        """7. ``ensure_managed_log_root`` is idempotent for a VALID marker.

        A second call on a directory that already carries a correct marker
        must NOT overwrite it and must NOT raise. This is the P0-3 contract:
        ``ensure`` validates the existing marker (rather than blindly trusting
        it) and a valid marker passes validation.
        """
        tmp_path.mkdir(parents=True, exist_ok=True)
        ensure_managed_log_root(tmp_path)
        first_payload = _read_marker(tmp_path)
        # A second call must succeed (idempotent) and leave the marker intact.
        ensure_managed_log_root(tmp_path)
        second_payload = _read_marker(tmp_path)
        assert first_payload == second_payload
        assert second_payload["application"] == MANAGED_ROOT_APPLICATION
        assert second_payload["layout_version"] == MANAGED_ROOT_LAYOUT_VERSION

    def test_ensure_rejects_wrong_application_marker(self, tmp_path: Path) -> None:
        """8. ``ensure_managed_log_root`` must NOT blindly trust an existing
        marker with a wrong application. It must raise so the tampered marker
        is surfaced, not silently passed as authorized.
        """
        tmp_path.mkdir(parents=True, exist_ok=True)
        # Pre-write a marker with a different application.
        _write_marker(tmp_path, application="PreExisting")
        with pytest.raises(LogRootAuthorizationError, match="application mismatch"):
            ensure_managed_log_root(tmp_path)
        # The marker must be untouched (ensure does not overwrite on rejection).
        payload = _read_marker(tmp_path)
        assert payload["application"] == "PreExisting"

    def test_ensure_rejects_corrupt_marker(self, tmp_path: Path) -> None:
        """9. ``ensure_managed_log_root`` must reject a corrupt marker."""
        tmp_path.mkdir(parents=True, exist_ok=True)
        marker = tmp_path / MANAGED_ROOT_MARKER
        marker.write_text("{not valid json", encoding="utf-8")
        with pytest.raises(LogRootAuthorizationError, match=r"unreadable|corrupt"):
            ensure_managed_log_root(tmp_path)

    def test_ensure_rejects_wrong_version_marker(self, tmp_path: Path) -> None:
        """10. ``ensure_managed_log_root`` must reject an unsupported version."""
        tmp_path.mkdir(parents=True, exist_ok=True)
        _write_marker(tmp_path, layout_version=999)
        with pytest.raises(LogRootAuthorizationError, match="unsupported"):
            ensure_managed_log_root(tmp_path)


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
        root = Path("C:\\") if os.name == "nt" else Path("/")
        with pytest.raises(LogRootAuthorizationError, match=r"filesystem root|drive root"):
            paths._reject_unsafe_log_root(root)

    def test_rejects_drive_root_windows(self) -> None:
        """12b. A bare Windows drive root (C:\\) is rejected."""
        if os.name != "nt":
            pytest.skip("drive-root rejection is Windows-specific")
        with pytest.raises(LogRootAuthorizationError, match=r"drive root|filesystem root"):
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

    def test_rejects_windows_junction(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """12f. A Windows junction/reparse point is rejected."""
        junction_dir = tmp_path / "junction-logs"
        junction_dir.mkdir(parents=True, exist_ok=True)

        # Simulate a junction by patching ``inspect_filesystem_node``. The
        # production code (``_reject_unsafe_log_root``) calls
        # ``inspect_filesystem_node`` which uses ``os.lstat`` and
        # ``st_file_attributes & FILE_ATTRIBUTE_REPARSE_POINT`` — NOT
        # ``Path.is_junction()`` (Python 3.12+ only). Patching the shared
        # inspection API is sufficient regardless of Python version.
        original_inspect = filesystem_safety.inspect_filesystem_node
        fake_junction = filesystem_safety.FilesystemNodeInspection(
            path=junction_dir,
            kind=filesystem_safety.FilesystemNodeKind.JUNCTION,
            mode=0o040755,
            file_attributes=0,
            reparse_tag=filesystem_safety._IO_REPARSE_TAG_MOUNT_POINT,
        )

        def fake_inspect(path: Path) -> filesystem_safety.FilesystemNodeInspection:
            if Path(path) == junction_dir:
                return fake_junction
            return original_inspect(path)

        monkeypatch.setattr(filesystem_safety, "inspect_filesystem_node", fake_inspect)

        with pytest.raises(LogRootAuthorizationError, match="junction"):
            paths._reject_unsafe_log_root(junction_dir)

    def test_rejects_uninspectable_root(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """12g. A root whose ``inspect_filesystem_node`` raises ``OSError``
        (other than ``FileNotFoundError``) is rejected fail-closed. An
        uninspectable root is NEVER treated as a safe regular directory.
        """

        uninspectable = tmp_path / "uninspectable-logs"
        uninspectable.mkdir(parents=True, exist_ok=True)
        original_inspect = filesystem_safety.inspect_filesystem_node

        def fake_inspect(path: Path) -> filesystem_safety.FilesystemNodeInspection:
            if Path(path) == uninspectable:
                raise PermissionError("denied")
            return original_inspect(path)

        monkeypatch.setattr(filesystem_safety, "inspect_filesystem_node", fake_inspect)

        with pytest.raises(LogRootAuthorizationError, match="uninspectable"):
            paths._reject_unsafe_log_root(uninspectable)

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

    def test_active_run_protection_works_with_keep_recent_runs(self, tmp_path: Path) -> None:
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


# ---------------------------------------------------------------------------
# P0-3: --initialize-managed-root CLI flag
# ---------------------------------------------------------------------------


class TestInitializeManagedRootCli:
    """The ``--initialize-managed-root`` flag creates the marker and exits
    without running cleanup. It is the remediation path referenced by the
    error message in ``validate_managed_log_root``.
    """

    def test_initialize_creates_marker_and_exits_without_cleanup(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """--initialize-managed-root creates the marker and does NOT run
        cleanup (no files are deleted). The CLI exits 0.
        """
        from click.testing import CliRunner

        log_root = tmp_path / "logs"
        log_root.mkdir(parents=True, exist_ok=True)
        # Place a stale log file that cleanup WOULD delete — confirm it survives.
        stale_log = log_root / "stale.log"
        stale_log.write_bytes(b"old")

        runner = CliRunner()
        result = runner.invoke(
            log_cleanup.main,
            ["--initialize-managed-root", "--log-dir", str(log_root)],
        )
        assert result.exit_code == 0, result.output
        # Marker was created.
        assert (log_root / MANAGED_ROOT_MARKER).exists()
        # Cleanup did NOT run — the stale log is still there.
        assert stale_log.exists()
        # Output mentions authorization.
        assert "authorized" in result.output.lower()

    def test_initialize_idempotent_for_valid_marker(self, tmp_path: Path) -> None:
        """--initialize-managed-root on a directory with a valid marker
        succeeds (idempotent) and does NOT overwrite the marker.
        """
        from click.testing import CliRunner

        log_root = tmp_path / "logs"
        log_root.mkdir(parents=True, exist_ok=True)
        _authorize(log_root)
        first_payload = _read_marker(log_root)

        runner = CliRunner()
        result = runner.invoke(
            log_cleanup.main,
            ["--initialize-managed-root", "--log-dir", str(log_root)],
        )
        assert result.exit_code == 0, result.output
        second_payload = _read_marker(log_root)
        assert first_payload == second_payload

    def test_initialize_rejects_corrupt_marker(self, tmp_path: Path) -> None:
        """--initialize-managed-root must NOT overwrite a corrupt marker.
        It must exit non-zero so the user knows the marker is tampered.
        """
        from click.testing import CliRunner

        log_root = tmp_path / "logs"
        log_root.mkdir(parents=True, exist_ok=True)
        marker = log_root / MANAGED_ROOT_MARKER
        marker.write_text("{corrupt", encoding="utf-8")

        runner = CliRunner()
        result = runner.invoke(
            log_cleanup.main,
            ["--initialize-managed-root", "--log-dir", str(log_root)],
        )
        assert result.exit_code != 0
        # The corrupt marker is untouched.
        assert marker.read_text(encoding="utf-8") == "{corrupt"

    def test_initialize_rejects_wrong_application_marker(self, tmp_path: Path) -> None:
        """--initialize-managed-root must NOT overwrite a marker with the
        wrong application. It must exit non-zero.
        """
        from click.testing import CliRunner

        log_root = tmp_path / "logs"
        log_root.mkdir(parents=True, exist_ok=True)
        _write_marker(log_root, application="OtherApp")

        runner = CliRunner()
        result = runner.invoke(
            log_cleanup.main,
            ["--initialize-managed-root", "--log-dir", str(log_root)],
        )
        assert result.exit_code != 0
        # The marker is untouched.
        assert _read_marker(log_root)["application"] == "OtherApp"

    def test_initialize_rejects_repo_internal_path(self) -> None:
        """--initialize-managed-root must reject a repo-internal path."""
        from click.testing import CliRunner

        runner = CliRunner()
        result = runner.invoke(
            log_cleanup.main,
            ["--initialize-managed-root", "--log-dir", str(paths.ROOT / "logs")],
        )
        assert result.exit_code != 0

    def test_initialize_rejects_home(self) -> None:
        """--initialize-managed-root must reject the user home directory."""
        import pathlib

        from click.testing import CliRunner

        runner = CliRunner()
        result = runner.invoke(
            log_cleanup.main,
            ["--initialize-managed-root", "--log-dir", str(pathlib.Path.home())],
        )
        assert result.exit_code != 0


# ---------------------------------------------------------------------------
# P0-3: adopt_run_log_context marker contract
# ---------------------------------------------------------------------------


class TestAdoptRunLogContextMarker:
    """``adopt_run_log_context`` must authorize the managed root via
    ``ensure_managed_log_root`` so cleanup can later clean the directory.
    """

    def test_adopt_creates_marker_for_default_logs_dir(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """When the adopted log lives inside ``logs_dir``, the marker is
        created on ``logs_dir`` (NOT on the ``runs/`` subdirectory).
        """
        log_root = tmp_path / "logs"
        runtime_paths = paths.resolve_runtime_paths(explicit_log_dir=log_root)
        run_log_path = runtime_paths.run_logs_dir / "adopted.log"

        ctx = run_logging.adopt_run_log_context(
            path=run_log_path,
            runtime_paths=runtime_paths,
        )
        assert ctx.path.exists()
        # Marker is on logs_dir, NOT on run_logs_dir.
        assert (log_root / MANAGED_ROOT_MARKER).exists()
        assert not (runtime_paths.run_logs_dir / MANAGED_ROOT_MARKER).exists()

    def test_adopt_creates_marker_for_external_path(self, tmp_path: Path) -> None:
        """When the adopted log lives outside ``logs_dir``, the marker is
        created on the parent of the log file.
        """
        external_root = tmp_path / "external-logs"
        external_root.mkdir(parents=True, exist_ok=True)
        run_log_path = external_root / "adopted.log"
        runtime_paths = paths.resolve_runtime_paths(explicit_log_dir=tmp_path / "default-logs")

        run_logging.adopt_run_log_context(
            path=run_log_path,
            runtime_paths=runtime_paths,
        )
        # Marker is on the external root (parent of the log file).
        assert (external_root / MANAGED_ROOT_MARKER).exists()

    def test_adopt_rejects_repo_internal_path(self) -> None:
        """Adopting a log path inside the repo must fail fast."""
        runtime_paths = paths.resolve_runtime_paths()
        bad_path = paths.ROOT / "logs" / "run.log"
        with pytest.raises(run_logging.RunLogContextError, match="repositor"):
            run_logging.adopt_run_log_context(path=bad_path, runtime_paths=runtime_paths)

    def test_adopt_rejects_home_path(self) -> None:
        """Adopting a log path in the user home must fail fast."""
        import pathlib

        runtime_paths = paths.resolve_runtime_paths()
        bad_path = pathlib.Path.home() / "run.log"
        with pytest.raises(run_logging.RunLogContextError, match="home"):
            run_logging.adopt_run_log_context(path=bad_path, runtime_paths=runtime_paths)


# ---------------------------------------------------------------------------
# P0-3: create_run_log_context explicit path safety
# ---------------------------------------------------------------------------


class TestCreateRunLogContextExplicitPath:
    """``create_run_log_context`` with an explicit path must validate the
    path's safety. Paths inside the repo, home, filesystem root, a symlink
    root, or a junction root must be rejected fail-fast.
    """

    def test_explicit_path_inside_logs_dir_uses_logs_dir_marker(self, tmp_path: Path) -> None:
        """When the explicit path lives inside ``logs_dir``, the marker is
        created on ``logs_dir``.
        """
        log_root = tmp_path / "logs"
        runtime_paths = paths.resolve_runtime_paths(explicit_log_dir=log_root)
        explicit_path = runtime_paths.run_logs_dir / "explicit.log"

        ctx = run_logging.create_run_log_context(
            runtime_paths=runtime_paths,
            explicit_path=explicit_path,
        )
        assert ctx.path.exists()
        # Marker is on logs_dir.
        assert (log_root / MANAGED_ROOT_MARKER).exists()
        assert not (runtime_paths.run_logs_dir / MANAGED_ROOT_MARKER).exists()

    def test_explicit_path_outside_logs_dir_uses_parent_marker(self, tmp_path: Path) -> None:
        """When the explicit path lives outside ``logs_dir``, the marker is
        created on the parent of the log file.
        """
        external_root = tmp_path / "external"
        external_root.mkdir(parents=True, exist_ok=True)
        explicit_path = external_root / "explicit.log"
        runtime_paths = paths.resolve_runtime_paths(explicit_log_dir=tmp_path / "default")

        run_logging.create_run_log_context(
            runtime_paths=runtime_paths,
            explicit_path=explicit_path,
        )
        # Marker is on the external root.
        assert (external_root / MANAGED_ROOT_MARKER).exists()

    def test_explicit_path_in_repo_rejected(self) -> None:
        """An explicit path inside the repo must be rejected."""
        runtime_paths = paths.resolve_runtime_paths()
        bad_path = paths.ROOT / "logs" / "run.log"
        with pytest.raises(run_logging.RunLogContextError, match="repositor"):
            run_logging.create_run_log_context(
                runtime_paths=runtime_paths,
                explicit_path=bad_path,
            )

    def test_explicit_path_in_home_rejected(self) -> None:
        """An explicit path in the user home must be rejected."""
        import pathlib

        runtime_paths = paths.resolve_runtime_paths()
        bad_path = pathlib.Path.home() / "run.log"
        with pytest.raises(run_logging.RunLogContextError, match="home"):
            run_logging.create_run_log_context(
                runtime_paths=runtime_paths,
                explicit_path=bad_path,
            )


# ---------------------------------------------------------------------------
# P0-3: Root symlink rejection (ALL symlinks, not just escape-parent)
# ---------------------------------------------------------------------------


class TestRootSymlinkRejection:
    """P0-3 contract: ALL root symlinks are rejected, not just those that
    escape the parent. The marker binds to a directory identity; a symlink
    target can be swapped after authorization.
    """

    def test_symlink_root_that_stays_inside_parent_is_rejected(self, tmp_path: Path) -> None:
        """A symlink root whose target is INSIDE the parent directory must
        STILL be rejected. The old behavior only rejected symlinks that
        escaped; the new behavior rejects all symlink roots.
        """
        real_dir = tmp_path / "real-logs"
        real_dir.mkdir(parents=True, exist_ok=True)
        link = tmp_path / "link-logs"
        try:
            link.symlink_to(real_dir)
        except OSError:
            pytest.skip("symlink not supported on this platform")
        # The symlink target is inside tmp_path (the parent), so the old
        # "escape-parent" check would have allowed it. The new "reject all"
        # check must reject it.
        with pytest.raises(LogRootAuthorizationError, match="symlink"):
            paths._reject_unsafe_log_root(link)

    def test_symlink_root_that_escapes_parent_is_rejected(self, tmp_path: Path) -> None:
        """A symlink root whose target escapes the parent is rejected."""
        outside = tmp_path / "outside"
        outside.mkdir(parents=True, exist_ok=True)
        parent = tmp_path / "parent"
        parent.mkdir(parents=True, exist_ok=True)
        link = parent / "link-logs"
        try:
            link.symlink_to(outside)
        except OSError:
            pytest.skip("symlink not supported on this platform")
        with pytest.raises(LogRootAuthorizationError, match="symlink"):
            paths._reject_unsafe_log_root(link)

    def test_ensure_rejects_symlink_root(self, tmp_path: Path) -> None:
        """``ensure_managed_log_root`` must reject a symlink root."""
        real_dir = tmp_path / "real-logs"
        real_dir.mkdir(parents=True, exist_ok=True)
        link = tmp_path / "link-logs"
        try:
            link.symlink_to(real_dir)
        except OSError:
            pytest.skip("symlink not supported on this platform")
        with pytest.raises(LogRootAuthorizationError, match="symlink"):
            ensure_managed_log_root(link)

    def test_validate_rejects_symlink_root(self, tmp_path: Path) -> None:
        """``validate_managed_log_root`` must reject a symlink root."""
        real_dir = tmp_path / "real-logs"
        real_dir.mkdir(parents=True, exist_ok=True)
        link = tmp_path / "link-logs"
        try:
            link.symlink_to(real_dir)
        except OSError:
            pytest.skip("symlink not supported on this platform")
        with pytest.raises(LogRootAuthorizationError, match="symlink"):
            validate_managed_log_root(link)


# ---------------------------------------------------------------------------
# P0: Entry-point raw-path validation (no .resolve() before safety check)
#
# Spec section 7: directly testing ``_reject_unsafe_log_root`` is NOT enough
# to prove the CLI / create / adopt entries do not call ``.resolve()`` first.
# These tests drive the actual entry points with a symlink managed root and
# assert the entry rejects it. If the entry called ``.resolve()`` before the
# safety check, the symlink would be washed away to its real target and the
# rejection would NOT fire — which is exactly the regression we must catch.
# ---------------------------------------------------------------------------


def _make_symlink_root(tmp_path: Path) -> tuple[Path, Path]:
    """Create a symlink managed root and return (link, real_dir).

    The real directory is pre-populated so the symlink resolves to a valid,
    non-escaping target — the safety check must STILL reject it because the
    managed root itself is a symlink. Skips the test on platforms without
    symlink support.
    """

    real_dir = tmp_path / "real-logs"
    real_dir.mkdir(parents=True, exist_ok=True)
    link = tmp_path / "link-logs"
    try:
        link.symlink_to(real_dir, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("symlink not supported on this platform")
    return link, real_dir


def _make_junction_root(tmp_path: Path) -> tuple[Path, Path]:
    """Create a Windows junction managed root and return (link, real_dir).

    Uses ``cmd /d /c mklink /J`` via ``subprocess``. On Windows, junction
    creation failure is a TEST FAILURE (not skip) — the error includes the
    command, exit code, stdout, and stderr so CI logs prove the junction tests
    actually ran. Callers must guard entry with ``@pytest.mark.skipif`` for
    non-Windows platforms.
    """

    real_dir = tmp_path / "real-logs"
    real_dir.mkdir(parents=True, exist_ok=True)
    link = tmp_path / "link-logs"
    result = subprocess.run(
        ["cmd", "/d", "/c", "mklink", "/J", str(link), str(real_dir)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise OSError(f"mklink /J failed (exit {result.returncode}): stdout={result.stdout!r} stderr={result.stderr!r}")
    return link, real_dir


class TestCliSymlinkRootRejection:
    """Spec 7: the ``log_cleanup`` CLI must reject a symlink managed root at
    the ENTRY — ``--log-dir`` must NOT be ``.resolve()``-d before the safety
    check runs. If it were, the symlink would be washed away and the marker
    would be created on the real target.
    """

    def test_initialize_managed_root_rejects_symlink(self, tmp_path: Path) -> None:
        """``--initialize-managed-root --log-dir <symlink>`` exits non-zero
        and does NOT create a marker on the symlink target.
        """

        from click.testing import CliRunner

        link, real_dir = _make_symlink_root(tmp_path)
        runner = CliRunner()
        result = runner.invoke(
            log_cleanup.main,
            ["--initialize-managed-root", "--log-dir", str(link)],
        )
        assert result.exit_code != 0
        # The real target must NOT receive a marker — the entry must not
        # have resolved the symlink before the safety check.
        assert not (real_dir / MANAGED_ROOT_MARKER).exists()
        # And the symlink path itself must not have a marker either.
        assert not (link / MANAGED_ROOT_MARKER).exists()

    def test_cleanup_rejects_symlink_root(self, tmp_path: Path) -> None:
        """``cleanup --log-dir <symlink>`` exits non-zero and does NOT delete
        any logs from the symlink target.
        """

        from click.testing import CliRunner

        link, real_dir = _make_symlink_root(tmp_path)
        # Pre-authorize the REAL dir (simulating an attacker who got a marker
        # placed on the real target somehow) and put a stale log there. The
        # CLI must STILL reject the symlink at the entry — cleanup must not
        # reach into the real dir via the symlink.
        _authorize(real_dir)
        stale_log = real_dir / "stale.log"
        stale_log.write_bytes(b"old")
        runner = CliRunner()
        result = runner.invoke(
            log_cleanup.main,
            ["--log-dir", str(link), "--retention-days", "0"],
        )
        assert result.exit_code != 0
        # The stale log must survive — cleanup did not run.
        assert stale_log.exists()

    @pytest.mark.skipif(sys.platform != "win32", reason="Windows junction test")
    @pytest.mark.windows_junction
    def test_initialize_managed_root_rejects_junction(self, tmp_path: Path) -> None:
        """``--initialize-managed-root --log-dir <junction>`` exits non-zero
        on Windows junctions. Junction creation failure is a test failure
        (not skip) so CI logs prove the test ran.
        """

        from click.testing import CliRunner

        link, real_dir = _make_junction_root(tmp_path)
        runner = CliRunner()
        result = runner.invoke(
            log_cleanup.main,
            ["--initialize-managed-root", "--log-dir", str(link)],
        )
        assert result.exit_code != 0
        assert not (real_dir / MANAGED_ROOT_MARKER).exists()


class TestCreateAdoptRunLogSymlinkRejection:
    """Spec 7: ``create_run_log_context`` and ``adopt_run_log_context`` must
    reject an explicit path whose managed root is a symlink. The entry must
    NOT call ``.resolve()`` before the safety check — otherwise a symlink
    managed root would be washed away to its real target.
    """

    def test_create_run_log_context_rejects_symlink_managed_root(self, tmp_path: Path) -> None:
        link, real_dir = _make_symlink_root(tmp_path)
        runtime_paths = paths.resolve_runtime_paths(explicit_log_dir=tmp_path / "default")
        explicit_path = link / "run.log"
        with pytest.raises(run_logging.RunLogContextError, match="symlink"):
            run_logging.create_run_log_context(
                runtime_paths=runtime_paths,
                explicit_path=explicit_path,
            )
        # No marker on the real target.
        assert not (real_dir / MANAGED_ROOT_MARKER).exists()

    def test_adopt_run_log_context_rejects_symlink_managed_root(self, tmp_path: Path) -> None:
        link, real_dir = _make_symlink_root(tmp_path)
        runtime_paths = paths.resolve_runtime_paths(explicit_log_dir=tmp_path / "default")
        run_log_path = link / "run.log"
        with pytest.raises(run_logging.RunLogContextError, match="symlink"):
            run_logging.adopt_run_log_context(
                path=run_log_path,
                runtime_paths=runtime_paths,
            )
        # No marker on the real target.
        assert not (real_dir / MANAGED_ROOT_MARKER).exists()

    @pytest.mark.skipif(sys.platform != "win32", reason="Windows junction test")
    @pytest.mark.windows_junction
    def test_create_run_log_context_rejects_junction_managed_root(self, tmp_path: Path) -> None:
        link, real_dir = _make_junction_root(tmp_path)
        runtime_paths = paths.resolve_runtime_paths(explicit_log_dir=tmp_path / "default")
        explicit_path = link / "run.log"
        with pytest.raises(run_logging.RunLogContextError):
            run_logging.create_run_log_context(
                runtime_paths=runtime_paths,
                explicit_path=explicit_path,
            )
        assert not (real_dir / MANAGED_ROOT_MARKER).exists()


class TestEntryRawPathValidationPatched:
    """Spec 7: "at least one test must cover the entry does not call
    .resolve() first". On platforms/permissions where real symlinks and
    junctions cannot be created, this test patches ``Path.is_symlink`` to
    simulate a symlink at the entry and asserts the entry rejects it WITHOUT
    having resolved it away.
    """

    def test_create_run_log_context_rejects_patched_symlink_root(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Patch ``filesystem_safety.inspect_filesystem_node`` to return a
        SYMLINK inspection for the managed root. If ``create_run_log_context``
        called ``.resolve()`` before the safety check, the patched
        ``inspect_filesystem_node`` would no longer see the symlink (because
        the resolved path is the real directory) and the entry would NOT
        raise — which is the regression we must catch.

        The production code uses ``filesystem_safety.inspect_filesystem_node``
        (via ``os.lstat``) rather than ``Path.is_symlink`` so that Windows
        junctions and other reparse points are also caught on Python 3.10/3.11
        where ``Path.is_junction()`` is unavailable.
        """

        log_root = tmp_path / "logs"
        log_root.mkdir(parents=True, exist_ok=True)
        runtime_paths = paths.resolve_runtime_paths(explicit_log_dir=log_root)
        explicit_path = runtime_paths.run_logs_dir / "run.log"

        # Patch the shared filesystem-safety inspection API to simulate a
        # symlink at the managed root. ``ensure_managed_log_root`` calls
        # ``_reject_unsafe_log_root`` which calls ``inspect_filesystem_node``
        # on the raw path BEFORE resolving.
        original_inspect = filesystem_safety.inspect_filesystem_node
        fake_symlink = filesystem_safety.FilesystemNodeInspection(
            path=runtime_paths.logs_dir,
            kind=filesystem_safety.FilesystemNodeKind.SYMLINK,
            mode=0o120755,
        )

        def _patched_inspect(path: Path) -> filesystem_safety.FilesystemNodeInspection:
            if Path(path) == runtime_paths.logs_dir:
                return fake_symlink
            return original_inspect(path)

        monkeypatch.setattr(filesystem_safety, "inspect_filesystem_node", _patched_inspect)

        with pytest.raises(run_logging.RunLogContextError, match="symlink"):
            run_logging.create_run_log_context(
                runtime_paths=runtime_paths,
                explicit_path=explicit_path,
            )


# ---------------------------------------------------------------------------
# P1: Active run ID normalization (spec section 11)
# ---------------------------------------------------------------------------


class TestNormalizeActiveRunIds:
    """Spec 11.3: ``_normalize_active_run_ids`` strips whitespace, filters
    empties, de-duplicates exactly (case-sensitive, no substring), and
    preserves first-seen order. CLI and env share the helper.
    """

    def test_single_value(self) -> None:
        assert log_cleanup._normalize_active_run_ids(["id-a"]) == ("id-a",)

    def test_comma_separated_with_spaces(self) -> None:
        # The helper receives already-split values; the CLI splits on commas.
        # Simulate the CLI's split output for "id-a, id-b , ,id-a".
        values = ["id-a", " id-b ", " ", "id-a"]
        assert log_cleanup._normalize_active_run_ids(values) == ("id-a", "id-b")

    def test_duplicate_values_de_duplicated(self) -> None:
        assert log_cleanup._normalize_active_run_ids(["id-a", "id-b", "id-a"]) == (
            "id-a",
            "id-b",
        )

    def test_empty_values_filtered(self) -> None:
        assert log_cleanup._normalize_active_run_ids(["", "  ", "id-a", ""]) == ("id-a",)

    def test_case_sensitive(self) -> None:
        assert log_cleanup._normalize_active_run_ids(["ID-a", "id-a"]) == (
            "ID-a",
            "id-a",
        )

    def test_preserves_first_seen_order(self) -> None:
        assert log_cleanup._normalize_active_run_ids(["id-b", "id-a", "id-b"]) == (
            "id-b",
            "id-a",
        )


class TestActiveRunIdProtectionWithWhitespace:
    """Spec 11.3: active run IDs with surrounding whitespace must still be
    protected by retention and capacity. The CLI/env parsing must strip the
    whitespace before the protection check, and ``cleanup_logs`` must
    defensively normalize too.
    """

    def test_active_id_with_spaces_protected_from_retention(self, tmp_path: Path) -> None:
        """A run log tagged ``" run-abc "`` (with spaces) must be protected
        when the caller passes ``"  run-abc  "`` in ``active_run_ids``.
        """

        now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        _authorize(log_dir)
        # Run logs live in the ``runs/`` subdirectory; only files there are
        # tagged with an ``active_run_id`` by ``_discover_log_files``.
        runs_dir = log_dir / "runs"
        runs_dir.mkdir(parents=True, exist_ok=True)
        # Old run log (would be deleted by retention) — but it's active.
        old_time = now - timedelta(days=365)
        run_log = _touch_run_log(runs_dir / "run-abc.log", old_time, run_id="run-abc")
        # Caller passes the id with surrounding whitespace.
        result = log_cleanup.cleanup_logs(
            log_dir,
            retention_days=30,
            now=now,
            active_run_ids=("  run-abc  ",),
        )
        assert result.deleted_count == 0
        assert run_log.exists()

    def test_active_id_with_spaces_protected_from_capacity(self, tmp_path: Path) -> None:
        """A run log tagged ``"run-abc"`` must be protected from capacity
        eviction when the caller passes ``" run-abc"`` (leading space).
        """

        now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        _authorize(log_dir)
        runs_dir = log_dir / "runs"
        runs_dir.mkdir(parents=True, exist_ok=True)
        old_time = now - timedelta(days=365)
        run_log = _touch_run_log(runs_dir / "run-abc.log", old_time, run_id="run-abc")
        # Capacity cap of 1 byte forces eviction of everything not protected.
        log_cleanup.cleanup_logs(
            log_dir,
            retention_days=0,
            now=now,
            max_bytes=1,
            active_run_ids=(" run-abc",),
        )
        assert run_log.exists()

    def test_id_a_does_not_protect_id_ab(self, tmp_path: Path) -> None:
        """``active_run_ids=["id-a"]`` must NOT protect a run log tagged
        ``id-ab`` (no substring matching).
        """

        now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        _authorize(log_dir)
        runs_dir = log_dir / "runs"
        runs_dir.mkdir(parents=True, exist_ok=True)
        old_time = now - timedelta(days=365)
        other_log = _touch_run_log(runs_dir / "id-ab.log", old_time, run_id="id-ab")
        result = log_cleanup.cleanup_logs(
            log_dir,
            retention_days=0,
            keep_recent_runs=0,
            now=now,
            active_run_ids=("id-a",),
        )
        # id-ab is NOT protected by id-a — it should be deleted.
        assert result.deleted_count == 1
        assert not other_log.exists()

    def test_cli_env_merge_with_spaces(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """The CLI must merge ``--active-run-id`` and ``QDC_ACTIVE_RUN_ID``
        (comma-separated) and strip whitespace from both. A run log tagged
        ``run-abc`` must be protected when the env var is ``" run-abc "``.
        """

        from click.testing import CliRunner

        # Use a fixed old mtime so retention_days=0 will delete the log
        # unless it is protected by the active-run-id check.
        old_time = datetime(2025, 7, 25, 12, 0, tzinfo=timezone.utc)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        _authorize(log_dir)
        runs_dir = log_dir / "runs"
        runs_dir.mkdir(parents=True, exist_ok=True)
        run_log = _touch_run_log(runs_dir / "run-abc.log", old_time, run_id="run-abc")
        monkeypatch.setenv("QDC_ACTIVE_RUN_ID", " run-abc ,")
        runner = CliRunner()
        result = runner.invoke(
            log_cleanup.main,
            ["--log-dir", str(log_dir), "--retention-days", "0"],
        )
        assert result.exit_code == 0, result.output
        assert run_log.exists()
