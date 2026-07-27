"""Tests for the unified runtime path resolution and per-run log context."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from src.tools import run_logging
from src.utils import paths
from src.utils.paths import LogRootAuthorizationError

# ---------------------------------------------------------------------------
# RuntimePaths
# ---------------------------------------------------------------------------


def test_default_log_dir_is_outside_git_workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The default log root must NOT live inside the project repository."""

    # Use a LOCALAPPDATA that is a sibling of the repo, not inside it.
    fake_local_app_data = tmp_path.parent / "fake_app_data_for_test"
    fake_local_app_data.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("LOCALAPPDATA", str(fake_local_app_data))
    monkeypatch.delenv("QDC_LOG_DIR", raising=False)
    monkeypatch.setenv("QDC_ROOT", str(tmp_path))

    runtime = paths.resolve_runtime_paths()

    expected = fake_local_app_data / "QuantDataCenter" / "logs"
    assert runtime.logs_dir == expected.resolve()
    assert runtime.application_log_path == expected.resolve() / "application" / "qdc.log"
    assert runtime.run_logs_dir == expected.resolve() / "runs"
    # The default must not be inside the project root.
    assert not paths.is_path_inside(runtime.logs_dir, tmp_path)


def test_qdc_log_dir_env_overrides_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    custom = tmp_path / "custom-logs"
    monkeypatch.setenv("QDC_LOG_DIR", str(custom))
    monkeypatch.setenv("QDC_ROOT", str(tmp_path))

    runtime = paths.resolve_runtime_paths()

    assert runtime.logs_dir == custom.resolve()
    assert runtime.application_log_path == custom.resolve() / "application" / "qdc.log"


def test_explicit_log_dir_wins_over_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env_dir = tmp_path / "env-logs"
    explicit_dir = tmp_path / "explicit-logs"
    monkeypatch.setenv("QDC_LOG_DIR", str(env_dir))

    runtime = paths.resolve_runtime_paths(explicit_log_dir=explicit_dir)

    assert runtime.logs_dir == explicit_dir.resolve()


def test_unicode_and_spaces_in_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Windows usernames may contain non-ASCII characters and spaces."""

    unicode_dir = tmp_path / "用户 孙弈" / "logs dir"
    monkeypatch.setenv("QDC_LOG_DIR", str(unicode_dir))

    runtime = paths.resolve_runtime_paths()

    assert runtime.logs_dir == unicode_dir.resolve()
    assert "孙弈" in str(runtime.logs_dir)


def test_config_logs_dir_inside_repo_is_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A settings.yaml ``logs_dir`` that resolves inside the repo is ignored."""

    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "settings.yaml").write_text(
        "project:\n  name: test\npaths:\n  logs_dir: logs\n",
        encoding="utf-8",
    )
    # Use a LOCALAPPDATA that is OUTSIDE the repo so the fallback is clean.
    fake_local_app_data = tmp_path.parent / "fake_app_data_for_inside_repo_test"
    fake_local_app_data.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("LOCALAPPDATA", str(fake_local_app_data))
    monkeypatch.setenv("QDC_ROOT", str(tmp_path))
    monkeypatch.delenv("QDC_LOG_DIR", raising=False)

    runtime = paths.resolve_runtime_paths(root=tmp_path)

    # Should fall through to the OS default, NOT the in-repo ``logs`` dir.
    assert not paths.is_path_inside(runtime.logs_dir, tmp_path)
    assert runtime.logs_dir == (fake_local_app_data / "QuantDataCenter" / "logs").resolve()


def test_config_logs_dir_outside_repo_is_respected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # External path must be a sibling of tmp_path, not inside it.
    external = tmp_path.parent / "external_logs_for_test"
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "settings.yaml").write_text(
        f"project:\n  name: test\npaths:\n  logs_dir: {external.as_posix()}\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("QDC_ROOT", str(tmp_path))
    monkeypatch.delenv("QDC_LOG_DIR", raising=False)

    runtime = paths.resolve_runtime_paths(root=tmp_path)

    assert runtime.logs_dir == external.resolve()


# ---------------------------------------------------------------------------
# RunLogContext
# ---------------------------------------------------------------------------


def test_create_run_log_context_creates_file(tmp_path: Path) -> None:
    runtime = paths.RuntimePaths(
        app_data_dir=tmp_path,
        logs_dir=tmp_path,
        application_log_path=tmp_path / "application" / "qdc.log",
        run_logs_dir=tmp_path / "runs",
    )
    now = datetime(2026, 7, 25, 10, 30, 0)

    context = run_logging.create_run_log_context(runtime_paths=runtime, now=now)

    assert context.path.exists()
    assert context.path.parent == tmp_path / "runs"
    assert context.run_id.startswith("run-20260725-103000-")
    assert context.created_at == now


def test_create_run_log_context_refuses_overwrite(tmp_path: Path) -> None:
    """Creating a log at an existing explicit path must fail fast."""

    log_path = tmp_path / "runs" / "existing.log"
    log_path.parent.mkdir(parents=True)
    log_path.touch()

    with pytest.raises(run_logging.RunLogContextError):
        run_logging.create_run_log_context(explicit_path=log_path, now=datetime(2026, 7, 25, 10, 0, 0))


def test_adopt_run_log_context_preserves_existing_content(tmp_path: Path) -> None:
    log_path = tmp_path / "runs" / "existing.log"
    log_path.parent.mkdir(parents=True)
    log_path.write_text("[bat header]\n", encoding="utf-8")

    context = run_logging.adopt_run_log_context(path=log_path, now=datetime(2026, 7, 25, 10, 0, 0))

    assert context.path == log_path
    assert "[bat header]" in log_path.read_text(encoding="utf-8")


def test_status_payload_reports_available_when_file_exists(tmp_path: Path) -> None:
    log_path = tmp_path / "run.log"
    log_path.write_text("hello", encoding="utf-8")
    context = run_logging.RunLogContext(
        run_id="run-test",
        path=log_path,
        created_at=datetime(2026, 7, 25, 10, 0, 0),
    )

    payload = context.status_payload(now=datetime(2026, 7, 25, 10, 5, 0))

    assert payload["log_status"] == "available"
    assert payload["run_id"] == "run-test"
    assert payload["log_path"] == str(log_path)
    assert payload["log_created_at"] == "2026-07-25T10:00:00"
    assert payload["log_last_write_at"] is not None


def test_status_payload_reports_missing_when_file_deleted(tmp_path: Path) -> None:
    log_path = tmp_path / "vanished.log"
    context = run_logging.RunLogContext(
        run_id="run-test",
        path=log_path,
        created_at=datetime(2026, 7, 25, 10, 0, 0),
    )

    payload = context.status_payload(now=datetime(2026, 7, 25, 10, 5, 0))

    assert payload["log_status"] == "missing"
    assert payload["log_last_write_at"] is None


def test_run_log_created_once_and_shared(tmp_path: Path) -> None:
    """The orchestrator creates the log exactly once; subprocesses adopt it."""

    runtime = paths.RuntimePaths(
        app_data_dir=tmp_path,
        logs_dir=tmp_path,
        application_log_path=tmp_path / "application" / "qdc.log",
        run_logs_dir=tmp_path / "runs",
    )
    now = datetime(2026, 7, 25, 10, 0, 0)

    # Orchestrator creates the log.
    created = run_logging.create_run_log_context(runtime_paths=runtime, now=now)
    # Subprocess adopts the same path.
    adopted = run_logging.adopt_run_log_context(path=created.path, now=now)

    assert created.path == adopted.path
    assert created.path.exists()


def test_disable_file_log_env_for_subprocess_returns_compat_flag() -> None:
    env = run_logging.disable_file_log_env_for_subprocess()
    assert env["QDC_DISABLE_FILE_LOG"] == "1"


# ---------------------------------------------------------------------------
# P0: Raw-identity check for log-root authorization
#
# All four sources (explicit, environment, settings, default) must flow through
# ``_authorize_raw_log_root`` → ``validate_managed_root_candidate`` which checks
# the RAW (unresolved) candidate for symlink/junction/reparse identity BEFORE
# calling ``.resolve()``. A symlink managed root is rejected regardless of
# which source supplied it.
# ---------------------------------------------------------------------------


def _make_symlink(tmp_path: Path, name: str, target: Path) -> Path | None:
    """Create a symlink ``name`` → ``target`` under ``tmp_path``.

    Returns the symlink path, or ``None`` if symlinks are not supported.
    """

    link = tmp_path / name
    try:
        link.symlink_to(target)
    except OSError:
        return None
    return link


class TestRawIdentityRejectionExplicit:
    """explicit_log_dir source: symlink/junction managed root is rejected."""

    def test_explicit_symlink_log_dir_is_rejected(self, tmp_path: Path) -> None:
        real_dir = tmp_path / "real-logs"
        real_dir.mkdir()
        link = _make_symlink(tmp_path, "link-logs", real_dir)
        if link is None:
            pytest.skip("symlink not supported on this platform")

        with pytest.raises(LogRootAuthorizationError, match="symlink"):
            paths.resolve_runtime_paths(explicit_log_dir=link)

    def test_explicit_normal_log_dir_still_resolves(self, tmp_path: Path) -> None:
        real_dir = tmp_path / "real-logs"
        real_dir.mkdir()

        runtime = paths.resolve_runtime_paths(explicit_log_dir=real_dir)

        assert runtime.logs_dir == real_dir.resolve()


class TestRawIdentityRejectionEnvironment:
    """QDC_LOG_DIR source: symlink/junction managed root is rejected."""

    def test_env_symlink_log_dir_is_rejected(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        real_dir = tmp_path / "real-logs"
        real_dir.mkdir()
        link = _make_symlink(tmp_path, "link-logs", real_dir)
        if link is None:
            pytest.skip("symlink not supported on this platform")

        monkeypatch.setenv("QDC_LOG_DIR", str(link))
        monkeypatch.setenv("QDC_ROOT", str(tmp_path))

        with pytest.raises(LogRootAuthorizationError, match="symlink"):
            paths.resolve_runtime_paths()

    def test_env_normal_log_dir_still_resolves(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        real_dir = tmp_path / "real-logs"
        real_dir.mkdir()
        monkeypatch.setenv("QDC_LOG_DIR", str(real_dir))
        monkeypatch.setenv("QDC_ROOT", str(tmp_path))

        runtime = paths.resolve_runtime_paths()

        assert runtime.logs_dir == real_dir.resolve()


class TestRawIdentityRejectionSettings:
    """settings.yaml paths.logs_dir source: symlink managed root is rejected."""

    def test_settings_symlink_log_dir_is_rejected(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        real_dir = tmp_path.parent / "real-external-logs"
        real_dir.mkdir(parents=True, exist_ok=True)
        link = _make_symlink(tmp_path.parent, "link-external-logs", real_dir)
        if link is None:
            pytest.skip("symlink not supported on this platform")

        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "settings.yaml").write_text(
            f"project:\n  name: test\npaths:\n  logs_dir: {link.as_posix()}\n",
            encoding="utf-8",
        )
        monkeypatch.setenv("QDC_ROOT", str(tmp_path))
        monkeypatch.delenv("QDC_LOG_DIR", raising=False)

        with pytest.raises(LogRootAuthorizationError, match="symlink"):
            paths.resolve_runtime_paths(root=tmp_path)

    def test_settings_normal_log_dir_still_resolves(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        external = tmp_path.parent / "external_logs_for_settings_test"
        external.mkdir(parents=True, exist_ok=True)
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "settings.yaml").write_text(
            f"project:\n  name: test\npaths:\n  logs_dir: {external.as_posix()}\n",
            encoding="utf-8",
        )
        monkeypatch.setenv("QDC_ROOT", str(tmp_path))
        monkeypatch.delenv("QDC_LOG_DIR", raising=False)

        runtime = paths.resolve_runtime_paths(root=tmp_path)

        assert runtime.logs_dir == external.resolve()


class TestRawIdentityRejectionDefault:
    """Default log root source: symlink managed root is rejected.

    The default path is ``$LOCALAPPDATA/QuantDataCenter/logs`` on Windows or
    ``$XDG_DATA_HOME/QuantDataCenter/logs`` / ``~/.local/share/QuantDataCenter/logs``
    on POSIX. When the leaf ``logs`` directory is itself a symlink, the
    raw-identity check must reject it.
    """

    def test_default_symlink_log_dir_is_rejected(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        # Set up a fake LOCALAPPDATA that is OUTSIDE the repo.
        fake_app_data = tmp_path.parent / "fake_app_data_for_default_symlink"
        fake_app_data.mkdir(parents=True, exist_ok=True)
        qdc_root = fake_app_data / "QuantDataCenter"
        qdc_root.mkdir(parents=True, exist_ok=True)

        # Create a real target directory and a symlink ``logs`` → target.
        real_logs = qdc_root / "real-logs"
        real_logs.mkdir()
        link = _make_symlink(qdc_root, "logs", real_logs)
        if link is None:
            pytest.skip("symlink not supported on this platform")

        monkeypatch.setenv("LOCALAPPDATA", str(fake_app_data))
        monkeypatch.setenv("QDC_ROOT", str(tmp_path))
        monkeypatch.delenv("QDC_LOG_DIR", raising=False)

        with pytest.raises(LogRootAuthorizationError, match="symlink"):
            paths.resolve_runtime_paths()

    def test_default_normal_log_dir_still_resolves(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_app_data = tmp_path.parent / "fake_app_data_for_default_normal"
        fake_app_data.mkdir(parents=True, exist_ok=True)
        monkeypatch.setenv("LOCALAPPDATA", str(fake_app_data))
        monkeypatch.setenv("QDC_ROOT", str(tmp_path))
        monkeypatch.delenv("QDC_LOG_DIR", raising=False)

        runtime = paths.resolve_runtime_paths()

        expected = fake_app_data / "QuantDataCenter" / "logs"
        assert runtime.logs_dir == expected.resolve()


class TestRawIdentityPriority:
    """Priority: explicit > env > settings > default. No regressions."""

    def test_explicit_wins_over_env(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        env_dir = tmp_path / "env-logs"
        env_dir.mkdir()
        explicit_dir = tmp_path / "explicit-logs"
        explicit_dir.mkdir()
        monkeypatch.setenv("QDC_LOG_DIR", str(env_dir))

        runtime = paths.resolve_runtime_paths(explicit_log_dir=explicit_dir)

        assert runtime.logs_dir == explicit_dir.resolve()

    def test_env_wins_over_settings(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        env_dir = tmp_path / "env-logs"
        env_dir.mkdir()
        external = tmp_path.parent / "settings-logs"
        external.mkdir(parents=True, exist_ok=True)
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "settings.yaml").write_text(
            f"project:\n  name: test\npaths:\n  logs_dir: {external.as_posix()}\n",
            encoding="utf-8",
        )
        monkeypatch.setenv("QDC_LOG_DIR", str(env_dir))
        monkeypatch.setenv("QDC_ROOT", str(tmp_path))

        runtime = paths.resolve_runtime_paths(root=tmp_path)

        assert runtime.logs_dir == env_dir.resolve()

    def test_settings_wins_over_default(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        external = tmp_path.parent / "settings-logs-over-default"
        external.mkdir(parents=True, exist_ok=True)
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "settings.yaml").write_text(
            f"project:\n  name: test\npaths:\n  logs_dir: {external.as_posix()}\n",
            encoding="utf-8",
        )
        monkeypatch.setenv("QDC_ROOT", str(tmp_path))
        monkeypatch.delenv("QDC_LOG_DIR", raising=False)
        # Set a fake LOCALAPPDATA too so the default would be different.
        fake_app_data = tmp_path.parent / "fake_app_data_for_priority"
        fake_app_data.mkdir(parents=True, exist_ok=True)
        monkeypatch.setenv("LOCALAPPDATA", str(fake_app_data))

        runtime = paths.resolve_runtime_paths(root=tmp_path)

        assert runtime.logs_dir == external.resolve()


class TestRawIdentityCheckHappensBeforeResolve:
    """The symlink check must run on the RAW path, not the resolved path.

    This is verified by creating a symlink whose TARGET is itself inside a
    safe directory. If the code incorrectly resolved first, the symlink would
    be washed away to the target and the check would pass. The check must
    reject the symlink itself.
    """

    def test_symlink_to_safe_dir_still_rejected(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        # Create a safe real directory that WOULD pass all checks.
        safe_dir = tmp_path.parent / "safe-target"
        safe_dir.mkdir(parents=True, exist_ok=True)

        # Create a symlink pointing at the safe directory.
        link = _make_symlink(tmp_path, "link-to-safe", safe_dir)
        if link is None:
            pytest.skip("symlink not supported on this platform")

        monkeypatch.setenv("QDC_LOG_DIR", str(link))
        monkeypatch.setenv("QDC_ROOT", str(tmp_path))

        # Must reject even though the TARGET is safe — the raw identity is a symlink.
        with pytest.raises(LogRootAuthorizationError, match="symlink"):
            paths.resolve_runtime_paths()
