"""Tests for the unified runtime path resolution and per-run log context."""

from __future__ import annotations

import os
import uuid
from datetime import datetime
from pathlib import Path

import pytest

from src.tools import run_logging
from src.utils import paths


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
