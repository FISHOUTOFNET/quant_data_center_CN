"""Tests for P0-3 (fixed progress path) and P0-4 (stalled exit code).

P0-3: The orchestrator (``src.tools.run_update_daily``) must generate a unique
progress path per (run_instance, step_id) BEFORE spawn, pass it to the child
via env vars ``QDC_DERIVED_PROGRESS_PATH`` and ``QDC_DERIVED_RUN_ID``, and the
stall detector must read ONLY that path — never a directory scan.

P0-4: The stall detector (``_wait_with_stall_detection``) must preserve the
stall verdict: once stalled, return ``STALLED_EXIT_CODE`` (126) regardless of
the child's exit code. If process-tree cleanup fails, escalate to
``TIMEOUT_CLEANUP_FAILED_EXIT_CODE`` (125).
"""

from __future__ import annotations

import io
import json
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from src.tools import run_update_daily
from src.tools.run_logging import RunLogContext

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_step(
    step_id: str = "build-derived-daily-bar",
    *,
    target: str = "daily_bar",
    timeout_seconds: int | None = None,
) -> run_update_daily.DailyStep:
    """Build a DailyStep resembling the real build-derived command shape."""

    return run_update_daily.DailyStep(
        id=step_id,
        name=f"build derived {target}",
        command=(
            "qdc",
            "build-derived",
            "--target",
            target,
            "--mode",
            "incremental",
            "--no-build-duckdb-views",
        ),
        timeout_seconds=timeout_seconds,
    )


def _write_progress_state(
    path: Path,
    *,
    processed: int = 100,
    total: int = 500,
    stage: str = "BUILDING_PARTITIONS",
    heartbeat_age_seconds: int = 0,
) -> None:
    """Write a progress state JSON file with the requested heartbeat age.

    ``heartbeat_age_seconds=0`` produces a fresh heartbeat (now); larger values
    push ``heartbeat_at`` into the past so ``check_stall`` declares it stale.
    """

    heartbeat_at = datetime.now() - timedelta(seconds=heartbeat_age_seconds)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "stage": stage,
                "processed": processed,
                "total": total,
                "failed": 0,
                "current_security_id": "sh.600000",
                "throughput_per_minute": 10.0,
                "heartbeat_at": heartbeat_at.isoformat(timespec="seconds"),
                "started_at": heartbeat_at.isoformat(timespec="seconds"),
                "failed_security_ids": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


class _FakePopen:
    """Minimal Popen-like object for stall detection tests.

    The first ``poll()`` returns ``None`` (still running) so the stall check
    runs. Subsequent ``poll()`` calls return the configured exit code so any
    cleanup helper that checks liveness sees the child as exited.

    ``wait()`` returns the configured exit code unless
    ``wait_raises_timeout`` is set, in which case it raises
    :class:`subprocess.TimeoutExpired` (simulating a child that ignores the
    graceful SIGINT and has to be force-killed).
    """

    pid = 4242

    def __init__(
        self,
        *,
        exit_code_after_interrupt: int = 0,
        wait_raises_timeout: bool = False,
    ) -> None:
        self._exit_code_after_interrupt = exit_code_after_interrupt
        self._wait_raises_timeout = wait_raises_timeout
        self.poll_calls = 0
        self.wait_calls: list[int | None] = []
        self.send_signal_calls: list[Any] = []

    def poll(self) -> int | None:
        self.poll_calls += 1
        # First poll returns None so the stall check runs; afterwards the
        # child is "done" with the configured exit code.
        if self.poll_calls == 1:
            return None
        return self._exit_code_after_interrupt

    def wait(self, timeout: int | None = None) -> int:
        self.wait_calls.append(timeout)
        if self._wait_raises_timeout:
            raise subprocess.TimeoutExpired(cmd=("fake",), timeout=timeout)
        return self._exit_code_after_interrupt

    def send_signal(self, signal: int) -> None:
        self.send_signal_calls.append(signal)


class _CapturingPopen:
    """Popen stand-in that captures the spawn kwargs (esp. env) for asserts."""

    pid = 1234

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.captured_kwargs = kwargs

    def wait(self, timeout: int | None = None) -> int:
        return 0

    def poll(self) -> int | None:
        return 0


def _stale_pinned_path(
    tmp_path: Path,
    *,
    run_instance: str = "run_instance:20260725_120000",
    step_id: str = "build-derived-daily-bar",
    heartbeat_age_seconds: int = 35 * 60,
) -> Path:
    """Create a pinned progress path file with a stale heartbeat."""

    pinned = run_update_daily._derived_progress_path_for_step(tmp_path, run_instance, step_id)
    _write_progress_state(pinned, heartbeat_age_seconds=heartbeat_age_seconds)
    return pinned


# ---------------------------------------------------------------------------
# P0-3: Fixed Progress Path Tests
# ---------------------------------------------------------------------------


class TestP03FixedProgressPath:
    """P0-3: orchestrator generates a unique progress path per (run_instance,
    step_id) BEFORE spawn and passes it to the child via env vars.
    """

    def test_derived_progress_path_for_step_returns_unique_path(self, tmp_path: Path) -> None:
        run_instance = "run_instance:20260725_120000"
        step_id = "build-derived-daily-bar"
        path = run_update_daily._derived_progress_path_for_step(tmp_path, run_instance, step_id)

        assert path == (
            tmp_path
            / "data"
            / "metadata"
            / "derived-step-progress"
            / "20260725_120000"
            / "build-derived-daily-bar.state.json"
        )

    def test_different_run_instances_produce_different_paths(self, tmp_path: Path) -> None:
        step_id = "build-derived-daily-bar"
        path_a = run_update_daily._derived_progress_path_for_step(tmp_path, "run_instance:20260725_120000", step_id)
        path_b = run_update_daily._derived_progress_path_for_step(tmp_path, "run_instance:20260726_120000", step_id)

        assert path_a != path_b
        # Both live under the same ``derived-step-progress`` parent but in
        # different run-instance subdirectories.
        assert path_a.parent.parent == path_b.parent.parent
        assert path_a.parent != path_b.parent

    def test_different_step_ids_produce_different_paths(self, tmp_path: Path) -> None:
        run_instance = "run_instance:20260725_120000"
        path_a = run_update_daily._derived_progress_path_for_step(tmp_path, run_instance, "build-derived-daily-bar")
        path_b = run_update_daily._derived_progress_path_for_step(tmp_path, run_instance, "build-derived-valuation")

        assert path_a != path_b
        # Same run-instance directory, different filenames.
        assert path_a.parent == path_b.parent
        assert path_a.name == "build-derived-daily-bar.state.json"
        assert path_b.name == "build-derived-valuation.state.json"

    def test_step_uses_derived_stall_detection_true_for_daily_bar_only(self) -> None:
        daily_bar_step = _make_step("build-derived-daily-bar", target="daily_bar")
        valuation_step = _make_step("build-derived-valuation", target="valuation")

        assert run_update_daily._step_uses_derived_stall_detection(daily_bar_step) is True
        # Valuation is NOT in DERIVED_STALL_TARGETS, so it must NOT use
        # derived stall detection.
        assert run_update_daily._step_uses_derived_stall_detection(valuation_step) is False

    def test_step_uses_derived_stall_detection_false_for_non_build_derived(self) -> None:
        # An akshare step whose command happens to contain ``--target daily_bar``
        # must NOT use derived stall detection: the contract only applies to
        # ``build-derived-*`` steps.
        non_build_step = run_update_daily.DailyStep(
            id="akshare-daily-bar",
            name="akshare update daily_bar",
            command=("qdc", "akshare", "update", "--target", "daily_bar"),
        )
        assert run_update_daily._step_uses_derived_stall_detection(non_build_step) is False

    def test_extract_derived_target_returns_daily_bar(self) -> None:
        step = _make_step("build-derived-daily-bar", target="daily_bar")
        assert run_update_daily._extract_derived_target(step) == "daily_bar"

    def test_extract_derived_target_supports_equals_form(self) -> None:
        step = run_update_daily.DailyStep(
            id="build-derived-daily-bar",
            name="build derived daily_bar",
            command=("qdc", "build-derived", "--target=daily_bar", "--mode", "incremental"),
        )
        assert run_update_daily._extract_derived_target(step) == "daily_bar"

    def test_extract_derived_target_returns_none_when_missing(self) -> None:
        step = run_update_daily.DailyStep(
            id="build-derived-daily-bar",
            name="build derived daily_bar",
            command=("qdc", "build-derived", "--mode", "incremental"),
        )
        assert run_update_daily._extract_derived_target(step) is None

    def test_derived_stall_targets_is_daily_bar_only(self) -> None:
        # P0-3 contract: valuation must NOT be in DERIVED_STALL_TARGETS.
        assert {"daily_bar"} == run_update_daily.DERIVED_STALL_TARGETS
        assert "valuation" not in run_update_daily.DERIVED_STALL_TARGETS

    def test_run_subprocess_sets_derived_progress_env_for_daily_bar(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        log_file = tmp_path / "run.log"
        step = _make_step("build-derived-daily-bar", target="daily_bar")
        captured: dict[str, Any] = {}

        class _Capturing(_CapturingPopen):
            def __init__(self, *args: Any, **kwargs: Any) -> None:  # type: ignore[no-redef]
                super().__init__(*args, **kwargs)
                captured["env"] = kwargs.get("env")

        monkeypatch.setattr(run_update_daily.subprocess, "Popen", _Capturing)

        run_instance_key = "run_instance:20260725_120000"
        run_update_daily._run_subprocess(
            step,
            log_file,
            tmp_path,
            run_instance_key=run_instance_key,
        )

        env = captured["env"]
        assert env is not None
        expected_path = run_update_daily._derived_progress_path_for_step(tmp_path, run_instance_key, step.id)
        assert env[run_update_daily.QDC_DERIVED_PROGRESS_PATH_ENV] == str(expected_path)
        assert env[run_update_daily.QDC_DERIVED_RUN_ID_ENV] == f"{run_instance_key}:{step.id}"

    def test_run_subprocess_does_not_set_derived_progress_env_for_valuation(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        log_file = tmp_path / "run.log"
        # Valuation is NOT in DERIVED_STALL_TARGETS, so the derived env vars
        # must NOT be set.
        step = _make_step("build-derived-valuation", target="valuation")
        captured: dict[str, Any] = {}

        class _Capturing(_CapturingPopen):
            def __init__(self, *args: Any, **kwargs: Any) -> None:  # type: ignore[no-redef]
                super().__init__(*args, **kwargs)
                captured["env"] = kwargs.get("env")

        monkeypatch.setattr(run_update_daily.subprocess, "Popen", _Capturing)

        run_update_daily._run_subprocess(
            step,
            log_file,
            tmp_path,
            run_instance_key="run_instance:20260725_120000",
        )

        env = captured["env"]
        assert env is not None
        assert run_update_daily.QDC_DERIVED_PROGRESS_PATH_ENV not in env
        assert run_update_daily.QDC_DERIVED_RUN_ID_ENV not in env

    def test_run_subprocess_sets_active_run_id_when_run_log_context_provided(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        log_file = tmp_path / "run.log"
        step = _make_step("build-derived-daily-bar", target="daily_bar")
        captured: dict[str, Any] = {}

        class _Capturing(_CapturingPopen):
            def __init__(self, *args: Any, **kwargs: Any) -> None:  # type: ignore[no-redef]
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
            run_instance_key="run_instance:20260725_120000",
        )

        env = captured["env"]
        assert env is not None
        assert env[run_update_daily.QDC_ACTIVE_RUN_ID_ENV] == "run-20260725-120000-abcdef"

    def test_run_subprocess_omits_active_run_id_when_run_log_context_absent(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        log_file = tmp_path / "run.log"
        step = _make_step("build-derived-daily-bar", target="daily_bar")
        captured: dict[str, Any] = {}

        class _Capturing(_CapturingPopen):
            def __init__(self, *args: Any, **kwargs: Any) -> None:  # type: ignore[no-redef]
                super().__init__(*args, **kwargs)
                captured["env"] = kwargs.get("env")

        monkeypatch.setattr(run_update_daily.subprocess, "Popen", _Capturing)

        # No run_log_context provided → QDC_ACTIVE_RUN_ID must NOT be set.
        run_update_daily._run_subprocess(
            step,
            log_file,
            tmp_path,
            run_instance_key="run_instance:20260725_120000",
        )

        env = captured["env"]
        assert env is not None
        assert run_update_daily.QDC_ACTIVE_RUN_ID_ENV not in env

    def test_stall_detector_reads_only_pinned_path_not_directory_scan(
        self,
        tmp_path: Path,
    ) -> None:
        """An older stale progress file must NOT cause a false stall.

        The orchestrator pins a unique path per (run_instance, step_id) and
        reads only that path. A stale file from a previous run (in a sibling
        run_instance subdirectory) must not influence the current step's stall
        detection.
        """

        current_run_instance = "run_instance:20260725_120000"
        old_run_instance = "run_instance:20260724_120000"
        step_id = "build-derived-daily-bar"

        # Stale file from a previous run lives in a sibling run_instance dir.
        old_pinned_path = run_update_daily._derived_progress_path_for_step(tmp_path, old_run_instance, step_id)
        _write_progress_state(old_pinned_path, heartbeat_age_seconds=3600)

        # Current run's pinned path has a FRESH heartbeat.
        current_pinned_path = run_update_daily._derived_progress_path_for_step(tmp_path, current_run_instance, step_id)
        _write_progress_state(current_pinned_path, heartbeat_age_seconds=0)

        step = _make_step(step_id, target="daily_bar")
        # First poll() returns None (still running); then wait() returns 0
        # because the fresh heartbeat means the stall check returns False and
        # the loop falls through to proc.wait().
        proc = _FakePopen(exit_code_after_interrupt=0)

        exit_code = run_update_daily._wait_with_stall_detection(
            proc,
            step,
            io.StringIO(),
            tmp_path,
            progress_path=current_pinned_path,
            derived_run_id=f"{current_run_instance}:{step_id}",
        )

        # Fresh heartbeat on the pinned path → no stall → child exit code 0.
        assert exit_code == 0
        # Stall detector must not have invoked the interrupt path.
        assert proc.send_signal_calls == []


# ---------------------------------------------------------------------------
# P0-4: Stalled Exit Code Tests
# ---------------------------------------------------------------------------


class TestP04StalledExitCode:
    """P0-4: stall verdict must be preserved regardless of child exit code."""

    @pytest.fixture
    def stale_pinned_path(self, tmp_path: Path) -> Path:
        """A pinned progress path with a stale heartbeat (35 minutes old).

        35 minutes exceeds the default ``DEFAULT_STALL_SECONDS`` (30 minutes),
        so ``check_stall`` returns ``stalled=True`` on the first poll (where
        ``previous_processed`` is still ``None``).
        """

        return _stale_pinned_path(tmp_path, heartbeat_age_seconds=35 * 60)

    def test_stalled_returns_126_even_if_child_exit_code_is_zero(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        stale_pinned_path: Path,
    ) -> None:
        step = _make_step("build-derived-daily-bar", target="daily_bar")
        proc = _FakePopen(exit_code_after_interrupt=0)

        # Cleanup succeeds → stall verdict is preserved as 126.
        monkeypatch.setattr(run_update_daily, "_terminate_process_tree", lambda p, log: True)
        monkeypatch.setattr(run_update_daily, "_send_interrupt", lambda p, log: None)

        exit_code = run_update_daily._wait_with_stall_detection(
            proc,
            step,
            io.StringIO(),
            tmp_path,
            progress_path=stale_pinned_path,
            derived_run_id="run_instance:20260725_120000:build-derived-daily-bar",
        )

        assert exit_code == run_update_daily.STALLED_EXIT_CODE
        assert exit_code == 126

    def test_stalled_returns_126_even_if_child_exit_code_is_one(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        stale_pinned_path: Path,
    ) -> None:
        step = _make_step("build-derived-daily-bar", target="daily_bar")
        proc = _FakePopen(exit_code_after_interrupt=1)

        monkeypatch.setattr(run_update_daily, "_terminate_process_tree", lambda p, log: True)
        monkeypatch.setattr(run_update_daily, "_send_interrupt", lambda p, log: None)

        exit_code = run_update_daily._wait_with_stall_detection(
            proc,
            step,
            io.StringIO(),
            tmp_path,
            progress_path=stale_pinned_path,
            derived_run_id="run_instance:20260725_120000:build-derived-daily-bar",
        )

        assert exit_code == run_update_daily.STALLED_EXIT_CODE
        assert exit_code == 126

    def test_stalled_returns_126_when_child_force_killed(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        stale_pinned_path: Path,
    ) -> None:
        """Child that ignores SIGINT and is force-killed still yields 126."""

        step = _make_step("build-derived-daily-bar", target="daily_bar")
        # wait(timeout=60) raises TimeoutExpired → child ignored graceful
        # shutdown; the orchestrator force-terminates and (cleanup success)
        # preserves the 126 verdict.
        proc = _FakePopen(exit_code_after_interrupt=-9, wait_raises_timeout=True)

        monkeypatch.setattr(run_update_daily, "_terminate_process_tree", lambda p, log: True)
        monkeypatch.setattr(run_update_daily, "_send_interrupt", lambda p, log: None)

        exit_code = run_update_daily._wait_with_stall_detection(
            proc,
            step,
            io.StringIO(),
            tmp_path,
            progress_path=stale_pinned_path,
            derived_run_id="run_instance:20260725_120000:build-derived-daily-bar",
        )

        assert exit_code == run_update_daily.STALLED_EXIT_CODE
        assert exit_code == 126

    def test_stalled_with_cleanup_failure_returns_125(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        stale_pinned_path: Path,
    ) -> None:
        """When process-tree cleanup fails, escalate to 125."""

        step = _make_step("build-derived-daily-bar", target="daily_bar")
        proc = _FakePopen(exit_code_after_interrupt=0)

        monkeypatch.setattr(run_update_daily, "_terminate_process_tree", lambda p, log: False)
        monkeypatch.setattr(run_update_daily, "_send_interrupt", lambda p, log: None)

        exit_code = run_update_daily._wait_with_stall_detection(
            proc,
            step,
            io.StringIO(),
            tmp_path,
            progress_path=stale_pinned_path,
            derived_run_id="run_instance:20260725_120000:build-derived-daily-bar",
        )

        assert exit_code == run_update_daily.TIMEOUT_CLEANUP_FAILED_EXIT_CODE
        assert exit_code == 125

    def test_stalled_with_cleanup_failure_returns_125_even_if_child_exit_zero(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        stale_pinned_path: Path,
    ) -> None:
        """Cleanup failure (125) takes precedence over child exit code 0."""

        step = _make_step("build-derived-daily-bar", target="daily_bar")
        proc = _FakePopen(exit_code_after_interrupt=0)

        monkeypatch.setattr(run_update_daily, "_terminate_process_tree", lambda p, log: False)
        monkeypatch.setattr(run_update_daily, "_send_interrupt", lambda p, log: None)

        exit_code = run_update_daily._wait_with_stall_detection(
            proc,
            step,
            io.StringIO(),
            tmp_path,
            progress_path=stale_pinned_path,
            derived_run_id="run_instance:20260725_120000:build-derived-daily-bar",
        )

        assert exit_code == 125

    def test_old_progress_file_does_not_cause_false_stall(
        self,
        tmp_path: Path,
    ) -> None:
        """A stale file at a different path must not stall the current step.

        The orchestrator pins a unique path per (run_instance, step_id) and
        reads only that path. A stale file at a sibling path (e.g., from a
        previous run) must not cause the current step to be declared stalled.
        """

        current_run_instance = "run_instance:20260725_120000"
        old_run_instance = "run_instance:20260724_120000"
        step_id = "build-derived-daily-bar"

        # Stale file from a previous run (sibling run_instance directory).
        old_pinned_path = run_update_daily._derived_progress_path_for_step(tmp_path, old_run_instance, step_id)
        _write_progress_state(old_pinned_path, heartbeat_age_seconds=3600)

        # Current run's pinned path does NOT exist (build hasn't started yet).
        # check_stall returns stalled=False ("state file not found") for it.
        current_pinned_path = run_update_daily._derived_progress_path_for_step(tmp_path, current_run_instance, step_id)
        assert not current_pinned_path.exists()

        step = _make_step(step_id, target="daily_bar")
        proc = _FakePopen(exit_code_after_interrupt=0)

        exit_code = run_update_daily._wait_with_stall_detection(
            proc,
            step,
            io.StringIO(),
            tmp_path,
            progress_path=current_pinned_path,
            derived_run_id=f"{current_run_instance}:{step_id}",
        )

        # Pinned path doesn't exist → not stalled → child exit code 0.
        assert exit_code == 0
        assert proc.send_signal_calls == []

    def test_stalled_exit_codes_are_distinct_from_timeout(self) -> None:
        """Sanity: 124/125/126 must be three distinct codes."""

        assert run_update_daily.TIMEOUT_EXIT_CODE == 124
        assert run_update_daily.TIMEOUT_CLEANUP_FAILED_EXIT_CODE == 125
        assert run_update_daily.STALLED_EXIT_CODE == 126
        assert (
            len(
                {
                    run_update_daily.TIMEOUT_EXIT_CODE,
                    run_update_daily.TIMEOUT_CLEANUP_FAILED_EXIT_CODE,
                    run_update_daily.STALLED_EXIT_CODE,
                }
            )
            == 3
        )

    def test_no_pinned_path_skips_stall_detection(self, tmp_path: Path) -> None:
        """Defensive: ``progress_path=None`` means stall detection is skipped.

        The orchestrator refuses to invent a path; it just waits for the child.
        This guards against a regression where a missing pinned path causes a
        directory scan that could mask a real stall or stall a healthy build.
        """

        step = _make_step("build-derived-daily-bar", target="daily_bar")
        proc = _FakePopen(exit_code_after_interrupt=42)

        exit_code = run_update_daily._wait_with_stall_detection(
            proc,
            step,
            io.StringIO(),
            tmp_path,
            progress_path=None,
            derived_run_id=None,
        )

        # proc.wait() is called immediately (no stall check).
        assert exit_code == 42
        assert proc.send_signal_calls == []
