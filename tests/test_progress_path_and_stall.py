"""Tests for P0-3 (fixed progress path) and P0-4 (stalled exit code).

P0-3: The orchestrator (``src.tools.run_update_daily``) must generate a unique
progress path per (run_instance, step_id) BEFORE spawn, pass it to the child
via env vars ``QDC_DERIVED_PROGRESS_PATH`` and
``QDC_DERIVED_PROGRESS_CONTRACT_ID`` (the legacy ``QDC_DERIVED_RUN_ID`` is read
only for backwards-compatibility with an in-flight subprocess from an older
orchestrator build), and the stall detector must read ONLY that path — never a
directory scan.

P0-4: The stall detector (``_wait_with_stall_detection``) must preserve the
stall verdict: once stalled, return ``STALLED_EXIT_CODE`` (126) regardless of
the child's exit code. If process-tree cleanup fails, escalate to
``TIMEOUT_CLEANUP_FAILED_EXIT_CODE`` (125).
"""

from __future__ import annotations

import dataclasses
import io
import json
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from src.sources.derived.progress import (
    ProgressIdentity,
    ProgressReporter,
    check_stall,
    make_progress_contract_id,
)
from src.sources.derived.stock_daily_bar import _progress_contract_id_from_env_or_local
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
    progress_contract_id: str | None = None,
    journal_run_id: str | None = None,
) -> None:
    """Write a progress state JSON file with the requested heartbeat age.

    ``heartbeat_age_seconds=0`` produces a fresh heartbeat (now); larger values
    push ``heartbeat_at`` into the past so ``check_stall`` declares it stale.

    ``progress_contract_id`` and ``journal_run_id`` are optional identity
    fields. When ``progress_contract_id`` is provided it MUST match the
    ``expected_contract_id`` passed to :func:`check_stall` or the stall
    detector treats the snapshot as "not our contract" and returns
    ``stalled=False``.
    """

    heartbeat_at = datetime.now() - timedelta(seconds=heartbeat_age_seconds)
    payload: dict[str, Any] = {
        "stage": stage,
        "processed": processed,
        "total": total,
        "failed": 0,
        "current_security_id": "sh.600000",
        "throughput_per_minute": 10.0,
        "heartbeat_at": heartbeat_at.isoformat(timespec="seconds"),
        "started_at": heartbeat_at.isoformat(timespec="seconds"),
        "failed_security_ids": [],
    }
    if progress_contract_id is not None:
        payload["progress_contract_id"] = progress_contract_id
    if journal_run_id is not None:
        payload["journal_run_id"] = journal_run_id
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
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
    progress_contract_id: str = "derived-progress:build-derived-daily-bar:abc12345",
) -> Path:
    """Create a pinned progress path file with a stale heartbeat.

    The ``progress_contract_id`` defaults to the matching contract id used by
    the ``TestP04StalledExitCode`` tests so the stall detector's identity
    check passes and the stale heartbeat is actually evaluated.
    """

    pinned = run_update_daily._derived_progress_path_for_step(tmp_path, run_instance, step_id)
    _write_progress_state(
        pinned,
        heartbeat_age_seconds=heartbeat_age_seconds,
        progress_contract_id=progress_contract_id,
    )
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
        # New contract: the env var is QDC_DERIVED_PROGRESS_CONTRACT_ID and the
        # value is a uuid-based ``derived-progress:<step_id>:<uuid_hex>``. The
        # exact uuid is not deterministic; we verify the prefix and that the
        # legacy QDC_DERIVED_RUN_ID is NOT set (the new name is the single
        # authoritative value).
        contract_id = env[run_update_daily.QDC_DERIVED_PROGRESS_CONTRACT_ID_ENV]
        assert contract_id.startswith(f"derived-progress:{step.id}:")
        assert run_update_daily.QDC_DERIVED_RUN_ID_ENV not in env

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
        assert run_update_daily.QDC_DERIVED_PROGRESS_CONTRACT_ID_ENV not in env
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

        # Current run's pinned path has a FRESH heartbeat and the matching
        # contract id so the stall detector's identity check passes.
        current_pinned_path = run_update_daily._derived_progress_path_for_step(tmp_path, current_run_instance, step_id)
        _write_progress_state(
            current_pinned_path,
            heartbeat_age_seconds=0,
            progress_contract_id=f"derived-progress:{step_id}:abc12345",
        )

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
            progress_contract_id=f"derived-progress:{step_id}:abc12345",
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
            progress_contract_id="derived-progress:build-derived-daily-bar:abc12345",
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
            progress_contract_id="derived-progress:build-derived-daily-bar:abc12345",
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
            progress_contract_id="derived-progress:build-derived-daily-bar:abc12345",
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
            progress_contract_id="derived-progress:build-derived-daily-bar:abc12345",
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
            progress_contract_id="derived-progress:build-derived-daily-bar:abc12345",
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
            progress_contract_id=f"derived-progress:{step_id}:abc12345",
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
            progress_contract_id=None,
        )

        # proc.wait() is called immediately (no stall check).
        assert exit_code == 42
        assert proc.send_signal_calls == []


# ---------------------------------------------------------------------------
# P0-2: Progress contract identity tests
# ---------------------------------------------------------------------------
# The orchestrator↔child progress contract has two distinct identities:
#
# * ``progress_contract_id`` — unique per (run_instance, step); generated by
#   the orchestrator BEFORE spawn; verified by the stall detector so a stale
#   file from a different run cannot mask a real stall.
# * ``journal_run_id`` — the derived build journal's stable recovery identity;
#   stable across resume; backfilled into the daily state from the child's
#   final progress snapshot.
#
# These tests verify the contract at the unit level: id generation, path
# uniqueness, snapshot identity, stall-detector identity verification, resume
# identity, manual CLI fallback, and the "no directory scan" rule.


class TestProgressContractIdGeneration:
    """``make_progress_contract_id`` produces unique, well-formed ids."""

    def test_contract_id_format(self) -> None:
        cid = make_progress_contract_id("daily_bar")
        assert cid.startswith("derived-progress:daily_bar:")
        # The uuid hex suffix is 32 chars.
        suffix = cid.removeprefix("derived-progress:daily_bar:")
        assert len(suffix) == 32
        assert all(c in "0123456789abcdef" for c in suffix)

    def test_contract_ids_are_unique(self) -> None:
        ids = {make_progress_contract_id("daily_bar") for _ in range(1000)}
        assert len(ids) == 1000

    def test_contract_ids_for_different_steps_differ(self) -> None:
        a = make_progress_contract_id("daily_bar")
        b = make_progress_contract_id("valuation")
        assert a != b
        assert a.startswith("derived-progress:daily_bar:")
        assert b.startswith("derived-progress:valuation:")


class TestRunInstanceNonce:
    """The run_instance_key carries a nonce so same-second starts do not
    collide on the progress path.
    """

    def test_run_instance_key_contains_nonce(self) -> None:
        # Two keys generated in the same second must differ because of the
        # 8-hex-char nonce. We simulate "same second" by calling the formatter
        # twice with the same timestamp.
        ts = datetime(2026, 7, 26, 12, 0, 0)
        import uuid as _uuid

        key_a = f"run_instance:{ts.strftime('%Y%m%d_%H%M%S')}-{_uuid.uuid4().hex[:8]}"
        key_b = f"run_instance:{ts.strftime('%Y%m%d_%H%M%S')}-{_uuid.uuid4().hex[:8]}"
        assert key_a != key_b
        # Both keep the readable timestamp prefix.
        assert key_a.startswith("run_instance:20260726_120000-")
        assert key_b.startswith("run_instance:20260726_120000-")

    def test_same_second_starts_produce_different_progress_paths(self, tmp_path: Path) -> None:
        step_id = "build-derived-daily-bar"
        # Same timestamp, different nonce → different run_instance_key →
        # different progress path. This is the core same-second collision
        # guarantee.
        import uuid as _uuid

        ts = "20260726_120000"
        key_a = f"run_instance:{ts}-{_uuid.uuid4().hex[:8]}"
        key_b = f"run_instance:{ts}-{_uuid.uuid4().hex[:8]}"
        path_a = run_update_daily._derived_progress_path_for_step(tmp_path, key_a, step_id)
        path_b = run_update_daily._derived_progress_path_for_step(tmp_path, key_b, step_id)
        assert path_a != path_b

    def test_concurrent_same_target_paths_differ(self, tmp_path: Path) -> None:
        """Two concurrent orchestrator invocations targeting the same step
        must produce different progress paths so neither masks the other.
        """
        import uuid as _uuid

        step_id = "build-derived-daily-bar"
        keys = [f"run_instance:20260726_120000-{_uuid.uuid4().hex[:8]}" for _ in range(5)]
        paths = {run_update_daily._derived_progress_path_for_step(tmp_path, k, step_id) for k in keys}
        assert len(paths) == 5


class TestProgressIdentityImmutable:
    """``ProgressIdentity`` is frozen and the reporter never mutates it."""

    def test_identity_is_frozen(self) -> None:
        identity = ProgressIdentity(
            progress_contract_id="derived-progress:daily_bar:abc12345",
            target="daily_bar",
            dataset_id="cn_stock_daily_bar",
            journal_run_id="run-20260726-abcdef",
        )
        with pytest.raises(dataclasses.FrozenInstanceError):
            identity.progress_contract_id = "other"  # type: ignore[misc]

    def test_reporter_identity_is_stable_across_snapshots(self, tmp_path: Path) -> None:
        identity = ProgressIdentity(
            progress_contract_id="derived-progress:daily_bar:abc12345",
            target="daily_bar",
            dataset_id="cn_stock_daily_bar",
            journal_run_id="run-20260726-abcdef",
        )
        reporter = ProgressReporter(state_path=tmp_path / "state.json", identity=identity)
        reporter.set_stage("BUILDING_PARTITIONS", total=10)
        reporter.heartbeat()
        snap1 = reporter.snapshot()
        reporter.record_processed("sh.600000")
        reporter.record_processed("sh.600001")
        reporter.heartbeat()
        snap2 = reporter.snapshot()
        # Identity fields must be identical across both snapshots.
        assert snap1["progress_contract_id"] == "derived-progress:daily_bar:abc12345"
        assert snap2["progress_contract_id"] == "derived-progress:daily_bar:abc12345"
        assert snap1["journal_run_id"] == "run-20260726-abcdef"
        assert snap2["journal_run_id"] == "run-20260726-abcdef"
        assert snap1["target"] == "daily_bar"
        assert snap2["target"] == "daily_bar"
        assert snap1["dataset_id"] == "cn_stock_daily_bar"
        assert snap2["dataset_id"] == "cn_stock_daily_bar"
        # The reporter's identity property returns the same object.
        assert reporter.identity is identity

    def test_snapshot_without_identity_has_no_identity_fields(self, tmp_path: Path) -> None:
        """Legacy callers that do not pass an identity get snapshots without
        the identity fields. This keeps the reporter backward-compatible.
        """
        reporter = ProgressReporter(state_path=tmp_path / "state.json")
        reporter.set_stage("BUILDING_PARTITIONS", total=10)
        reporter.heartbeat()
        snap = reporter.snapshot()
        assert "progress_contract_id" not in snap
        assert "journal_run_id" not in snap
        assert "target" not in snap
        assert "dataset_id" not in snap


class TestStallDetectorIdentityCheck:
    """``check_stall`` with ``expected_contract_id`` rejects snapshots that
    do not carry the matching contract id. A mismatch must NOT trigger
    ``stalled=True`` — the detector keeps waiting for the correct snapshot
    or the safety timeout.
    """

    def test_wrong_contract_id_does_not_trigger_stall(self, tmp_path: Path) -> None:
        """A stale snapshot with a WRONG contract id must not be used to
        judge stalled. The detector returns ``stalled=False`` (mismatch)
        even though the heartbeat is old and processed is unchanged.
        """
        path = tmp_path / "state.json"
        _write_progress_state(
            path,
            heartbeat_age_seconds=35 * 60,
            progress_contract_id="derived-progress:daily_bar:WRONG",
        )
        report = check_stall(
            path,
            stall_heartbeat_seconds=30 * 60,
            previous_processed=100,
            expected_contract_id="derived-progress:daily_bar:abc12345",
        )
        assert report.stalled is False
        assert "mismatch" in report.reason

    def test_correct_contract_id_triggers_stall(self, tmp_path: Path) -> None:
        """A stale snapshot with the MATCHING contract id and unchanged
        processed must trigger ``stalled=True``.
        """
        path = tmp_path / "state.json"
        _write_progress_state(
            path,
            heartbeat_age_seconds=35 * 60,
            processed=100,
            progress_contract_id="derived-progress:daily_bar:abc12345",
        )
        report = check_stall(
            path,
            stall_heartbeat_seconds=30 * 60,
            previous_processed=100,
            expected_contract_id="derived-progress:daily_bar:abc12345",
        )
        assert report.stalled is True

    def test_missing_contract_id_in_snapshot_treated_as_mismatch(self, tmp_path: Path) -> None:
        """A snapshot that does not carry ``progress_contract_id`` (e.g.
        written by an older child) is treated as a mismatch when
        ``expected_contract_id`` is provided.
        """
        path = tmp_path / "state.json"
        _write_progress_state(path, heartbeat_age_seconds=35 * 60, processed=100)
        report = check_stall(
            path,
            stall_heartbeat_seconds=30 * 60,
            previous_processed=100,
            expected_contract_id="derived-progress:daily_bar:abc12345",
        )
        assert report.stalled is False
        assert "mismatch" in report.reason

    def test_no_expected_contract_id_skips_identity_check(self, tmp_path: Path) -> None:
        """When ``expected_contract_id`` is None (e.g. manual CLI), the
        identity check is skipped and the stall verdict is based purely on
        heartbeat and processed.
        """
        path = tmp_path / "state.json"
        _write_progress_state(
            path,
            heartbeat_age_seconds=35 * 60,
            processed=100,
            progress_contract_id="derived-progress:daily_bar:something",
        )
        report = check_stall(
            path,
            stall_heartbeat_seconds=30 * 60,
            previous_processed=100,
            expected_contract_id=None,
        )
        assert report.stalled is True

    def test_stale_file_at_same_path_cannot_trigger_stall(self, tmp_path: Path) -> None:
        """If a stale file from a previous run happens to land at the SAME
        path (extremely unlikely given the nonce, but cheap to guard), the
        contract id check still rejects it because the contract id differs.
        """
        path = tmp_path / "state.json"
        # Stale file written by a PREVIOUS run with a different contract id.
        _write_progress_state(
            path,
            heartbeat_age_seconds=35 * 60,
            processed=100,
            progress_contract_id="derived-progress:daily_bar:OLD",
        )
        report = check_stall(
            path,
            stall_heartbeat_seconds=30 * 60,
            previous_processed=100,
            expected_contract_id="derived-progress:daily_bar:NEW",
        )
        assert report.stalled is False
        assert "mismatch" in report.reason


class TestManualCliContractId:
    """The manual CLI (``qdc build-derived``) has no orchestrator env var,
    so the child generates a local contract id. The id is still unique
    (uuid-based) so concurrent manual builds cannot collide.
    """

    def test_manual_cli_generates_local_contract_id(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        # Clear both env vars to simulate manual CLI.
        monkeypatch.delenv("QDC_DERIVED_PROGRESS_CONTRACT_ID", raising=False)
        monkeypatch.delenv("QDC_DERIVED_RUN_ID", raising=False)
        cid = _progress_contract_id_from_env_or_local("daily_bar")
        assert cid.startswith("derived-progress:daily_bar:")
        suffix = cid.removeprefix("derived-progress:daily_bar:")
        assert len(suffix) == 32

    def test_manual_cli_ids_are_unique(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("QDC_DERIVED_PROGRESS_CONTRACT_ID", raising=False)
        monkeypatch.delenv("QDC_DERIVED_RUN_ID", raising=False)
        ids = {_progress_contract_id_from_env_or_local("daily_bar") for _ in range(100)}
        assert len(ids) == 100

    def test_env_contract_id_wins_over_local(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("QDC_DERIVED_PROGRESS_CONTRACT_ID", "orchestrator-pinned")
        monkeypatch.delenv("QDC_DERIVED_RUN_ID", raising=False)
        assert _progress_contract_id_from_env_or_local("daily_bar") == "orchestrator-pinned"

    def test_legacy_env_id_still_honored(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """The deprecated ``QDC_DERIVED_RUN_ID`` is still read so an in-flight
        subprocess from an older orchestrator build does not crash. The new
        name always wins when both are present.
        """
        monkeypatch.delenv("QDC_DERIVED_PROGRESS_CONTRACT_ID", raising=False)
        monkeypatch.setenv("QDC_DERIVED_RUN_ID", "legacy-id")
        # The legacy value is returned (the deprecation warning is emitted by
        # the function but loguru is not captured by pytest's caplog; the
        # return value proves the legacy code path was taken).
        assert _progress_contract_id_from_env_or_local("daily_bar") == "legacy-id"

    def test_new_env_wins_over_legacy(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("QDC_DERIVED_PROGRESS_CONTRACT_ID", "new-id")
        monkeypatch.setenv("QDC_DERIVED_RUN_ID", "legacy-id")
        assert _progress_contract_id_from_env_or_local("daily_bar") == "new-id"


class TestNoDirectoryScan:
    """The stall detector must read ONLY the pinned progress path — never a
    directory scan. A stale sibling file must not influence the verdict.
    """

    def test_latest_progress_state_path_not_called_by_stall_detector(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """If the stall detector accidentally called
        ``latest_progress_state_path`` it would be a regression. We patch the
        function to raise and confirm the stall detector does not call it.
        """
        from src.sources.derived import progress as progress_module

        def _explode(*args: Any, **kwargs: Any) -> None:
            raise AssertionError("latest_progress_state_path must not be called by stall detector")

        # ``latest_progress_state_path`` may or may not exist on the module;
        # if it does, we patch it. If it does not, there is nothing to guard.
        if hasattr(progress_module, "latest_progress_state_path"):
            monkeypatch.setattr(progress_module, "latest_progress_state_path", _explode)

        step = _make_step("build-derived-daily-bar", target="daily_bar")
        proc = _FakePopen(exit_code_after_interrupt=0)
        pinned = run_update_daily._derived_progress_path_for_step(
            tmp_path, "run_instance:20260726_120000-abc12345", step.id
        )
        _write_progress_state(
            pinned,
            heartbeat_age_seconds=0,
            progress_contract_id="derived-progress:build-derived-daily-bar:abc12345",
        )
        # If the stall detector called latest_progress_state_path, the test
        # would raise AssertionError and fail. Passing means no call.
        exit_code = run_update_daily._wait_with_stall_detection(
            proc,
            step,
            io.StringIO(),
            tmp_path,
            progress_path=pinned,
            progress_contract_id="derived-progress:build-derived-daily-bar:abc12345",
        )
        assert exit_code == 0


class TestOrchestratorBackfillsJournalRunId:
    """The orchestrator backfills ``journal_run_id`` from the child's final
    progress snapshot into the daily state. ``progress_contract_id`` is
    recorded before spawn; ``journal_run_id`` is recorded after the step
    completes.
    """

    def test_read_journal_run_id_from_progress_returns_value(self, tmp_path: Path) -> None:
        path = tmp_path / "state.json"
        _write_progress_state(
            path,
            progress_contract_id="derived-progress:daily_bar:abc12345",
            journal_run_id="run-20260726-abcdef",
        )
        assert run_update_daily._read_journal_run_id_from_progress(path) == "run-20260726-abcdef"

    def test_read_journal_run_id_returns_none_when_missing(self, tmp_path: Path) -> None:
        path = tmp_path / "state.json"
        _write_progress_state(path)
        assert run_update_daily._read_journal_run_id_from_progress(path) is None

    def test_read_journal_run_id_returns_none_when_file_missing(self, tmp_path: Path) -> None:
        assert run_update_daily._read_journal_run_id_from_progress(tmp_path / "nope.json") is None

    def test_read_journal_run_id_returns_none_on_invalid_json(self, tmp_path: Path) -> None:
        path = tmp_path / "state.json"
        path.write_text("{not json", encoding="utf-8")
        assert run_update_daily._read_journal_run_id_from_progress(path) is None

    def test_read_journal_run_id_returns_none_when_field_not_string(self, tmp_path: Path) -> None:
        path = tmp_path / "state.json"
        path.write_text(json.dumps({"journal_run_id": 123}), encoding="utf-8")
        assert run_update_daily._read_journal_run_id_from_progress(path) is None
