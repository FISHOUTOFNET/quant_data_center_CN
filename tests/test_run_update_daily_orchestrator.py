from __future__ import annotations

import json
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from src.tools import run_update_daily
from src.utils.process_lock import acquire_process_lock


def _state(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_orchestrator_resumes_after_successful_steps(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    def runner(step: run_update_daily.DailyStep, log_path: Path) -> int:
        del log_path
        calls.append(step.id)
        return 0

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 5),
            now=lambda: datetime(2026, 6, 5, 18, 0),
            command_runner=runner,
        )
        == 0
    )
    first_run = list(calls)
    assert first_run[0] == "cleanup"
    assert "baostock-qfq" in first_run

    calls.clear()
    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 5),
            now=lambda: datetime(2026, 6, 5, 19, 0),
            command_runner=runner,
        )
        == 0
    )

    assert calls == []
    states = _state(state_file)["runs"]["2026-06-05"]["steps"]
    assert states["calendar"]["status"] == "success"
    assert states["baostock-qfq"]["status"] == "success"


def test_orchestrator_continues_independent_steps_and_blocks_dependents(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    independent = run_update_daily.DailyStep("independent", "independent", (sys.executable, "-c", "print('ok')"))
    failing = run_update_daily.DailyStep("source", "source", (sys.executable, "-c", "raise SystemExit(7)"))
    dependent = run_update_daily.DailyStep(
        "dependent",
        "dependent",
        (sys.executable, "-c", "print('blocked')"),
        depends_on=("source",),
    )

    def failing_runner(step: run_update_daily.DailyStep, log_path: Path) -> int:
        del log_path
        calls.append(step.id)
        return 7 if step.id == "source" else 0

    original_steps = run_update_daily.daily_steps
    run_update_daily.daily_steps = lambda today=None: [independent, failing, dependent]
    try:
        result = run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 5),
            now=lambda: datetime(2026, 6, 5, 18, 0),
            command_runner=failing_runner,
        )
    finally:
        run_update_daily.daily_steps = original_steps

    assert result == 7
    assert calls == ["independent", "source"]
    states = _state(state_file)["runs"]["2026-06-05"]["steps"]
    assert states["independent"]["status"] == "success"
    assert states["source"]["status"] == "failed"
    assert states["dependent"]["status"] == "blocked"
    assert states["dependent"]["blocked_by"] == ["source"]


def test_orchestrator_retries_failed_and_unblocks_dependents(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []
    steps = [
        run_update_daily.DailyStep("source", "source", (sys.executable, "-c", "print('source')")),
        run_update_daily.DailyStep(
            "dependent",
            "dependent",
            (sys.executable, "-c", "print('dependent')"),
            depends_on=("source",),
        ),
    ]

    monkeypatch.setattr(run_update_daily, "daily_steps", lambda today=None: steps)
    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 5),
            now=lambda: datetime(2026, 6, 5, 18, 0),
            command_runner=lambda step, log_path: calls.append(step.id) or (7 if step.id == "source" else 0),
        )
        == 7
    )
    assert calls == ["source"]
    states = _state(state_file)["runs"]["2026-06-05"]["steps"]
    assert states["source"]["status"] == "failed"
    assert states["dependent"]["status"] == "blocked"

    calls.clear()
    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 5),
            now=lambda: datetime(2026, 6, 5, 19, 0),
            command_runner=lambda step, log_path: calls.append(step.id) or 0,
        )
        == 0
    )
    assert calls == ["source", "dependent"]
    states = _state(state_file)["runs"]["2026-06-05"]["steps"]
    assert states["source"]["status"] == "success"
    assert states["dependent"]["status"] == "success"


def test_orchestrator_force_and_start_at_control_resume(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    state_file.write_text(
        json.dumps(
            {
                "runs": {
                    "2026-06-05": {
                        "steps": {
                            "cleanup": {"status": "success"},
                            "calendar": {"status": "success"},
                            "baostock-qfq": {"status": "success"},
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 5),
            force=True,
            command_runner=lambda step, log_path: calls.append(step.id) or 0,
        )
        == 0
    )
    assert calls[0] == "cleanup"

    calls.clear()
    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 5),
            start_at="baostock-qfq",
            command_runner=lambda step, log_path: calls.append(step.id) or 0,
        )
        == 0
    )
    assert calls[0] == "baostock-qfq"
    assert "calendar" not in calls


def test_orchestrator_weekend_window_filters_heavy_steps(tmp_path: Path) -> None:
    friday = [step.id for step in run_update_daily.daily_steps(date(2026, 6, 5))]
    monday = [step.id for step in run_update_daily.daily_steps(date(2026, 6, 8))]

    assert "baostock-qfq" in friday
    assert "akshare-valuation-full" in friday
    assert "akshare-yjyg-em" in friday
    assert "baostock-qfq" not in monday
    assert "akshare-valuation-full" not in monday
    assert "akshare-yjyg-em" in monday
    assert "financial-report" in monday


def test_daily_steps_include_yjyg_em_before_build_views_on_weekday() -> None:
    steps = run_update_daily.daily_steps(date(2026, 6, 8))
    by_id = {step.id: step for step in steps}

    assert by_id["akshare-spot-quote"].optional is True
    assert by_id["akshare-spot-quote"].timeout_seconds is None
    assert "akshare-yjyg-em" in by_id
    assert steps.index(by_id["akshare-yjyg-em"]) < steps.index(by_id["build-duckdb-views"])
    assert by_id["akshare-yjyg-em"].command[1:] == (
        "-m",
        "src.cli",
        "akshare",
        "update",
        "--target",
        "yjyg_em",
        "--mode",
        "incremental",
        "--no-build-duckdb-views",
    )
    assert by_id["akshare-yjyg-em"].optional is True
    assert by_id["akshare-yjyg-em"].timeout_seconds == 900


@pytest.mark.parametrize("today", [date(2026, 6, 8), date(2026, 6, 6)])
def test_daily_steps_build_derived_all_before_views(today: date) -> None:
    steps = run_update_daily.daily_steps(today)
    by_id = {step.id: step for step in steps}

    assert "build-derived" in by_id
    assert "build-security-master" not in by_id
    assert steps.index(by_id["financial-report"]) < steps.index(by_id["build-derived"])
    assert steps.index(by_id["build-derived"]) < steps.index(by_id["build-duckdb-views"])
    assert by_id["build-derived"].depends_on == (
        "akshare-spot-quote",
        "baostock-unadjusted",
        "baostock-basic",
        "baostock-valuation-percentile",
        "financial-report",
        *(
            (
                "akshare-delist",
                "baostock-adjustment-factor",
                "baostock-qfq",
                "baostock-hfq",
                "akshare-valuation-full",
                "akshare-daily-bar",
                "sync-qlib",
            )
            if today.weekday() in {4, 5, 6}
            else ()
        ),
    )
    assert by_id["build-duckdb-views"].depends_on == ("build-derived",)
    assert by_id["build-derived"].command[1:] == (
        "-m",
        "src.cli",
        "build-derived",
        "--target",
        "all",
        "--no-build-duckdb-views",
    )


def test_run_daily_update_records_yjyg_em_step_on_weekday(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 8),
            command_runner=lambda step, log_path: calls.append(step.id) or 0,
        )
        == 0
    )

    assert "akshare-yjyg-em" in calls
    states = _state(state_file)["runs"]["2026-06-08"]["steps"]
    assert states["akshare-yjyg-em"]["status"] == "success"


@pytest.mark.parametrize("failed_step", ["baostock-unadjusted", "baostock-valuation-percentile"])
def test_core_baostock_failure_blocks_build_derived(tmp_path: Path, failed_step: str) -> None:
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 8),
            command_runner=lambda step, log_path: calls.append(step.id) or (7 if step.id == failed_step else 0),
        )
        == 7
    )

    assert "build-derived" not in calls
    states = _state(state_file)["runs"]["2026-06-08"]["steps"]
    assert states["build-derived"]["status"] == "blocked"
    assert failed_step in states["build-derived"]["blocked_by"]


def test_weekend_daily_bar_failure_blocks_build_derived(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 6),
            command_runner=lambda step, log_path: calls.append(step.id) or (7 if step.id == "akshare-daily-bar" else 0),
        )
        == 7
    )

    assert "build-derived" not in calls
    states = _state(state_file)["runs"]["2026-06-06"]["steps"]
    assert states["build-derived"]["status"] == "blocked"
    assert states["build-derived"]["blocked_by"] == ["akshare-daily-bar"]


def test_optional_plain_skipped_does_not_block_build_derived(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 8),
            command_runner=lambda step, log_path: calls.append(step.id) or (7 if step.id == "akshare-spot-quote" else 0),
        )
        == 0
    )

    assert "build-derived" in calls
    states = _state(state_file)["runs"]["2026-06-08"]["steps"]
    assert states["akshare-spot-quote"]["status"] == "skipped"
    assert states["build-derived"]["status"] == "success"


@pytest.mark.parametrize("status", ["failed_resource_locked", "failed_timeout_cleanup"])
def test_failed_optional_hard_status_blocks_followup(status: str) -> None:
    step = run_update_daily.DailyStep("derived", "derived", ("cmd",), depends_on=("optional-source",))
    step_state = {"optional-source": {"status": status}}

    assert run_update_daily._blocked_dependencies(step, step_state) == ("optional-source",)


def test_weekend_akshare_valuation_failure_blocks_build_derived(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 6),
            now=lambda: datetime(2026, 6, 6, 1, 0),
            command_runner=lambda step, log_path: (
                calls.append(step.id) or (7 if step.id == "akshare-valuation-full" else 0)
            ),
        )
        == 7
    )

    assert "akshare-valuation-full" in calls
    assert "akshare-report-disclosure" in calls
    assert "akshare-yysj-em" in calls
    assert "akshare-yjyg-em" in calls
    assert "akshare-daily-bar" in calls
    assert "sync-qlib" in calls
    assert "financial-report" in calls
    assert "build-derived" not in calls
    assert "build-duckdb-views" not in calls
    states = _state(state_file)["runs"]["2026-06-06"]["steps"]
    assert states["akshare-valuation-full"]["status"] == "failed"
    assert states["akshare-report-disclosure"]["status"] == "success"
    assert states["akshare-yysj-em"]["status"] == "success"
    assert states["akshare-yjyg-em"]["status"] == "success"
    assert states["akshare-daily-bar"]["status"] == "success"
    assert states["sync-qlib"]["status"] == "success"
    assert states["financial-report"]["status"] == "success"
    assert states["build-derived"]["status"] == "blocked"
    assert states["build-derived"]["blocked_by"] == ["akshare-valuation-full"]
    assert states["build-duckdb-views"]["status"] == "blocked"


def test_run_subprocess_disables_child_file_logging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    log_file = tmp_path / "run.log"
    step = run_update_daily.DailyStep("sample", "sample step", (sys.executable, "-c", "print('sample')"))

    class FakePopen:
        pid = 123

        def __init__(self, *args, **kwargs):
            captured["args"] = args
            captured["kwargs"] = kwargs
            kwargs["stdout"].write("captured child output\n")

        def wait(self, timeout=None):
            captured["wait_timeout"] = timeout
            return 0

        def poll(self):
            return 0

    monkeypatch.setattr(run_update_daily.subprocess, "Popen", FakePopen)

    assert run_update_daily._run_subprocess(step, log_file, tmp_path) == 0

    env = captured["kwargs"]["env"]
    assert env["QDC_DISABLE_FILE_LOG"] == "1"
    assert captured["kwargs"]["stderr"] == subprocess.STDOUT
    assert log_file.read_text(encoding="utf-8") == "captured child output\n"


def test_orchestrator_rejects_corrupt_state_file(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    state_file.write_text("{bad json", encoding="utf-8")

    with pytest.raises(run_update_daily.StateFileError, match="--force"):
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            today=date(2026, 6, 5),
            command_runner=lambda step, log_path: 0,
        )


def test_run_daily_update_rejects_active_global_lock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lock_dir = tmp_path / "data" / "metadata" / "locks" / "run-update-daily.lock"
    monkeypatch.setattr(
        run_update_daily,
        "daily_steps",
        lambda today=None: [run_update_daily.DailyStep("one", "one", ("cmd",))],
    )

    with acquire_process_lock(
        lock_dir,
        lock_name="run-update-daily",
        purpose="outer",
        stale_after_seconds=60,
    ):
        with pytest.raises(run_update_daily.RunDailyUpdateLockError) as exc_info:
            run_update_daily.run_daily_update(
                root=tmp_path,
                state_file=tmp_path / "state.json",
                today=date(2026, 6, 8),
                command_runner=lambda step, log_path: 0,
            )
        assert str(lock_dir) in str(exc_info.value)


def test_run_daily_update_recovers_stale_global_lock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lock_dir = tmp_path / "data" / "metadata" / "locks" / "run-update-daily.lock"
    lock_dir.mkdir(parents=True)
    (lock_dir / "owner.json").write_text("{bad", encoding="utf-8")
    monkeypatch.setattr(
        run_update_daily,
        "daily_steps",
        lambda today=None: [run_update_daily.DailyStep("one", "one", ("cmd",))],
    )

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=tmp_path / "state.json",
            today=date(2026, 6, 8),
            command_runner=lambda step, log_path: 0,
        )
        == 0
    )

    assert not lock_dir.exists()


def test_write_state_is_atomic_json_and_cleans_temp_files(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"

    run_update_daily._write_state(state_file, {"runs": {"2026-06-12": {"步骤": "成功"}}})

    assert json.loads(state_file.read_text(encoding="utf-8")) == {"runs": {"2026-06-12": {"步骤": "成功"}}}
    assert list(tmp_path.glob(".state.json.*.tmp")) == []


def test_running_dead_pid_is_marked_abandoned(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    step = run_update_daily.DailyStep("source", "source", ("cmd",))
    step_state = {
        "source": {
            "status": "running",
            "started_at": "2026-06-08T09:00:00",
            "orchestrator_pid": 999999,
        }
    }
    monkeypatch.setattr(run_update_daily, "is_pid_alive", lambda pid: False)

    changed = run_update_daily._mark_abandoned_running_steps(
        [step],
        step_state,
        tmp_path / "run.log",
        lambda: datetime(2026, 6, 8, 10, 0),
        {"pid": 123},
    )

    assert changed is True
    assert step_state["source"]["status"] == "abandoned"
    assert "not alive" in step_state["source"]["reason"]
    assert run_update_daily._final_exit_code([step], step_state, None) == 1


def test_abandoned_dependency_blocks_until_retried_successfully(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    steps = [
        run_update_daily.DailyStep("source", "source", ("cmd",)),
        run_update_daily.DailyStep("dependent", "dependent", ("cmd",), depends_on=("source",)),
    ]
    state_file.write_text(
        json.dumps({"runs": {"2026-06-08": {"steps": {"source": {"status": "abandoned", "exit_code": 1}}}}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(run_update_daily, "daily_steps", lambda today=None: steps)
    calls: list[str] = []

    assert run_update_daily._blocked_dependencies(steps[1], {"source": {"status": "abandoned"}}) == ("source",)
    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 8),
            command_runner=lambda step, log_path: calls.append(step.id) or 0,
        )
        == 0
    )

    assert calls == ["source", "dependent"]
    states = _state(state_file)["runs"]["2026-06-08"]["steps"]
    assert states["source"]["status"] == "success"
    assert states["dependent"]["status"] == "success"


def test_stale_running_record_is_abandoned_unless_owned_by_active_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    step = run_update_daily.DailyStep("source", "source", ("cmd",))
    started = datetime(2026, 6, 8, 8, 0)
    old_row = {
        "status": "running",
        "started_at": started.isoformat(),
        "orchestrator_pid": 111,
    }
    monkeypatch.setattr(run_update_daily, "is_pid_alive", lambda pid: True)

    stale_state = {"source": dict(old_row)}
    assert run_update_daily._mark_abandoned_running_steps(
        [step],
        stale_state,
        tmp_path / "run.log",
        lambda: started + timedelta(days=2),
        {"pid": 222},
    )
    assert stale_state["source"]["status"] == "abandoned"

    active_state = {"source": dict(old_row)}
    assert not run_update_daily._mark_abandoned_running_steps(
        [step],
        active_state,
        tmp_path / "run.log",
        lambda: started + timedelta(days=2),
        {"pid": 111},
    )
    assert active_state["source"]["status"] == "running"


def test_orchestrator_prints_step_progress_to_console(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 8),
            now=lambda: datetime(2026, 6, 8, 9, 0),
            command_runner=lambda step, log_path: 0,
        )
        == 0
    )

    output = capsys.readouterr().out
    assert "Running cleanup" in output
    assert "Completed cleanup" in output
    assert str(log_file) in output


def test_orchestrator_optional_step_timeout_is_skipped_and_continues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    timeout_step = run_update_daily.DailyStep(
        "optional-timeout",
        "optional timeout",
        (sys.executable, "-c", "import time; time.sleep(5)"),
        optional=True,
        timeout_seconds=1,
    )
    next_step = run_update_daily.DailyStep(
        "after-timeout",
        "after timeout",
        (sys.executable, "-c", "print('after')"),
    )

    monkeypatch.setattr(run_update_daily, "daily_steps", lambda today=None: [timeout_step, next_step])
    monkeypatch.setattr(run_update_daily, "_wait_for_duckdb_available", lambda root: True)
    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 8),
            command_runner=lambda step, log_path: (
                calls.append(step.id) or run_update_daily.TIMEOUT_EXIT_CODE
                if step.id == "optional-timeout"
                else calls.append(step.id) or 0
            ),
        )
        == 0
    )

    assert calls == ["optional-timeout", "after-timeout"]
    states = _state(state_file)["runs"]["2026-06-08"]["steps"]
    assert states["optional-timeout"]["status"] == "skipped_timeout"
    assert states["after-timeout"]["status"] == "success"


def test_orchestrator_optional_step_timeout_stops_when_duckdb_is_locked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    timeout_step = run_update_daily.DailyStep(
        "optional-timeout",
        "optional timeout",
        (sys.executable, "-c", "import time; time.sleep(5)"),
        optional=True,
        timeout_seconds=1,
    )
    next_step = run_update_daily.DailyStep(
        "after-timeout",
        "after timeout",
        (sys.executable, "-c", "print('after')"),
    )

    monkeypatch.setattr(run_update_daily, "daily_steps", lambda today=None: [timeout_step, next_step])
    monkeypatch.setattr(run_update_daily, "_wait_for_duckdb_available", lambda root: False)

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 8),
            command_runner=lambda step, log_path: calls.append(step.id) or run_update_daily.TIMEOUT_EXIT_CODE,
        )
        == run_update_daily.TIMEOUT_EXIT_CODE
    )

    assert calls == ["optional-timeout"]
    states = _state(state_file)["runs"]["2026-06-08"]["steps"]
    assert states["optional-timeout"]["status"] == "failed_resource_locked"
    assert "after-timeout" not in states


def test_orchestrator_required_step_timeout_fails_and_continues_independent_steps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    timeout_step = run_update_daily.DailyStep(
        "required-timeout",
        "required timeout",
        (sys.executable, "-c", "import time; time.sleep(5)"),
        timeout_seconds=1,
    )
    next_step = run_update_daily.DailyStep(
        "after-timeout",
        "after timeout",
        (sys.executable, "-c", "print('after')"),
    )

    monkeypatch.setattr(run_update_daily, "daily_steps", lambda today=None: [timeout_step, next_step])
    monkeypatch.setattr(run_update_daily, "_wait_for_duckdb_available", lambda root: True)
    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 8),
            command_runner=lambda step, log_path: (
                calls.append(step.id) or run_update_daily.TIMEOUT_EXIT_CODE
                if step.id == "required-timeout"
                else calls.append(step.id) or 0
            ),
        )
        != 0
    )

    assert calls == ["required-timeout", "after-timeout"]
    states = _state(state_file)["runs"]["2026-06-08"]["steps"]
    assert states["required-timeout"]["status"] == "failed"
    assert states["after-timeout"]["status"] == "success"


def test_run_subprocess_timeout_terminates_process_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    log_file = tmp_path / "run.log"
    step = run_update_daily.DailyStep(
        "timeout",
        "timeout",
        (sys.executable, "-c", "import time; time.sleep(5)"),
        timeout_seconds=1,
    )
    terminated: list[int] = []

    class FakePopen:
        pid = 456

        def __init__(self, *args, **kwargs):
            pass

        def wait(self, timeout=None):
            raise subprocess.TimeoutExpired(step.command, timeout)

        def poll(self):
            return None

    def fake_terminate(proc, log):
        del log
        terminated.append(proc.pid)
        return True

    monkeypatch.setattr(run_update_daily.subprocess, "Popen", FakePopen)
    monkeypatch.setattr(run_update_daily, "_terminate_process_tree", fake_terminate)

    assert run_update_daily._run_subprocess(step, log_file, tmp_path) == run_update_daily.TIMEOUT_EXIT_CODE
    assert terminated == [456]


def test_run_subprocess_timeout_cleanup_failure_returns_distinct_code(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    log_file = tmp_path / "run.log"
    step = run_update_daily.DailyStep(
        "timeout",
        "timeout",
        (sys.executable, "-c", "import time; time.sleep(5)"),
        timeout_seconds=1,
    )

    class FakePopen:
        pid = 789

        def __init__(self, *args, **kwargs):
            pass

        def wait(self, timeout=None):
            raise subprocess.TimeoutExpired(step.command, timeout)

        def poll(self):
            return None

    monkeypatch.setattr(run_update_daily.subprocess, "Popen", FakePopen)
    monkeypatch.setattr(run_update_daily, "_terminate_process_tree", lambda proc, log: False)

    assert (
        run_update_daily._run_subprocess(step, log_file, tmp_path) == run_update_daily.TIMEOUT_CLEANUP_FAILED_EXIT_CODE
    )


def test_orchestrator_optional_timeout_cleanup_failure_stops(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    timeout_step = run_update_daily.DailyStep(
        "optional-timeout",
        "optional timeout",
        (sys.executable, "-c", "import time; time.sleep(5)"),
        optional=True,
        timeout_seconds=1,
    )
    next_step = run_update_daily.DailyStep(
        "after-timeout",
        "after timeout",
        (sys.executable, "-c", "print('after')"),
    )

    monkeypatch.setattr(run_update_daily, "daily_steps", lambda today=None: [timeout_step, next_step])

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 8),
            command_runner=lambda step, log_path: (
                calls.append(step.id) or run_update_daily.TIMEOUT_CLEANUP_FAILED_EXIT_CODE
            ),
        )
        == run_update_daily.TIMEOUT_CLEANUP_FAILED_EXIT_CODE
    )

    assert calls == ["optional-timeout"]
    states = _state(state_file)["runs"]["2026-06-08"]["steps"]
    assert states["optional-timeout"]["status"] == "failed_timeout_cleanup"
    assert "after-timeout" not in states
