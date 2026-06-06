from __future__ import annotations

import json
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

import pytest

from src.tools import run_update_daily


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

    assert run_update_daily.run_daily_update(
        root=tmp_path,
        state_file=state_file,
        run_log=log_file,
        today=date(2026, 6, 5),
        now=lambda: datetime(2026, 6, 5, 18, 0),
        command_runner=runner,
    ) == 0
    first_run = list(calls)
    assert first_run[0] == "cleanup"
    assert "baostock-qfq" in first_run

    calls.clear()
    assert run_update_daily.run_daily_update(
        root=tmp_path,
        state_file=state_file,
        run_log=log_file,
        today=date(2026, 6, 5),
        now=lambda: datetime(2026, 6, 5, 19, 0),
        command_runner=runner,
    ) == 0

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
    assert run_update_daily.run_daily_update(
        root=tmp_path,
        state_file=state_file,
        run_log=log_file,
        today=date(2026, 6, 5),
        now=lambda: datetime(2026, 6, 5, 18, 0),
        command_runner=lambda step, log_path: calls.append(step.id) or (7 if step.id == "source" else 0),
    ) == 7
    assert calls == ["source"]
    states = _state(state_file)["runs"]["2026-06-05"]["steps"]
    assert states["source"]["status"] == "failed"
    assert states["dependent"]["status"] == "blocked"

    calls.clear()
    assert run_update_daily.run_daily_update(
        root=tmp_path,
        state_file=state_file,
        run_log=log_file,
        today=date(2026, 6, 5),
        now=lambda: datetime(2026, 6, 5, 19, 0),
        command_runner=lambda step, log_path: calls.append(step.id) or 0,
    ) == 0
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

    assert run_update_daily.run_daily_update(
        root=tmp_path,
        state_file=state_file,
        run_log=log_file,
        today=date(2026, 6, 5),
        force=True,
        command_runner=lambda step, log_path: calls.append(step.id) or 0,
    ) == 0
    assert calls[0] == "cleanup"

    calls.clear()
    assert run_update_daily.run_daily_update(
        root=tmp_path,
        state_file=state_file,
        run_log=log_file,
        today=date(2026, 6, 5),
        start_at="baostock-qfq",
        command_runner=lambda step, log_path: calls.append(step.id) or 0,
    ) == 0
    assert calls[0] == "baostock-qfq"
    assert "calendar" not in calls


def test_orchestrator_weekend_window_filters_heavy_steps(tmp_path: Path) -> None:
    friday = [step.id for step in run_update_daily.daily_steps(date(2026, 6, 5))]
    monday = [step.id for step in run_update_daily.daily_steps(date(2026, 6, 8))]

    assert "baostock-qfq" in friday
    assert "akshare-valuation-full" in friday
    assert "baostock-qfq" not in monday
    assert "akshare-valuation-full" not in monday
    assert "financial-report" in monday


def test_weekend_akshare_valuation_failure_does_not_block_independent_later_steps(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    assert run_update_daily.run_daily_update(
        root=tmp_path,
        state_file=state_file,
        run_log=log_file,
        today=date(2026, 6, 6),
        now=lambda: datetime(2026, 6, 6, 1, 0),
        command_runner=lambda step, log_path: calls.append(step.id) or (7 if step.id == "akshare-valuation-full" else 0),
    ) == 7

    assert "akshare-valuation-full" in calls
    assert "akshare-report-disclosure" in calls
    assert "akshare-yysj-em" in calls
    assert "akshare-daily-bar" in calls
    assert "sync-qlib" in calls
    assert "financial-report" in calls
    assert calls[-1] == "build-duckdb-views"
    states = _state(state_file)["runs"]["2026-06-06"]["steps"]
    assert states["akshare-valuation-full"]["status"] == "failed"
    assert states["akshare-report-disclosure"]["status"] == "success"
    assert states["akshare-yysj-em"]["status"] == "success"
    assert states["akshare-daily-bar"]["status"] == "success"
    assert states["sync-qlib"]["status"] == "success"
    assert states["financial-report"]["status"] == "success"
    assert states["build-duckdb-views"]["status"] == "success"


def test_run_subprocess_disables_child_file_logging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    log_file = tmp_path / "run.log"
    step = run_update_daily.DailyStep("sample", "sample step", (sys.executable, "-c", "print('sample')"))

    def fake_run(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        kwargs["stdout"].write("captured child output\n")
        return subprocess.CompletedProcess(args[0], 0)

    monkeypatch.setattr(run_update_daily.subprocess, "run", fake_run)

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


def test_orchestrator_prints_step_progress_to_console(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"

    assert run_update_daily.run_daily_update(
        root=tmp_path,
        state_file=state_file,
        run_log=log_file,
        today=date(2026, 6, 8),
        now=lambda: datetime(2026, 6, 8, 9, 0),
        command_runner=lambda step, log_path: 0,
    ) == 0

    output = capsys.readouterr().out
    assert "Running cleanup" in output
    assert "Completed cleanup" in output
    assert str(log_file) in output


def test_orchestrator_optional_step_timeout_is_skipped_and_continues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"

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
    assert run_update_daily.run_daily_update(
        root=tmp_path,
        state_file=state_file,
        run_log=log_file,
        today=date(2026, 6, 8),
    ) == 0

    states = _state(state_file)["runs"]["2026-06-08"]["steps"]
    assert states["optional-timeout"]["status"] == "skipped_timeout"
    assert states["after-timeout"]["status"] == "success"


def test_orchestrator_required_step_timeout_fails_and_continues_independent_steps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"

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
    assert run_update_daily.run_daily_update(
        root=tmp_path,
        state_file=state_file,
        run_log=log_file,
        today=date(2026, 6, 8),
    ) != 0

    states = _state(state_file)["runs"]["2026-06-08"]["steps"]
    assert states["required-timeout"]["status"] == "failed"
    assert states["after-timeout"]["status"] == "success"
