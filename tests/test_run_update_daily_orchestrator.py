from __future__ import annotations

import json
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


def test_orchestrator_retries_from_failed_step(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    def failing_runner(step: run_update_daily.DailyStep, log_path: Path) -> int:
        del log_path
        calls.append(step.id)
        return 7 if step.id == "baostock-unadjusted" else 0

    assert run_update_daily.run_daily_update(
        root=tmp_path,
        state_file=state_file,
        run_log=log_file,
        today=date(2026, 6, 5),
        now=lambda: datetime(2026, 6, 5, 18, 0),
        command_runner=failing_runner,
    ) == 7
    assert calls[-1] == "baostock-unadjusted"
    assert _state(state_file)["runs"]["2026-06-05"]["steps"]["baostock-unadjusted"]["status"] == "failed"

    calls.clear()
    assert run_update_daily.run_daily_update(
        root=tmp_path,
        state_file=state_file,
        run_log=log_file,
        today=date(2026, 6, 5),
        now=lambda: datetime(2026, 6, 5, 19, 0),
        command_runner=lambda step, log_path: calls.append(step.id) or 0,
    ) == 0
    assert calls[0] == "baostock-unadjusted"


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
