from __future__ import annotations

import json
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.storage.parquet_store import ParquetStore
from src.tools import run_update_daily
from src.utils.process_lock import acquire_process_lock

REPO_ROOT = Path(__file__).resolve().parents[1]


def _state(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _steps(path: Path, key: str) -> dict:
    return _state(path)["runs"][key]["steps"]


def _dependency_ids(step: run_update_daily.DailyStep) -> tuple[str, ...]:
    return tuple(run_update_daily._dependency_step_id(dependency) for dependency in step.depends_on)


def _write_settings(root: Path) -> None:
    config_dir = root / "config"
    config_dir.mkdir(exist_ok=True)
    (config_dir / "settings.yaml").write_text("project:\n  timezone: Asia/Shanghai\n", encoding="utf-8")


def _write_repo_workflow(root: Path) -> None:
    config_dir = root / "config"
    config_dir.mkdir(exist_ok=True)
    (config_dir / "daily_workflow.yaml").write_text(
        (REPO_ROOT / "config" / "daily_workflow.yaml").read_text(encoding="utf-8"),
        encoding="utf-8",
    )


def _write_minimal_workflow(root: Path, extra_steps: str = "") -> None:
    config_dir = root / "config"
    config_dir.mkdir(exist_ok=True)
    (config_dir / "daily_workflow.yaml").write_text(
        f"""
steps:
  - id: cleanup
    name: cleanup
    schedule_policy: daily
    state_key_policy: run_instance
    resume_policy: always_run
    data_freshness_policy: maintenance
    command: ["cmd"]
  - id: market
    name: market
    schedule_policy: market_window
    state_key_policy: market_date
    resume_policy: skip_if_success
    data_freshness_policy: market_session
    command: ["cmd", "{{market_date}}", "{{hist_start}}"]
  - id: financial
    name: financial
    schedule_policy: daily
    state_key_policy: natural_date
    resume_policy: skip_if_success
    data_freshness_policy: natural_daily
    command: ["cmd"]
  - id: build-derived
    name: build
    schedule_policy: daily
    state_key_policy: natural_date
    resume_policy: skip_if_success
    data_freshness_policy: natural_daily
    command: ["cmd"]
    depends_on:
      - step: market
        state_key_policy: market_date
      - step: financial
        state_key_policy: natural_date
{extra_steps}
""".lstrip(),
        encoding="utf-8",
    )


def _write_market_heavy_workflow(root: Path, *, include_build: bool = True) -> None:
    build_step = (
        """
  - id: build-derived
    name: build
    schedule_policy: daily
    state_key_policy: natural_date
    resume_policy: skip_if_success
    data_freshness_policy: natural_daily
    command: ["cmd", "build"]
    depends_on:
      - step: market-heavy
        state_key_policy: market_date
      - step: financial
        state_key_policy: natural_date
""".rstrip()
        if include_build
        else ""
    )
    config_dir = root / "config"
    config_dir.mkdir(exist_ok=True)
    (config_dir / "daily_workflow.yaml").write_text(
        f"""
steps:
  - id: cleanup
    name: cleanup
    schedule_policy: daily
    state_key_policy: run_instance
    resume_policy: always_run
    data_freshness_policy: maintenance
    command: ["cmd", "cleanup"]
  - id: market-heavy
    name: market-heavy
    schedule_policy: market_window
    state_key_policy: market_date
    resume_policy: skip_if_success
    data_freshness_policy: market_session
    command: ["cmd", "market-heavy", "--end", "{{market_date}}", "--start", "{{hist_start}}"]
  - id: financial
    name: financial
    schedule_policy: daily
    state_key_policy: natural_date
    resume_policy: skip_if_success
    data_freshness_policy: natural_daily
    command: ["cmd", "financial"]
{build_step}
""".lstrip(),
        encoding="utf-8",
    )


def _write_calendar(root: Path, rows: list[tuple[str, str]]) -> None:
    _write_settings(root)
    store = ParquetStore(root=root)
    store.ensure_layout()
    try:
        store.write_dataset(
            "baostock_cn_trading_calendar",
            pd.DataFrame([{"calendar_date": day, "is_trading_day": flag} for day, flag in rows]),
        )
    finally:
        store.close()


def test_weekday_after_cutoff_does_not_schedule_market_window_heavy_step(tmp_path: Path) -> None:
    _write_market_heavy_workflow(tmp_path, include_build=False)
    _write_calendar(tmp_path, [("2026-06-09", "1")])
    calls: list[str] = []

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=tmp_path / "state.json",
            run_log=tmp_path / "run.log",
            today=date(2026, 6, 9),
            now=lambda: datetime(2026, 6, 9, 18, 0),
            command_runner=lambda step, log_path: calls.append(step.id) or 0,
        )
        == 0
    )

    assert "market-heavy" not in calls
    assert "financial" in calls


def test_friday_holiday_reuses_thursday_market_state_for_build_derived(tmp_path: Path) -> None:
    _write_market_heavy_workflow(tmp_path)
    _write_calendar(
        tmp_path,
        [
            ("2026-06-11", "1"),
            ("2026-06-12", "0"),
            ("2026-06-13", "0"),
            ("2026-06-14", "0"),
        ],
    )
    state_file = tmp_path / "state.json"
    calls: list[str] = []

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=tmp_path / "run.log",
            today=date(2026, 6, 11),
            as_of_date="2026-06-11",
            market_date="2026-06-11",
            now=lambda: datetime(2026, 6, 11, 18, 0),
            command_runner=lambda step, log_path: calls.append(step.id) or 0,
        )
        == 0
    )
    assert "market-heavy" in calls
    assert _steps(state_file, "market_date:2026-06-11")["market-heavy"]["status"] == "success"

    calls.clear()
    effective_dates = run_update_daily.resolve_daily_effective_dates(
        root=tmp_path,
        today=date(2026, 6, 12),
        as_of_date="2026-06-12",
    )
    assert effective_dates.market_date == date(2026, 6, 11)
    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=tmp_path / "run.log",
            today=date(2026, 6, 12),
            as_of_date="2026-06-12",
            now=lambda: datetime(2026, 6, 12, 18, 0),
            command_runner=lambda step, log_path: calls.append(step.id) or 0,
        )
        == 0
    )

    assert "market-heavy" not in calls
    assert "financial" in calls
    assert "build-derived" in calls
    friday_states = _steps(state_file, "natural_date:2026-06-12")
    assert friday_states["financial"]["status"] == "success"
    assert friday_states["build-derived"]["status"] == "success"


def test_friday_holiday_backfills_previous_market_date_when_state_is_missing(tmp_path: Path) -> None:
    _write_market_heavy_workflow(tmp_path)
    _write_calendar(tmp_path, [("2026-06-11", "1"), ("2026-06-12", "0")])
    state_file = tmp_path / "state.json"
    calls: list[str] = []
    commands: dict[str, tuple[str, ...]] = {}

    def runner(step: run_update_daily.DailyStep, log_path: Path) -> int:
        del log_path
        calls.append(step.id)
        commands[step.id] = step.command
        return 0

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=tmp_path / "run.log",
            today=date(2026, 6, 12),
            as_of_date="2026-06-12",
            now=lambda: datetime(2026, 6, 12, 18, 0),
            command_runner=runner,
        )
        == 0
    )

    assert "market-heavy" in calls
    assert commands["market-heavy"][commands["market-heavy"].index("--end") + 1] == "2026-06-11"
    assert _steps(state_file, "market_date:2026-06-11")["market-heavy"]["status"] == "success"
    assert _steps(state_file, "natural_date:2026-06-12")["financial"]["status"] == "success"


def test_holiday_monday_reuses_previous_friday_market_state(tmp_path: Path) -> None:
    _write_market_heavy_workflow(tmp_path, include_build=False)
    _write_calendar(
        tmp_path,
        [
            ("2026-06-12", "1"),
            ("2026-06-13", "0"),
            ("2026-06-14", "0"),
            ("2026-06-15", "0"),
        ],
    )
    state_file = tmp_path / "state.json"
    calls: list[str] = []

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=tmp_path / "run.log",
            today=date(2026, 6, 12),
            as_of_date="2026-06-12",
            market_date="2026-06-12",
            command_runner=lambda step, log_path: calls.append(step.id) or 0,
        )
        == 0
    )
    calls.clear()

    effective_dates = run_update_daily.resolve_daily_effective_dates(
        root=tmp_path,
        today=date(2026, 6, 15),
        as_of_date="2026-06-15",
    )
    assert effective_dates.market_date == date(2026, 6, 12)
    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=tmp_path / "run.log",
            today=date(2026, 6, 15),
            as_of_date="2026-06-15",
            command_runner=lambda step, log_path: calls.append(step.id) or 0,
        )
        == 0
    )

    assert "market-heavy" not in calls
    assert "financial" in calls
    assert _steps(state_file, "natural_date:2026-06-15")["financial"]["status"] == "success"


def test_trading_monday_before_cutoff_targets_previous_friday_market_date(tmp_path: Path) -> None:
    _write_market_heavy_workflow(tmp_path, include_build=False)
    _write_calendar(
        tmp_path,
        [
            ("2026-06-12", "1"),
            ("2026-06-13", "0"),
            ("2026-06-14", "0"),
            ("2026-06-15", "1"),
        ],
    )
    state_file = tmp_path / "state.json"
    calls: list[str] = []
    commands: dict[str, tuple[str, ...]] = {}

    def runner(step: run_update_daily.DailyStep, log_path: Path) -> int:
        del log_path
        calls.append(step.id)
        commands[step.id] = step.command
        return 0

    effective_dates = run_update_daily.resolve_daily_effective_dates(
        root=tmp_path,
        today=date(2026, 6, 15),
        now=lambda: datetime(2026, 6, 15, 17, 59),
    )
    assert effective_dates.candidate_date == date(2026, 6, 14)
    assert effective_dates.market_date == date(2026, 6, 12)

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=tmp_path / "run.log",
            today=date(2026, 6, 15),
            now=lambda: datetime(2026, 6, 15, 17, 59),
            command_runner=runner,
        )
        == 0
    )

    assert "market-heavy" in calls
    assert commands["market-heavy"][commands["market-heavy"].index("--end") + 1] == "2026-06-12"
    assert _steps(state_file, "market_date:2026-06-12")["market-heavy"]["status"] == "success"


def test_trading_monday_after_cutoff_filters_market_window_and_keeps_build_unblocked(tmp_path: Path) -> None:
    _write_market_heavy_workflow(tmp_path)
    _write_calendar(tmp_path, [("2026-06-15", "1")])
    state_file = tmp_path / "state.json"
    calls: list[str] = []

    effective_dates = run_update_daily.resolve_daily_effective_dates(
        root=tmp_path,
        today=date(2026, 6, 15),
        now=lambda: datetime(2026, 6, 15, 18, 0),
    )
    assert effective_dates.candidate_date == date(2026, 6, 15)
    assert effective_dates.market_date == date(2026, 6, 15)

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=tmp_path / "run.log",
            today=date(2026, 6, 15),
            now=lambda: datetime(2026, 6, 15, 18, 0),
            command_runner=lambda step, log_path: calls.append(step.id) or 0,
        )
        == 0
    )

    assert "market-heavy" not in calls
    assert "financial" in calls
    assert "build-derived" in calls
    monday_states = _steps(state_file, "natural_date:2026-06-15")
    assert monday_states["build-derived"]["status"] == "success"


def test_market_date_override_forces_market_window_step(tmp_path: Path) -> None:
    _write_market_heavy_workflow(tmp_path, include_build=False)
    state_file = tmp_path / "state.json"
    calls: list[str] = []
    commands: dict[str, tuple[str, ...]] = {}

    effective_dates = run_update_daily.resolve_daily_effective_dates(
        root=tmp_path,
        today=date(2026, 6, 9),
        as_of_date="2026-06-09",
        market_date="2026-06-08",
    )
    assert effective_dates.market_date_overridden is True

    def runner(step: run_update_daily.DailyStep, log_path: Path) -> int:
        del log_path
        calls.append(step.id)
        commands[step.id] = step.command
        return 0

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=tmp_path / "run.log",
            today=date(2026, 6, 9),
            as_of_date="2026-06-09",
            market_date="2026-06-08",
            command_runner=runner,
        )
        == 0
    )

    assert "market-heavy" in calls
    assert commands["market-heavy"][commands["market-heavy"].index("--end") + 1] == "2026-06-08"
    assert _steps(state_file, "market_date:2026-06-08")["market-heavy"]["status"] == "success"


def test_orchestrator_resumes_after_successful_steps(tmp_path: Path) -> None:
    _write_repo_workflow(tmp_path)
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
    assert "baostock-market-session" in first_run

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

    assert calls == ["cleanup", "baostock-market-session"]
    natural_states = _steps(state_file, "natural_date:2026-06-05")
    market_states = _steps(state_file, "market_date:2026-06-05")
    assert natural_states["calendar"]["status"] == "success"
    assert market_states["baostock-market-session"]["status"] == "success"


def test_baostock_market_session_always_runs_after_same_market_date_regular_day_success(tmp_path: Path) -> None:
    _write_repo_workflow(tmp_path)
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 10),
            as_of_date="2026-06-10",
            market_date="2026-06-10",
            now=lambda: datetime(2026, 6, 10, 18, 0),
            command_runner=lambda step, log_path: calls.append(step.id) or 0,
        )
        == 0
    )
    assert "baostock-market-session" in calls
    assert _steps(state_file, "market_date:2026-06-10")["baostock-market-session"]["status"] == "success"

    calls.clear()
    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 12),
            as_of_date="2026-06-10",
            market_date="2026-06-10",
            now=lambda: datetime(2026, 6, 12, 18, 0),
            command_runner=lambda step, log_path: calls.append(step.id) or 0,
        )
        == 0
    )

    assert "baostock-market-session" in calls
    assert not {"baostock-unadjusted", "baostock-adjustment-factor", "baostock-qfq", "baostock-hfq"} & set(calls)


def test_market_date_success_is_reused_on_weekend_and_holiday_monday(tmp_path: Path) -> None:
    _write_minimal_workflow(tmp_path)
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
            today=date(2026, 6, 12),
            as_of_date="2026-06-12",
            market_date="2026-06-12",
            now=lambda: datetime(2026, 6, 12, 18, 0),
            command_runner=runner,
        )
        == 0
    )
    assert "market" in calls
    assert _steps(state_file, "market_date:2026-06-12")["market"]["status"] == "success"

    calls.clear()
    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 13),
            as_of_date="2026-06-13",
            market_date="2026-06-12",
            now=lambda: datetime(2026, 6, 13, 18, 0),
            command_runner=runner,
        )
        == 0
    )
    assert "market" not in calls
    assert "financial" in calls
    assert _steps(state_file, "natural_date:2026-06-13")["financial"]["status"] == "success"

    calls.clear()
    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 14),
            as_of_date="2026-06-14",
            market_date="2026-06-12",
            now=lambda: datetime(2026, 6, 14, 18, 0),
            command_runner=runner,
        )
        == 0
    )
    assert "market" not in calls
    assert "financial" in calls
    assert _steps(state_file, "natural_date:2026-06-14")["financial"]["status"] == "success"

    calls.clear()
    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 15),
            as_of_date="2026-06-15",
            market_date="2026-06-12",
            now=lambda: datetime(2026, 6, 15, 18, 0),
            command_runner=runner,
        )
        == 0
    )
    assert "market" not in calls
    assert "financial" in calls
    assert _steps(state_file, "natural_date:2026-06-15")["financial"]["status"] == "success"
    assert _steps(state_file, "natural_date:2026-06-15")["build-derived"]["status"] == "success"


def test_monday_cutoff_resolves_previous_or_current_market_date(tmp_path: Path) -> None:
    _write_minimal_workflow(tmp_path)
    _write_calendar(
        tmp_path,
        [
            ("2026-06-05", "1"),
            ("2026-06-06", "0"),
            ("2026-06-07", "0"),
            ("2026-06-08", "1"),
        ],
    )
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 5),
            as_of_date="2026-06-05",
            market_date="2026-06-05",
            now=lambda: datetime(2026, 6, 5, 18, 0),
            command_runner=lambda step, log_path: calls.append(step.id) or 0,
        )
        == 0
    )
    calls.clear()

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 8),
            now=lambda: datetime(2026, 6, 8, 17, 59),
            command_runner=lambda step, log_path: calls.append(step.id) or 0,
        )
        == 0
    )
    assert "market" not in calls

    calls.clear()
    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 8),
            now=lambda: datetime(2026, 6, 8, 18, 0),
            command_runner=lambda step, log_path: calls.append(step.id) or 0,
        )
        == 0
    )
    assert "market" not in calls
    assert calls == ["cleanup"]
    assert _steps(state_file, "natural_date:2026-06-08")["financial"]["status"] == "success"


def test_build_derived_depends_on_market_and_natural_state_keys(tmp_path: Path) -> None:
    _write_minimal_workflow(tmp_path)
    state_file = tmp_path / "state.json"
    state_file.write_text(
        json.dumps(
            {
                "version": 2,
                "runs": {
                    "market_date:2026-06-12": {"steps": {"market": {"status": "success"}}},
                    "natural_date:2026-06-13": {"steps": {"financial": {"status": "success"}}},
                },
            }
        ),
        encoding="utf-8",
    )
    calls: list[str] = []

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=tmp_path / "run.log",
            today=date(2026, 6, 13),
            as_of_date="2026-06-13",
            market_date="2026-06-12",
            start_at="build-derived",
            command_runner=lambda step, log_path: calls.append(step.id) or 0,
        )
        == 0
    )

    assert calls == ["build-derived"]
    assert _steps(state_file, "natural_date:2026-06-13")["build-derived"]["status"] == "success"


def test_always_run_policy_reruns_successful_step(tmp_path: Path) -> None:
    _write_minimal_workflow(tmp_path)
    calls: list[str] = []
    kwargs = {
        "root": tmp_path,
        "state_file": tmp_path / "state.json",
        "run_log": tmp_path / "run.log",
        "today": date(2026, 6, 13),
        "as_of_date": "2026-06-13",
        "market_date": "2026-06-12",
        "now": lambda: datetime(2026, 6, 13, 18, 0),
        "command_runner": lambda step, log_path: calls.append(step.id) or 0,
    }

    assert run_update_daily.run_daily_update(**kwargs) == 0
    assert run_update_daily.run_daily_update(**kwargs) == 0

    assert calls.count("cleanup") == 2
    assert calls.count("market") == 1


def test_force_reruns_successful_market_date_step(tmp_path: Path) -> None:
    _write_minimal_workflow(tmp_path)
    state_file = tmp_path / "state.json"
    state_file.write_text(
        json.dumps({"version": 2, "runs": {"market_date:2026-06-12": {"steps": {"market": {"status": "success"}}}}}),
        encoding="utf-8",
    )
    calls: list[str] = []

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=tmp_path / "run.log",
            today=date(2026, 6, 13),
            as_of_date="2026-06-13",
            market_date="2026-06-12",
            force=True,
            command_runner=lambda step, log_path: calls.append(step.id) or 0,
        )
        == 0
    )

    assert "market" in calls


def test_legacy_natural_date_state_is_recognized_for_skip(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "daily_workflow.yaml").write_text(
        """
steps:
  - id: financial
    name: financial
    schedule_policy: daily
    state_key_policy: natural_date
    resume_policy: skip_if_success
    data_freshness_policy: natural_daily
    command: ["cmd"]
""".lstrip(),
        encoding="utf-8",
    )
    state_file = tmp_path / "state.json"
    state_file.write_text(
        json.dumps({"runs": {"2026-06-13": {"steps": {"financial": {"status": "success"}}}}}),
        encoding="utf-8",
    )
    calls: list[str] = []

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=tmp_path / "run.log",
            today=date(2026, 6, 13),
            as_of_date="2026-06-13",
            market_date="2026-06-12",
            command_runner=lambda step, log_path: calls.append(step.id) or 0,
        )
        == 0
    )

    assert calls == []
    assert _state(state_file)["version"] == 2


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schedule_policy", "sometimes"),
        ("state_key_policy", "business_date"),
        ("resume_policy", "maybe_skip"),
        ("data_freshness_policy", "stale"),
    ],
)
def test_daily_workflow_config_rejects_invalid_policy(tmp_path: Path, field: str, value: str) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "daily_workflow.yaml").write_text(
        f"""
steps:
  - id: broken
    name: broken
    {field}: {value}
    command: ["cmd"]
""".lstrip(),
        encoding="utf-8",
    )

    with pytest.raises(run_update_daily.DailyWorkflowConfigError, match=field):
        run_update_daily.daily_steps(date(2026, 6, 13), root=tmp_path)


def test_daily_workflow_config_missing_file_fails_fast(tmp_path: Path) -> None:
    with pytest.raises(run_update_daily.DailyWorkflowConfigError, match=r"config[\\/]daily_workflow\.yaml"):
        run_update_daily.daily_steps(date(2026, 6, 13), root=tmp_path)


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
    states = _steps(state_file, "natural_date:2026-06-05")
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
    states = _steps(state_file, "natural_date:2026-06-05")
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
    states = _steps(state_file, "natural_date:2026-06-05")
    assert states["source"]["status"] == "success"
    assert states["dependent"]["status"] == "success"


def test_orchestrator_force_and_start_at_control_resume(tmp_path: Path) -> None:
    _write_repo_workflow(tmp_path)
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
                            "baostock-market-session": {"status": "success"},
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
            start_at="baostock-market-session",
            command_runner=lambda step, log_path: calls.append(step.id) or 0,
        )
        == 0
    )
    assert calls[0] == "baostock-market-session"
    assert "calendar" not in calls


def test_orchestrator_market_window_schedules_heavy_steps_only_in_window() -> None:
    friday = [step.id for step in run_update_daily.daily_steps(date(2026, 6, 5), root=REPO_ROOT)]
    monday = [step.id for step in run_update_daily.daily_steps(date(2026, 6, 8), root=REPO_ROOT)]

    assert "baostock-market-session" in friday
    assert "akshare-valuation-full" in friday
    assert "akshare-yjyg-em" in friday
    assert "baostock-market-session" in monday
    assert "baostock-valuation-percentile" in monday
    assert "baostock-qfq" not in friday
    assert "baostock-qfq" not in monday
    assert "akshare-valuation-full" not in monday
    assert "akshare-yjyg-em" in monday
    assert "financial-report" in monday


def test_daily_steps_include_yjyg_em_before_build_views_on_weekday() -> None:
    steps = run_update_daily.daily_steps(date(2026, 6, 8), root=REPO_ROOT)
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
    steps = run_update_daily.daily_steps(today, root=REPO_ROOT)
    by_id = {step.id: step for step in steps}
    expected_dependencies = (
        (
            "akshare-spot-quote",
            "baostock-basic",
            "baostock-market-session",
            "baostock-valuation-percentile",
            "financial-report",
            "akshare-delist",
            "akshare-valuation-full",
            "akshare-daily-bar",
            "sync-qlib",
        )
        if today.weekday() in {4, 5, 6}
        else (
            "akshare-spot-quote",
            "baostock-basic",
            "baostock-market-session",
            "baostock-valuation-percentile",
            "financial-report",
        )
    )

    assert "build-derived" in by_id
    assert "build-security-master" not in by_id
    assert steps.index(by_id["financial-report"]) < steps.index(by_id["build-derived"])
    assert steps.index(by_id["build-derived"]) < steps.index(by_id["build-duckdb-views"])
    assert _dependency_ids(by_id["build-derived"]) == expected_dependencies
    assert _dependency_ids(by_id["build-duckdb-views"]) == ("build-derived",)
    assert by_id["build-derived"].command[1:] == (
        "-m",
        "src.cli",
        "build-derived",
        "--target",
        "all",
        "--mode",
        "incremental",
        "--no-build-duckdb-views",
    )


def test_daily_steps_load_weekday_steps_from_config() -> None:
    steps = run_update_daily.daily_steps(date(2026, 6, 8), root=REPO_ROOT)
    by_id = {step.id: step for step in steps}

    assert by_id["baostock-market-session"].schedule_policy == "daily"
    assert by_id["baostock-market-session"].state_key_policy == "market_date"
    assert by_id["baostock-market-session"].resume_policy == "always_run"
    assert _dependency_ids(by_id["baostock-market-session"]) == ("baostock-basic",)
    assert steps.index(by_id["baostock-basic"]) < steps.index(by_id["baostock-market-session"])
    assert "baostock-qfq" not in by_id
    assert by_id["build-derived"].command[1:] == (
        "-m",
        "src.cli",
        "build-derived",
        "--target",
        "all",
        "--mode",
        "incremental",
        "--no-build-duckdb-views",
    )


def test_daily_steps_load_weekend_steps_from_config() -> None:
    steps = run_update_daily.daily_steps(date(2026, 6, 6), root=REPO_ROOT)
    by_id = {step.id: step for step in steps}

    assert "baostock-market-session" in by_id
    assert "baostock-qfq" not in by_id
    assert "--start" in by_id["akshare-daily-bar"].command
    assert "2026-05-07" in by_id["akshare-daily-bar"].command
    assert "--end" in by_id["akshare-daily-bar"].command
    assert "akshare-valuation-full" in _dependency_ids(by_id["build-derived"])


def test_daily_workflow_config_missing_required_field_is_clear(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "daily_workflow.yaml").write_text("steps:\n  - id: broken\n", encoding="utf-8")

    with pytest.raises(run_update_daily.DailyWorkflowConfigError, match=r"broken.*command"):
        run_update_daily.daily_steps(date(2026, 6, 8), root=tmp_path)


def test_run_daily_update_records_yjyg_em_step_on_weekday(tmp_path: Path) -> None:
    _write_repo_workflow(tmp_path)
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
    states = _steps(state_file, "natural_date:2026-06-08")
    assert states["akshare-yjyg-em"]["status"] == "success"


@pytest.mark.parametrize("failed_step", ["baostock-market-session", "baostock-valuation-percentile"])
def test_core_baostock_failure_blocks_build_derived(tmp_path: Path, failed_step: str) -> None:
    _write_repo_workflow(tmp_path)
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
    states = _steps(state_file, "natural_date:2026-06-08")
    assert states["build-derived"]["status"] == "blocked"
    assert failed_step in states["build-derived"]["blocked_by"]


def test_weekend_daily_bar_failure_blocks_build_derived(tmp_path: Path) -> None:
    _write_repo_workflow(tmp_path)
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
    states = _steps(state_file, "natural_date:2026-06-06")
    assert states["build-derived"]["status"] == "blocked"
    assert states["build-derived"]["blocked_by"] == ["akshare-daily-bar"]


def test_optional_plain_skipped_does_not_block_build_derived(tmp_path: Path) -> None:
    _write_repo_workflow(tmp_path)
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 8),
            command_runner=lambda step, log_path: (
                calls.append(step.id) or (7 if step.id == "akshare-spot-quote" else 0)
            ),
        )
        == 0
    )

    assert "build-derived" in calls
    market_states = _steps(state_file, "market_date:2026-06-08")
    natural_states = _steps(state_file, "natural_date:2026-06-08")
    assert market_states["akshare-spot-quote"]["status"] == "skipped"
    assert natural_states["build-derived"]["status"] == "success"


@pytest.mark.parametrize("status", ["failed_resource_locked", "failed_timeout_cleanup"])
def test_failed_optional_hard_status_blocks_followup(status: str) -> None:
    step = run_update_daily.DailyStep("derived", "derived", ("cmd",), depends_on=("optional-source",))
    step_state = {"optional-source": {"status": status}}

    assert run_update_daily._blocked_dependencies(step, step_state) == ("optional-source",)


def test_weekend_akshare_valuation_failure_blocks_build_derived(tmp_path: Path) -> None:
    _write_repo_workflow(tmp_path)
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
    market_states = _steps(state_file, "market_date:2026-06-06")
    natural_states = _steps(state_file, "natural_date:2026-06-06")
    assert market_states["akshare-valuation-full"]["status"] == "failed"
    assert natural_states["akshare-report-disclosure"]["status"] == "success"
    assert natural_states["akshare-yysj-em"]["status"] == "success"
    assert natural_states["akshare-yjyg-em"]["status"] == "success"
    assert market_states["akshare-daily-bar"]["status"] == "success"
    assert market_states["sync-qlib"]["status"] == "success"
    assert natural_states["financial-report"]["status"] == "success"
    assert natural_states["build-derived"]["status"] == "blocked"
    assert natural_states["build-derived"]["blocked_by"] == ["akshare-valuation-full"]
    assert natural_states["build-duckdb-views"]["status"] == "blocked"


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
    _write_repo_workflow(tmp_path)
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

    assert json.loads(state_file.read_text(encoding="utf-8")) == {
        "version": 2,
        "runs": {"2026-06-12": {"步骤": "成功"}},
    }
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
    states = _steps(state_file, "natural_date:2026-06-08")
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
    _write_repo_workflow(tmp_path)
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
    states = _steps(state_file, "natural_date:2026-06-08")
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
    states = _steps(state_file, "natural_date:2026-06-08")
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
    states = _steps(state_file, "natural_date:2026-06-08")
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
    states = _steps(state_file, "natural_date:2026-06-08")
    assert states["optional-timeout"]["status"] == "failed_timeout_cleanup"
    assert "after-timeout" not in states
