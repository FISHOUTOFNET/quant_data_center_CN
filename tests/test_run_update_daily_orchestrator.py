from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.pipeline.step_health import StepHealthSummary, write_step_health_summary
from src.storage.parquet_store import ParquetStore
from src.tools import run_update_daily
from src.utils.process_lock import acquire_process_lock

REPO_ROOT = Path(__file__).resolve().parents[1]


def _write_summary_for_current_step(status: str, *, reason: str = "", failed_records: int = 0) -> None:
    """Write a StepHealthSummary to the path pointed to by QDC_STEP_RESULT_PATH.

    Test command runners call this to simulate a CLI command that wrote its
    own health summary (e.g. baostock-market-session with a tolerant policy).
    """

    path_str = os.environ.get(run_update_daily.STEP_RESULT_PATH_ENV)
    if not path_str:
        return
    summary = StepHealthSummary(
        status=status,
        reason=reason or f"test summary status={status}",
        total_records=200,
        success_records=200 - failed_records,
        failed_records=failed_records,
        skipped_records=0,
        failed_record_ratio=failed_records / 200.0 if failed_records else 0.0,
        failed_codes=("sh.699999",) if failed_records else (),
        failed_code_ratio=1 / 200.0 if failed_records else 0.0,
        failed_datasets=("baostock_cn_stock_basic",) if failed_records else (),
        fatal_datasets=(),
        fatal_statuses=(),
        examples=(),
        policy={},
    )
    write_step_health_summary(Path(path_str), summary)


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


def test_repo_workflow_maps_legacy_build_derived_start_at(tmp_path: Path) -> None:
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
            start_at="build-derived",
            command_runner=lambda step, log_path: calls.append(step.id) or 0,
        )
        == 0
    )

    assert calls == [
        "build-derived-security-master",
        "build-derived-daily-bar",
        "build-derived-valuation",
        "build-duckdb-views",
    ]
    states = _steps(state_file, "natural_date:2026-06-06")
    assert states["build-derived-security-master"]["status"] == "success"
    assert "Mapped legacy start-at build-derived to build-derived-security-master" in log_file.read_text(
        encoding="utf-8"
    )


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
        ("network_profile", "vpn"),
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
    assert monday == ["cleanup", "akshare-spot-quote"]
    assert "baostock-qfq" not in friday
    assert "baostock-qfq" not in monday
    assert "akshare-valuation-full" not in monday
    assert "akshare-yjyg-em" not in monday
    assert "financial-report" not in monday


def test_daily_steps_include_only_spot_quote_on_weekday() -> None:
    steps = run_update_daily.daily_steps(date(2026, 6, 8), root=REPO_ROOT)
    by_id = {step.id: step for step in steps}

    assert list(by_id) == ["cleanup", "akshare-spot-quote"]
    assert by_id["akshare-spot-quote"].optional is True
    assert by_id["akshare-spot-quote"].timeout_seconds is None
    assert by_id["akshare-spot-quote"].schedule_policy == "daily"
    assert by_id["akshare-spot-quote"].state_key_policy == "market_date"


def test_daily_steps_build_derived_stages_before_views_on_market_window() -> None:
    today = date(2026, 6, 6)
    steps = run_update_daily.daily_steps(today, root=REPO_ROOT)
    by_id = {step.id: step for step in steps}

    assert "build-derived-security-master" in by_id
    assert "build-derived-daily-bar" in by_id
    assert "build-derived-valuation" in by_id
    assert "build-derived" not in by_id
    assert "build-security-master" not in by_id
    assert steps.index(by_id["build-derived-security-master"]) < steps.index(by_id["build-derived-daily-bar"])
    assert steps.index(by_id["build-derived-security-master"]) < steps.index(by_id["build-derived-valuation"])
    assert steps.index(by_id["build-derived-daily-bar"]) < steps.index(by_id["build-duckdb-views"])
    assert steps.index(by_id["build-derived-valuation"]) < steps.index(by_id["build-duckdb-views"])
    assert _dependency_ids(by_id["build-derived-security-master"]) == ("baostock-basic", "akshare-delist")
    assert _dependency_ids(by_id["build-derived-daily-bar"]) == (
        "build-derived-security-master",
        "akshare-spot-quote",
        "baostock-market-session",
        "akshare-daily-bar",
    )
    assert _dependency_ids(by_id["build-derived-valuation"]) == (
        "build-derived-security-master",
        "baostock-valuation-percentile",
        "akshare-valuation-full",
    )
    assert _dependency_ids(by_id["build-duckdb-views"]) == (
        "build-derived-security-master",
        "build-derived-daily-bar",
        "build-derived-valuation",
        "sync-qlib",
    )
    assert by_id["build-derived-security-master"].command[1:] == (
        "-m",
        "src.cli",
        "build-derived",
        "--target",
        "security_master",
        "--mode",
        "incremental",
        "--no-build-duckdb-views",
    )
    assert "--no-include-security-master" in by_id["build-derived-daily-bar"].command
    assert "--no-include-security-master" in by_id["build-derived-valuation"].command


def test_daily_steps_load_weekday_steps_from_config() -> None:
    steps = run_update_daily.daily_steps(date(2026, 6, 8), root=REPO_ROOT)
    by_id = {step.id: step for step in steps}

    assert list(by_id) == ["cleanup", "akshare-spot-quote"]
    assert by_id["cleanup"].schedule_policy == "daily"
    assert by_id["akshare-spot-quote"].schedule_policy == "daily"
    assert "baostock-qfq" not in by_id
    assert "build-derived-security-master" not in by_id


def test_daily_steps_load_weekend_steps_from_config() -> None:
    steps = run_update_daily.daily_steps(date(2026, 6, 6), root=REPO_ROOT)
    by_id = {step.id: step for step in steps}

    assert "baostock-market-session" in by_id
    assert "baostock-qfq" not in by_id
    assert "--start" in by_id["akshare-daily-bar"].command
    assert "2026-05-07" in by_id["akshare-daily-bar"].command
    assert "--end" in by_id["akshare-daily-bar"].command
    assert "akshare-valuation-full" in _dependency_ids(by_id["build-derived-valuation"])


def test_daily_workflow_network_profiles_from_repo_config() -> None:
    steps = run_update_daily.daily_steps(date(2026, 6, 6), root=REPO_ROOT)
    by_id = {step.id: step for step in steps}

    assert by_id["sync-qlib"].network_profile == "inherit"
    for step_id in (
        "calendar",
        "akshare-spot-quote",
        "baostock-basic",
        "financial-report",
        "build-derived-security-master",
        "build-derived-daily-bar",
        "build-derived-valuation",
    ):
        assert by_id[step_id].network_profile == "direct"


def test_daily_workflow_config_missing_required_field_is_clear(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "daily_workflow.yaml").write_text("steps:\n  - id: broken\n", encoding="utf-8")

    with pytest.raises(run_update_daily.DailyWorkflowConfigError, match=r"broken.*command"):
        run_update_daily.daily_steps(date(2026, 6, 8), root=tmp_path)


def test_run_daily_update_records_only_spot_quote_on_weekday(tmp_path: Path) -> None:
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

    assert calls == ["cleanup", "akshare-spot-quote"]
    market_states = _steps(state_file, "market_date:2026-06-08")
    assert market_states["akshare-spot-quote"]["status"] == "success"
    assert "natural_date:2026-06-08" not in _state(state_file)["runs"]


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
            today=date(2026, 6, 6),
            command_runner=lambda step, log_path: calls.append(step.id) or (7 if step.id == failed_step else 0),
        )
        == 7
    )

    blocked_step = "build-derived-daily-bar" if failed_step == "baostock-market-session" else "build-derived-valuation"
    assert blocked_step not in calls
    states = _steps(state_file, "natural_date:2026-06-06")
    assert states[blocked_step]["status"] == "blocked"
    assert failed_step in states[blocked_step]["blocked_by"]


def test_weekend_daily_bar_failure_blocks_only_daily_bar_stage(tmp_path: Path) -> None:
    _write_repo_workflow(tmp_path)
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []
    commands: dict[str, tuple[str, ...]] = {}

    def runner(step: run_update_daily.DailyStep, log_path: Path) -> int:
        del log_path
        calls.append(step.id)
        commands[step.id] = step.command
        return 7 if step.id == "akshare-daily-bar" else 0

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 6),
            command_runner=runner,
        )
        == 7
    )

    assert "build-derived-security-master" in calls
    assert "build-derived-daily-bar" not in calls
    assert "build-derived-valuation" in calls
    assert "build-duckdb-views" not in calls
    states = _steps(state_file, "natural_date:2026-06-06")
    assert states["build-derived-security-master"]["network_profile"] == "direct"
    log_text = log_file.read_text(encoding="utf-8")
    assert "cannot run explicit target=daily_bar" in log_text
    assert "status failed for market_date:2026-06-06" in log_text
    assert "status pending" not in log_text
    assert states["build-derived-security-master"]["status"] == "success"
    assert states["build-derived-daily-bar"]["status"] == "blocked"
    assert states["build-derived-daily-bar"]["reason"].startswith("degraded: cannot build target=daily_bar")
    assert states["build-derived-valuation"]["status"] == "success"
    assert states["build-duckdb-views"]["status"] == "blocked"


def test_recovered_daily_bar_reruns_blocked_stage_and_views(tmp_path: Path) -> None:
    _write_repo_workflow(tmp_path)
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    first_calls: list[str] = []
    second_calls: list[str] = []
    second_commands: dict[str, tuple[str, ...]] = {}

    def first_runner(step: run_update_daily.DailyStep, log_path: Path) -> int:
        del log_path
        first_calls.append(step.id)
        return 7 if step.id == "akshare-daily-bar" else 0

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 6),
            now=lambda: datetime(2026, 6, 6, 1, 0),
            command_runner=first_runner,
        )
        == 7
    )
    first_states = _steps(state_file, "natural_date:2026-06-06")
    assert first_states["build-derived-security-master"]["status"] == "success"
    assert first_states["build-derived-daily-bar"]["status"] == "blocked"
    assert first_states["build-derived-valuation"]["status"] == "success"
    assert first_states["build-duckdb-views"]["status"] == "blocked"

    def second_runner(step: run_update_daily.DailyStep, log_path: Path) -> int:
        del log_path
        second_calls.append(step.id)
        second_commands[step.id] = step.command
        return 0

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 6),
            now=lambda: datetime(2026, 6, 6, 2, 0),
            command_runner=second_runner,
        )
        == 0
    )

    assert "akshare-daily-bar" in second_calls
    assert "build-derived-security-master" not in second_calls
    assert "build-derived-daily-bar" in second_calls
    assert "build-derived-valuation" not in second_calls
    assert "build-duckdb-views" in second_calls
    assert "--exclude-target" not in second_commands["build-derived-daily-bar"]
    second_states = _steps(state_file, "natural_date:2026-06-06")
    assert second_states["build-derived-daily-bar"]["status"] == "success"
    assert second_states["build-duckdb-views"]["status"] == "success"


def test_daily_bar_success_keeps_build_derived_command_and_success_status(tmp_path: Path) -> None:
    _write_repo_workflow(tmp_path)
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    commands: dict[str, tuple[str, ...]] = {}

    def runner(step: run_update_daily.DailyStep, log_path: Path) -> int:
        del log_path
        commands[step.id] = step.command
        return 0

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 6),
            command_runner=runner,
        )
        == 0
    )

    assert "--exclude-target" not in commands["build-derived-daily-bar"]
    states = _steps(state_file, "natural_date:2026-06-06")
    assert states["build-derived-daily-bar"]["status"] == "success"


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
            today=date(2026, 6, 6),
            command_runner=lambda step, log_path: (
                calls.append(step.id) or (7 if step.id == "akshare-spot-quote" else 0)
            ),
        )
        == 0
    )

    assert "build-derived-security-master" in calls
    assert "build-derived-daily-bar" in calls
    assert "build-derived-valuation" in calls
    market_states = _steps(state_file, "market_date:2026-06-06")
    natural_states = _steps(state_file, "natural_date:2026-06-06")
    assert market_states["akshare-spot-quote"]["status"] == "skipped"
    assert natural_states["build-derived-security-master"]["status"] == "success"
    assert natural_states["build-derived-daily-bar"]["status"] == "success"


@pytest.mark.parametrize("status", ["failed_resource_locked", "failed_timeout_cleanup"])
def test_failed_optional_hard_status_blocks_followup(status: str) -> None:
    step = run_update_daily.DailyStep("derived", "derived", ("cmd",), depends_on=("optional-source",))
    step_state = {"optional-source": {"status": status}}

    assert run_update_daily._blocked_dependencies(step, step_state) == ("optional-source",)


@pytest.mark.parametrize(
    "status", ["failed", "failed_resource_locked", "failed_timeout_cleanup", "blocked", "abandoned"]
)
def test_soft_dependency_failure_does_not_block(status: str) -> None:
    soft_dep = run_update_daily.DailyDependency("soft-source", soft=True)
    hard_dep = run_update_daily.DailyDependency("hard-source")
    step = run_update_daily.DailyStep("derived", "derived", ("cmd",), depends_on=(soft_dep, hard_dep))
    step_state = {"soft-source": {"status": status}, "hard-source": {"status": "success"}}

    assert run_update_daily._blocked_dependencies(step, step_state) == ()


def test_soft_dependency_pending_does_not_block() -> None:
    soft_dep = run_update_daily.DailyDependency("soft-source", soft=True)
    step = run_update_daily.DailyStep("derived", "derived", ("cmd",), depends_on=(soft_dep,))
    step_state = {"soft-source": {"status": "pending"}}

    assert run_update_daily._blocked_dependencies(step, step_state) == ()


def test_soft_and_hard_dependency_both_failed_blocks_on_hard_only() -> None:
    soft_dep = run_update_daily.DailyDependency("soft-source", soft=True)
    hard_dep = run_update_daily.DailyDependency("hard-source")
    step = run_update_daily.DailyStep("derived", "derived", ("cmd",), depends_on=(soft_dep, hard_dep))
    step_state = {"soft-source": {"status": "failed"}, "hard-source": {"status": "failed"}}

    assert run_update_daily._blocked_dependencies(step, step_state) == ("hard-source",)


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
    assert "build-derived-security-master" in calls
    assert "build-derived-daily-bar" in calls
    assert "build-derived-valuation" not in calls
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
    assert natural_states["build-derived-security-master"]["status"] == "success"
    assert natural_states["build-derived-daily-bar"]["status"] == "success"
    assert natural_states["build-derived-valuation"]["status"] == "blocked"
    assert natural_states["build-derived-valuation"]["blocked_by"] == ["akshare-valuation-full"]
    assert natural_states["build-duckdb-views"]["status"] == "blocked"


def test_sync_qlib_failure_degrades_build_views_without_blocking(tmp_path: Path) -> None:
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
            command_runner=lambda step, log_path: calls.append(step.id) or (7 if step.id == "sync-qlib" else 0),
        )
        == 7
    )

    assert "sync-qlib" in calls
    assert "build-derived-security-master" in calls
    assert "build-derived-daily-bar" in calls
    assert "build-derived-valuation" in calls
    assert "build-duckdb-views" in calls
    market_states = _steps(state_file, "market_date:2026-06-06")
    natural_states = _steps(state_file, "natural_date:2026-06-06")
    assert market_states["sync-qlib"]["status"] == "failed"
    assert natural_states["build-duckdb-views"]["status"] == "success_degraded"
    assert natural_states["build-duckdb-views"]["reason"].startswith("degraded: soft dependency sync-qlib")


def test_run_subprocess_disables_child_file_logging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.example")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example")
    monkeypatch.setenv("ALL_PROXY", "socks://proxy.example")
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
    assert env["QDC_NETWORK_PROFILE"] == "direct"
    assert env["NO_PROXY"] == "*"
    assert env["no_proxy"] == "*"
    assert "HTTP_PROXY" not in env
    assert "HTTPS_PROXY" not in env
    assert "ALL_PROXY" not in env
    assert captured["kwargs"]["stderr"] == subprocess.STDOUT
    assert log_file.read_text(encoding="utf-8") == "captured child output\n"


def test_run_subprocess_inherit_network_profile_preserves_proxy_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.example")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example")
    monkeypatch.setenv("ALL_PROXY", "socks://proxy.example")
    captured: dict[str, object] = {}
    log_file = tmp_path / "run.log"
    step = run_update_daily.DailyStep(
        "sync-qlib",
        "sync qlib",
        (sys.executable, "-c", "print('sample')"),
        network_profile="inherit",
    )

    class FakePopen:
        pid = 123

        def __init__(self, *args, **kwargs):
            captured["args"] = args
            captured["kwargs"] = kwargs

        def wait(self, timeout=None):
            return 0

        def poll(self):
            return 0

    monkeypatch.setattr(run_update_daily.subprocess, "Popen", FakePopen)

    assert run_update_daily._run_subprocess(step, log_file, tmp_path) == 0

    env = captured["kwargs"]["env"]
    assert env["QDC_DISABLE_FILE_LOG"] == "1"
    assert env["QDC_NETWORK_PROFILE"] == "inherit"
    assert env["HTTP_PROXY"] == "http://proxy.example"
    assert env["HTTPS_PROXY"] == "http://proxy.example"
    assert env["ALL_PROXY"] == "socks://proxy.example"


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


def test_run_daily_update_marks_historical_running_dead_pid_abandoned(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_file = tmp_path / "state.json"
    active_pid = os.getpid()
    state_file.write_text(
        json.dumps(
            {
                "version": 2,
                "runs": {
                    "natural_date:2026-06-01": {
                        "steps": {
                            "dead-step": {
                                "status": "running",
                                "command": "old command",
                                "log_path": "logs/old.log",
                                "started_at": "2026-06-01 18:00:00",
                                "updated_at": "2026-06-01 18:00:00",
                                "ended_at": None,
                                "exit_code": None,
                                "orchestrator_pid": 999999,
                            },
                            "active-step": {
                                "status": "running",
                                "command": "active command",
                                "log_path": "logs/active.log",
                                "started_at": "2026-06-01 18:00:00",
                                "updated_at": "2026-06-01 18:00:00",
                                "ended_at": None,
                                "exit_code": None,
                                "orchestrator_pid": active_pid,
                            },
                        }
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        run_update_daily,
        "daily_steps",
        lambda *args, **kwargs: [run_update_daily.DailyStep("one", "one", ("cmd",))],
    )
    monkeypatch.setattr(run_update_daily, "is_pid_alive", lambda pid: pid == active_pid)

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=tmp_path / "run.log",
            today=date(2026, 6, 8),
            now=lambda: datetime(2026, 6, 8, 18, 0),
            command_runner=lambda step, log_path: 0,
        )
        == 0
    )

    old_steps = _steps(state_file, "natural_date:2026-06-01")
    assert old_steps["dead-step"]["status"] == "abandoned"
    assert old_steps["dead-step"]["command"] == "old command"
    assert old_steps["dead-step"]["log_path"] == "logs/old.log"
    assert old_steps["dead-step"]["reason"] == "orchestrator pid 999999 is not alive"
    assert old_steps["active-step"]["status"] == "running"


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


# ---------------------------------------------------------------------------
# Step Health integration tests
# ---------------------------------------------------------------------------


def test_step_result_path_layout() -> None:
    base = Path("/tmp/fake")
    step = run_update_daily.DailyStep("my-step", "my step", ("cmd",))
    path = run_update_daily._step_result_path(base, "run_instance:20260606_180000", step)
    assert path == Path("/tmp/fake/data/metadata/step_results/20260606_180000/my-step.json")


def test_critical_derived_parquet_available_returns_false_when_missing(tmp_path: Path) -> None:
    assert run_update_daily._critical_derived_parquet_available(tmp_path) is False


def test_critical_derived_parquet_available_returns_false_when_partial(tmp_path: Path) -> None:
    parquet_root = tmp_path / "data" / "parquet"
    (parquet_root / "cn_security_master" / "code=sh.600000").mkdir(parents=True)
    (parquet_root / "cn_security_master" / "code=sh.600000" / "data.parquet").write_bytes(b"")
    # cn_stock_daily_bar and cn_stock_valuation missing
    assert run_update_daily._critical_derived_parquet_available(tmp_path) is False


def test_critical_derived_parquet_available_returns_true_when_all_present(tmp_path: Path) -> None:
    parquet_root = tmp_path / "data" / "parquet"
    for dataset_id in run_update_daily.CRITICAL_DERIVED_DATASETS_FOR_VIEWS:
        dataset_dir = parquet_root / dataset_id / "partition=1"
        dataset_dir.mkdir(parents=True)
        (dataset_dir / "data.parquet").write_bytes(b"")
    assert run_update_daily._critical_derived_parquet_available(tmp_path) is True


def test_critical_derived_parquet_ignores_tmp_files(tmp_path: Path) -> None:
    parquet_root = tmp_path / "data" / "parquet"
    for dataset_id in run_update_daily.CRITICAL_DERIVED_DATASETS_FOR_VIEWS:
        dataset_dir = parquet_root / dataset_id
        dataset_dir.mkdir(parents=True)
        # Only tmp parquet files should not count
        (dataset_dir / "data.tmp.parquet").write_bytes(b"")
    assert run_update_daily._critical_derived_parquet_available(tmp_path) is False


def _write_critical_derived_parquet(root: Path) -> None:
    """Create minimal parquet files for all critical derived datasets."""
    parquet_root = root / "data" / "parquet"
    for dataset_id in run_update_daily.CRITICAL_DERIVED_DATASETS_FOR_VIEWS:
        dataset_dir = parquet_root / dataset_id / "partition=1"
        dataset_dir.mkdir(parents=True, exist_ok=True)
        (dataset_dir / "data.parquet").write_bytes(b"PAR1")


def test_baostock_market_session_degraded_summary_lets_build_derived_continue(tmp_path: Path) -> None:
    _write_repo_workflow(tmp_path)
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    def runner(step: run_update_daily.DailyStep, log_path: Path) -> int:
        calls.append(step.id)
        if step.id == "baostock-market-session":
            _write_summary_for_current_step(
                "success_degraded",
                reason="degraded: 1 of 200 records failed",
                failed_records=1,
            )
        return 0

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 6),
            now=lambda: datetime(2026, 6, 6, 1, 0),
            command_runner=runner,
        )
        == 0
    )

    assert "baostock-market-session" in calls
    assert "build-derived-daily-bar" in calls
    assert "build-derived-valuation" in calls
    assert "build-duckdb-views" in calls
    market_states = _steps(state_file, "market_date:2026-06-06")
    natural_states = _steps(state_file, "natural_date:2026-06-06")
    assert market_states["baostock-market-session"]["status"] == "success_degraded"
    assert market_states["baostock-market-session"]["failed_count"] == 1
    assert market_states["baostock-market-session"]["record_count"] == 200
    assert "health_summary_path" in market_states["baostock-market-session"]
    # Downstream should inherit degraded reason
    assert natural_states["build-derived-daily-bar"]["status"] == "success_degraded"
    assert "upstream dependency baostock-market-session" in natural_states["build-derived-daily-bar"]["reason"]


def test_baostock_market_session_failed_summary_blocks_downstream(tmp_path: Path) -> None:
    _write_repo_workflow(tmp_path)
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    def runner(step: run_update_daily.DailyStep, log_path: Path) -> int:
        calls.append(step.id)
        if step.id == "baostock-market-session":
            _write_summary_for_current_step("failed", reason="fatal: metadata failure")
        return 0

    # Even though exit_code is 0, the failed summary should make final exit non-zero
    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 6),
            now=lambda: datetime(2026, 6, 6, 1, 0),
            command_runner=runner,
        )
        != 0
    )

    assert "baostock-market-session" in calls
    assert "build-derived-daily-bar" not in calls
    market_states = _steps(state_file, "market_date:2026-06-06")
    natural_states = _steps(state_file, "natural_date:2026-06-06")
    assert market_states["baostock-market-session"]["status"] == "failed"
    assert natural_states["build-derived-daily-bar"]["status"] == "blocked"


def test_akshare_valuation_full_degraded_summary_lets_build_derived_continue(tmp_path: Path) -> None:
    _write_repo_workflow(tmp_path)
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    def runner(step: run_update_daily.DailyStep, log_path: Path) -> int:
        calls.append(step.id)
        if step.id == "akshare-valuation-full":
            _write_summary_for_current_step(
                "success_degraded",
                reason="degraded: 2 of 500 codes timed out",
                failed_records=2,
            )
        return 0

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 6),
            now=lambda: datetime(2026, 6, 6, 1, 0),
            command_runner=runner,
        )
        == 0
    )

    assert "akshare-valuation-full" in calls
    assert "build-derived-valuation" in calls
    market_states = _steps(state_file, "market_date:2026-06-06")
    natural_states = _steps(state_file, "natural_date:2026-06-06")
    assert market_states["akshare-valuation-full"]["status"] == "success_degraded"
    assert natural_states["build-derived-valuation"]["status"] == "success_degraded"
    assert "upstream dependency akshare-valuation-full" in natural_states["build-derived-valuation"]["reason"]


def test_akshare_daily_bar_degraded_summary_lets_build_derived_continue(tmp_path: Path) -> None:
    _write_repo_workflow(tmp_path)
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    def runner(step: run_update_daily.DailyStep, log_path: Path) -> int:
        calls.append(step.id)
        if step.id == "akshare-daily-bar":
            _write_summary_for_current_step(
                "success_degraded",
                reason="degraded: 1 of 300 adjustment tasks failed",
                failed_records=1,
            )
        return 0

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 6),
            now=lambda: datetime(2026, 6, 6, 1, 0),
            command_runner=runner,
        )
        == 0
    )

    assert "akshare-daily-bar" in calls
    assert "build-derived-daily-bar" in calls
    market_states = _steps(state_file, "market_date:2026-06-06")
    assert market_states["akshare-daily-bar"]["status"] == "success_degraded"


def test_build_duckdb_views_continues_when_upstream_success_degraded(tmp_path: Path) -> None:
    _write_repo_workflow(tmp_path)
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    def runner(step: run_update_daily.DailyStep, log_path: Path) -> int:
        calls.append(step.id)
        if step.id == "build-derived-daily-bar":
            _write_summary_for_current_step("success_degraded", reason="degraded: upstream baostock")
        return 0

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 6),
            now=lambda: datetime(2026, 6, 6, 1, 0),
            command_runner=runner,
        )
        == 0
    )

    assert "build-duckdb-views" in calls
    natural_states = _steps(state_file, "natural_date:2026-06-06")
    assert natural_states["build-derived-daily-bar"]["status"] == "success_degraded"
    assert natural_states["build-duckdb-views"]["status"] == "success_degraded"
    assert "upstream dependency build-derived-daily-bar" in natural_states["build-duckdb-views"]["reason"]


def test_build_duckdb_views_stale_rebuild_when_upstream_failed_but_parquet_exists(tmp_path: Path) -> None:
    _write_repo_workflow(tmp_path)
    _write_critical_derived_parquet(tmp_path)
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    def runner(step: run_update_daily.DailyStep, log_path: Path) -> int:
        calls.append(step.id)
        # build-derived-daily-bar fails (exit code 7), but critical parquet exists
        if step.id == "build-derived-daily-bar":
            return 7
        return 0

    # Final exit code should be non-zero because build-derived-daily-bar failed
    result = run_update_daily.run_daily_update(
        root=tmp_path,
        state_file=state_file,
        run_log=log_file,
        today=date(2026, 6, 6),
        now=lambda: datetime(2026, 6, 6, 1, 0),
        command_runner=runner,
    )
    assert result != 0

    assert "build-derived-daily-bar" in calls
    assert "build-duckdb-views" in calls
    natural_states = _steps(state_file, "natural_date:2026-06-06")
    assert natural_states["build-derived-daily-bar"]["status"] == "failed"
    # build-duckdb-views should run with stale-aware reason
    assert natural_states["build-duckdb-views"]["status"] == "success_degraded"
    assert "critical derived parquet exists" in natural_states["build-duckdb-views"]["reason"]
    assert "stale parquet" in natural_states["build-duckdb-views"]["reason"]


def test_build_duckdb_views_blocked_when_upstream_failed_and_parquet_missing(tmp_path: Path) -> None:
    _write_repo_workflow(tmp_path)
    # Do NOT create critical derived parquet files
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    def runner(step: run_update_daily.DailyStep, log_path: Path) -> int:
        calls.append(step.id)
        if step.id == "build-derived-daily-bar":
            return 7
        return 0

    result = run_update_daily.run_daily_update(
        root=tmp_path,
        state_file=state_file,
        run_log=log_file,
        today=date(2026, 6, 6),
        now=lambda: datetime(2026, 6, 6, 1, 0),
        command_runner=runner,
    )
    assert result != 0

    assert "build-derived-daily-bar" in calls
    assert "build-duckdb-views" not in calls
    natural_states = _steps(state_file, "natural_date:2026-06-06")
    assert natural_states["build-derived-daily-bar"]["status"] == "failed"
    assert natural_states["build-duckdb-views"]["status"] == "blocked"
    assert "build-derived-daily-bar" in natural_states["build-duckdb-views"]["blocked_by"]


def test_build_duckdb_views_stale_rebuild_uses_stale_reason_when_valuation_failed(tmp_path: Path) -> None:
    _write_repo_workflow(tmp_path)
    _write_critical_derived_parquet(tmp_path)
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    def runner(step: run_update_daily.DailyStep, log_path: Path) -> int:
        calls.append(step.id)
        if step.id == "build-derived-valuation":
            return 7
        return 0

    result = run_update_daily.run_daily_update(
        root=tmp_path,
        state_file=state_file,
        run_log=log_file,
        today=date(2026, 6, 6),
        now=lambda: datetime(2026, 6, 6, 1, 0),
        command_runner=runner,
    )
    assert result != 0

    assert "build-duckdb-views" in calls
    natural_states = _steps(state_file, "natural_date:2026-06-06")
    assert natural_states["build-derived-valuation"]["status"] == "failed"
    assert natural_states["build-duckdb-views"]["status"] == "success_degraded"
    assert "stale parquet" in natural_states["build-duckdb-views"]["reason"]


def test_strict_build_derived_failure_still_blocks_without_summary(tmp_path: Path) -> None:
    """build-derived is strict by default; a failed record must still fail it."""
    _write_repo_workflow(tmp_path)
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    calls: list[str] = []

    def runner(step: run_update_daily.DailyStep, log_path: Path) -> int:
        calls.append(step.id)
        # build-derived-security-master returns failure exit code
        if step.id == "build-derived-security-master":
            return 7
        return 0

    result = run_update_daily.run_daily_update(
        root=tmp_path,
        state_file=state_file,
        run_log=log_file,
        today=date(2026, 6, 6),
        now=lambda: datetime(2026, 6, 6, 1, 0),
        command_runner=runner,
    )
    assert result != 0
    assert "build-derived-daily-bar" not in calls
    natural_states = _steps(state_file, "natural_date:2026-06-06")
    assert natural_states["build-derived-security-master"]["status"] == "failed"
    assert natural_states["build-derived-daily-bar"]["status"] == "blocked"


def test_orchestrator_restores_env_var_after_step_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """QDC_STEP_RESULT_PATH should be unset after the orchestrator runs a step."""
    _write_repo_workflow(tmp_path)
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    monkeypatch.delenv(run_update_daily.STEP_RESULT_PATH_ENV, raising=False)

    run_update_daily.run_daily_update(
        root=tmp_path,
        state_file=state_file,
        run_log=log_file,
        today=date(2026, 6, 6),
        now=lambda: datetime(2026, 6, 6, 1, 0),
        command_runner=lambda step, log_path: 0,
    )
    # Env var should not leak outside the orchestrator
    assert run_update_daily.STEP_RESULT_PATH_ENV not in os.environ


def test_orchestrator_preserves_existing_env_var_after_step_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If QDC_STEP_RESULT_PATH was already set, it should be restored."""
    _write_repo_workflow(tmp_path)
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"
    existing_path = str(tmp_path / "preexisting.json")
    monkeypatch.setenv(run_update_daily.STEP_RESULT_PATH_ENV, existing_path)

    run_update_daily.run_daily_update(
        root=tmp_path,
        state_file=state_file,
        run_log=log_file,
        today=date(2026, 6, 6),
        now=lambda: datetime(2026, 6, 6, 1, 0),
        command_runner=lambda step, log_path: 0,
    )
    assert os.environ.get(run_update_daily.STEP_RESULT_PATH_ENV) == existing_path


def test_orchestrator_records_health_summary_fields_in_state(tmp_path: Path) -> None:
    _write_repo_workflow(tmp_path)
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"

    def runner(step: run_update_daily.DailyStep, log_path: Path) -> int:
        if step.id == "baostock-market-session":
            _write_summary_for_current_step(
                "success_degraded",
                reason="degraded: 3 codes failed",
                failed_records=3,
            )
        return 0

    run_update_daily.run_daily_update(
        root=tmp_path,
        state_file=state_file,
        run_log=log_file,
        today=date(2026, 6, 6),
        now=lambda: datetime(2026, 6, 6, 1, 0),
        command_runner=runner,
    )
    market_states = _steps(state_file, "market_date:2026-06-06")
    row = market_states["baostock-market-session"]
    assert row["status"] == "success_degraded"
    assert row["record_count"] == 200
    assert row["success_count"] == 197
    assert row["failed_count"] == 3
    assert row["failed_ratio"] == 3 / 200.0
    assert "sh.699999" in row["failed_codes_sample"]
    assert "health_summary_path" in row


def test_success_degraded_does_not_cause_nonzero_final_exit(tmp_path: Path) -> None:
    _write_repo_workflow(tmp_path)
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "run.log"

    def runner(step: run_update_daily.DailyStep, log_path: Path) -> int:
        if step.id == "baostock-market-session":
            _write_summary_for_current_step("success_degraded", reason="degraded")
        if step.id == "akshare-valuation-full":
            _write_summary_for_current_step("success_degraded", reason="degraded")
        return 0

    assert (
        run_update_daily.run_daily_update(
            root=tmp_path,
            state_file=state_file,
            run_log=log_file,
            today=date(2026, 6, 6),
            now=lambda: datetime(2026, 6, 6, 1, 0),
            command_runner=runner,
        )
        == 0
    )
