"""Resumable daily update orchestrator."""

from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any


class StateFileError(RuntimeError):
    """Raised when the daily update state file cannot be read safely."""


@dataclass(frozen=True)
class DailyStep:
    id: str
    name: str
    command: tuple[str, ...]
    optional: bool = False
    timeout_seconds: int | None = None

    @property
    def command_text(self) -> str:
        return " ".join(self.command)


CommandRunner = Callable[[DailyStep, Path], int]
TIMEOUT_EXIT_CODE = 124


def daily_steps(today: date | None = None) -> list[DailyStep]:
    resolved_today = today or date.today()
    weekend_window = resolved_today.weekday() in {4, 5, 6}
    hist_start = (resolved_today - timedelta(days=30)).isoformat()

    steps = [
        DailyStep("cleanup", "cleanup expired logs", _cmd("src.tools.log_cleanup", "--retention-days", "30"), True),
        DailyStep(
            "calendar",
            "update-baostock-daily calendar",
            _cli("update-baostock-daily", "--dataset", "baostock_cn_trading_calendar", "--no-build-duckdb-views"),
        ),
    ]
    if weekend_window:
        steps.append(
            DailyStep(
                "akshare-delist",
                "akshare update delist",
                _cli("akshare", "update", "--target", "delist", "--no-build-duckdb-views"),
            )
        )

    steps.extend(
        [
            DailyStep(
                "akshare-spot-quote",
                "akshare update spot_quote",
                _cli("akshare", "update", "--target", "spot_quote", "--no-build-duckdb-views"),
                True,
                900,
            ),
            DailyStep(
                "baostock-unadjusted",
                "update-baostock-daily unadjusted",
                _cli(
                    "update-baostock-daily",
                    "--dataset",
                    "baostock_cn_stock_daily_bar_unadjusted",
                    "--no-build-duckdb-views",
                ),
            ),
            DailyStep(
                "baostock-basic",
                "update-baostock-daily stock basic",
                _cli("update-baostock-daily", "--dataset", "baostock_cn_stock_basic", "--no-build-duckdb-views"),
            ),
            DailyStep(
                "baostock-valuation-percentile",
                "update-baostock-valuation-percentile",
                _cli("update-baostock-valuation-percentile", "--no-build-duckdb-views"),
            ),
        ]
    )

    if weekend_window:
        steps.extend(
            [
                DailyStep(
                    "baostock-adjustment-factor",
                    "update-baostock-daily adjustment factor",
                    _cli(
                        "update-baostock-daily",
                        "--dataset",
                        "baostock_cn_stock_adjustment_factor",
                        "--no-build-duckdb-views",
                    ),
                ),
                DailyStep(
                    "baostock-qfq",
                    "update-baostock-daily qfq",
                    _cli(
                        "update-baostock-daily",
                        "--dataset",
                        "baostock_cn_stock_daily_bar_qfq",
                        "--no-build-duckdb-views",
                    ),
                ),
                DailyStep(
                    "baostock-hfq",
                    "update-baostock-daily hfq",
                    _cli(
                        "update-baostock-daily",
                        "--dataset",
                        "baostock_cn_stock_daily_bar_hfq",
                        "--no-build-duckdb-views",
                    ),
                ),
                DailyStep(
                    "akshare-valuation-full",
                    "akshare update valuation full",
                    _cli("akshare", "update", "--target", "valuation", "--mode", "full", "--no-build-duckdb-views"),
                ),
                DailyStep(
                    "akshare-report-disclosure",
                    "akshare update report_disclosure",
                    _cli("akshare", "update", "--target", "report_disclosure", "--no-build-duckdb-views"),
                ),
                DailyStep(
                    "akshare-yysj-em",
                    "akshare update yysj_em",
                    _cli("akshare", "update", "--target", "yysj_em", "--no-build-duckdb-views"),
                ),
                DailyStep(
                    "akshare-daily-bar",
                    f"akshare update daily_bar incremental all from {hist_start}",
                    _cli(
                        "akshare",
                        "update",
                        "--target",
                        "daily_bar",
                        "--mode",
                        "incremental",
                        "--adjustment",
                        "all",
                        "--start",
                        hist_start,
                        "--no-build-duckdb-views",
                    ),
                ),
                DailyStep(
                    "sync-qlib",
                    "sync-qlib",
                    _cli("sync-qlib", "--no-build-duckdb-views", "--max-runtime-seconds", "7200"),
                ),
            ]
        )

    steps.extend(
        [
            DailyStep(
                "financial-report",
                "akshare update financial_report incremental",
                _cli(
                    "akshare",
                    "update",
                    "--target",
                    "financial_report",
                    "--mode",
                    "incremental",
                    "--no-build-duckdb-views",
                ),
            ),
            DailyStep("build-duckdb-views", "build-duckdb-views", _cli("build-duckdb-views")),
        ]
    )
    return steps


def run_daily_update(
    *,
    root: Path | None = None,
    state_file: Path | None = None,
    run_log: Path | None = None,
    today: date | None = None,
    now: Callable[[], datetime] | None = None,
    force: bool = False,
    start_at: str | None = None,
    command_runner: CommandRunner | None = None,
) -> int:
    base = (root or Path.cwd()).resolve()
    resolved_today = today or date.today()
    run_key = resolved_today.isoformat()
    resolved_state_file = state_file or base / "data" / "metadata" / "run_update_daily_state.json"
    resolved_log = run_log or _default_run_log(base, now)
    runner = command_runner or (lambda step, log_path: _run_subprocess(step, log_path, base))
    steps = daily_steps(resolved_today)
    step_ids = [step.id for step in steps]

    if start_at is not None and start_at not in step_ids:
        raise ValueError(f"Unknown daily update step id: {start_at}")

    state = {"runs": {}} if force else _read_state(resolved_state_file)
    run_state = state.setdefault("runs", {}).setdefault(run_key, {"steps": {}})
    step_state = run_state.setdefault("steps", {})
    resolved_log.parent.mkdir(parents=True, exist_ok=True)

    start_seen = start_at is None
    for step in steps:
        if not start_seen:
            if step.id == start_at:
                start_seen = True
            else:
                _record_step(step_state, step, "skipped", 0, resolved_log, now)
                _emit(resolved_log, now, f"Skipped {step.id} before start-at {start_at}", console=True)
                continue

        current_status = str(step_state.get(step.id, {}).get("status", "pending"))
        if start_at is None and current_status == "success":
            _emit(resolved_log, now, f"Skipped {step.id}; already successful", console=True)
            continue

        _record_step(step_state, step, "running", None, resolved_log, now)
        _write_state(resolved_state_file, state)
        _emit(resolved_log, now, f"Running {step.id} ({step.name})... log={resolved_log}", console=True)
        _emit(resolved_log, now, f"Command: {step.command_text}", console=False)

        exit_code = int(runner(step, resolved_log))
        if exit_code != 0:
            timed_out = exit_code == TIMEOUT_EXIT_CODE and step.timeout_seconds is not None
            if step.optional:
                status = "skipped_timeout" if timed_out else "skipped"
                reason = (
                    f"{step.name} timed out after {step.timeout_seconds} seconds; continuing"
                    if timed_out
                    else f"{step.name} completed with warnings; continuing after error code {exit_code}"
                )
                _emit(
                    resolved_log,
                    now,
                    reason,
                    console=True,
                )
                _record_step(step_state, step, status, exit_code, resolved_log, now)
                _write_state(resolved_state_file, state)
                continue
            if timed_out:
                _emit(
                    resolved_log,
                    now,
                    f"{step.name} timed out after {step.timeout_seconds} seconds; stopping",
                    console=True,
                )
            else:
                _emit(resolved_log, now, f"{step.name} failed with error code {exit_code}", console=True)
            _record_step(step_state, step, "failed", exit_code, resolved_log, now)
            _write_state(resolved_state_file, state)
            return exit_code

        _emit(resolved_log, now, f"Completed {step.id} ({step.name})", console=True)
        _record_step(step_state, step, "success", 0, resolved_log, now)
        _write_state(resolved_state_file, state)

    _emit(resolved_log, now, "All updates completed successfully", console=True)
    return 0


def _cli(*args: str) -> tuple[str, ...]:
    return (sys.executable, "-m", "src.cli", *args)


def _cmd(module: str, *args: str) -> tuple[str, ...]:
    return (sys.executable, "-m", module, *args)


def _default_run_log(root: Path, now: Callable[[], datetime] | None) -> Path:
    stamp = (now or datetime.now)().strftime("%Y%m%d_%H%M%S")
    return root / "logs" / f"run_update_daily_{stamp}.log"


def _run_subprocess(step: DailyStep, log_path: Path, root: Path) -> int:
    with log_path.open("a", encoding="utf-8") as log:
        try:
            completed = subprocess.run(
                step.command,
                cwd=root,
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
                timeout=step.timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            return TIMEOUT_EXIT_CODE
    return int(completed.returncode)


def _read_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"runs": {}}
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise StateFileError(f"Daily update state file is corrupt: {path}. Re-run with --force to reset it.") from exc
    if not isinstance(state, dict):
        raise StateFileError(f"Daily update state file is invalid: {path}. Re-run with --force to reset it.")
    return state


def _write_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _record_step(
    step_state: dict[str, Any],
    step: DailyStep,
    status: str,
    exit_code: int | None,
    log_path: Path,
    now: Callable[[], datetime] | None,
) -> None:
    previous = step_state.get(step.id, {})
    started_at = previous.get("started_at") if status != "running" else _timestamp(now)
    step_state[step.id] = {
        "status": status,
        "command": step.command_text,
        "started_at": started_at,
        "ended_at": None if status == "running" else _timestamp(now),
        "exit_code": exit_code,
        "log_path": str(log_path),
    }


def _append_log(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(text)


def _emit(
    log_path: Path,
    now: Callable[[], datetime] | None,
    message: str,
    *,
    console: bool,
) -> None:
    _append_log(log_path, f"[{_timestamp(now)}] {message}\n")
    if console:
        print(f"[{_time_of_day(now)}] {message}", flush=True)


def _timestamp(now: Callable[[], datetime] | None) -> str:
    return (now or datetime.now)().strftime("%Y-%m-%d %H:%M:%S")


def _time_of_day(now: Callable[[], datetime] | None) -> str:
    return (now or datetime.now)().strftime("%H:%M:%S")
