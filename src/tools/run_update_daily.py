"""Resumable daily update orchestrator."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, TextIO

import yaml

from src.pipeline.common import (
    baostock_cn_trading_calendar_covers_range,
    baostock_cn_trading_calendar_fetch_start,
    date_iso,
    default_candidate_date,
    latest_trading_day_on_or_before,
)
from src.storage.metadata_store import default_metadata_duckdb_file
from src.storage.parquet_store import ParquetStore
from src.utils import paths
from src.utils.config_mgr import ConfigError, ConfigManager
from src.utils.process_lock import ProcessLockError, acquire_process_lock, is_pid_alive


class StateFileError(RuntimeError):
    """Raised when the daily update state file cannot be read safely."""


class RunDailyUpdateLockError(RuntimeError):
    """Raised when another run-update-daily process owns the global lock."""


class DailyWorkflowConfigError(ValueError):
    """Raised when the daily workflow configuration is invalid."""


@dataclass(frozen=True)
class DailyEffectiveDates:
    natural_date: date
    candidate_date: date
    market_date: date
    hist_start: date
    market_date_overridden: bool = False


@dataclass(frozen=True)
class DailyDependency:
    step_id: str
    state_key_policy: str | None = None


@dataclass(frozen=True)
class DailyStep:
    id: str
    name: str
    command: tuple[str, ...]
    optional: bool = False
    timeout_seconds: int | None = None
    depends_on: tuple[DailyDependency | str, ...] = ()
    schedule_policy: str = "daily"
    state_key_policy: str = "natural_date"
    resume_policy: str = "skip_if_success"
    data_freshness_policy: str = "natural_daily"

    @property
    def command_text(self) -> str:
        return " ".join(self.command)


CommandRunner = Callable[[DailyStep, Path], int]
SCHEDULE_POLICIES = {"daily", "market_window", "legacy_when"}
STATE_KEY_POLICIES = {"natural_date", "market_date", "run_instance"}
RESUME_POLICIES = {"skip_if_success", "always_run"}
DATA_FRESHNESS_POLICIES = {"market_session", "natural_daily", "disclosure_calendar", "maintenance"}
FAILED_DEPENDENCY_STATUSES = {"failed", "failed_resource_locked", "failed_timeout_cleanup", "blocked", "abandoned"}
SATISFIED_DEPENDENCY_STATUSES = {"success", "skipped", "skipped_checkpoint"}
TIMEOUT_EXIT_CODE = 124
TIMEOUT_CLEANUP_FAILED_EXIT_CODE = 125
PROCESS_CLEANUP_WAIT_SECONDS = 30
RUNNING_ABANDONED_AFTER_SECONDS = 24 * 60 * 60
RUN_UPDATE_DAILY_LOCK_STALE_AFTER_SECONDS = 24 * 60 * 60
DAILY_WORKFLOW_CONFIG = "daily_workflow.yaml"
_LAST_LOCKED_DUCKDB_PATHS: tuple[Path, ...] = ()
# Kept only as a test/migration reference for the legacy workflow shape.
# Production execution must load config/daily_workflow.yaml and must not
# silently fall back to this in-memory default.
DEFAULT_DAILY_WORKFLOW_CONFIG: dict[str, object] = {
    "steps": [
        {
            "id": "cleanup",
            "name": "cleanup expired logs",
            "command": ["{python}", "-m", "src.tools.log_cleanup", "--retention-days", "30"],
            "optional": True,
        },
        {
            "id": "calendar",
            "name": "update-baostock-daily calendar",
            "command": [
                "{qdc}",
                "update-baostock-daily",
                "--dataset",
                "baostock_cn_trading_calendar",
                "--no-build-duckdb-views",
            ],
        },
        {
            "id": "akshare-delist",
            "name": "akshare update delist",
            "when": ["friday_to_sunday"],
            "command": ["{qdc}", "akshare", "update", "--target", "delist", "--no-build-duckdb-views"],
        },
        {
            "id": "akshare-spot-quote",
            "name": "akshare update spot_quote",
            "command": ["{qdc}", "akshare", "update", "--target", "spot_quote", "--no-build-duckdb-views"],
            "optional": True,
        },
        {
            "id": "baostock-unadjusted",
            "name": "update-baostock-daily unadjusted",
            "command": [
                "{qdc}",
                "update-baostock-daily",
                "--dataset",
                "baostock_cn_stock_daily_bar_unadjusted",
                "--no-build-duckdb-views",
            ],
        },
        {
            "id": "baostock-basic",
            "name": "update-baostock-daily stock basic",
            "command": [
                "{qdc}",
                "update-baostock-daily",
                "--dataset",
                "baostock_cn_stock_basic",
                "--no-build-duckdb-views",
            ],
        },
        {
            "id": "baostock-valuation-percentile",
            "name": "update-baostock-valuation-percentile",
            "command": ["{qdc}", "update-baostock-valuation-percentile", "--no-build-duckdb-views"],
            "depends_on": ["baostock-unadjusted"],
        },
        {
            "id": "akshare-yjyg-em",
            "name": "akshare update yjyg_em incremental",
            "command": [
                "{qdc}",
                "akshare",
                "update",
                "--target",
                "yjyg_em",
                "--mode",
                "incremental",
                "--no-build-duckdb-views",
            ],
            "optional": True,
            "timeout_seconds": 900,
        },
        {
            "id": "baostock-adjustment-factor",
            "name": "update-baostock-daily adjustment factor",
            "when": ["friday_to_sunday"],
            "command": [
                "{qdc}",
                "update-baostock-daily",
                "--dataset",
                "baostock_cn_stock_adjustment_factor",
                "--no-build-duckdb-views",
            ],
        },
        {
            "id": "baostock-qfq",
            "name": "update-baostock-daily qfq",
            "when": ["friday_to_sunday"],
            "command": [
                "{qdc}",
                "update-baostock-daily",
                "--dataset",
                "baostock_cn_stock_daily_bar_qfq",
                "--no-build-duckdb-views",
            ],
            "depends_on": ["baostock-unadjusted", "baostock-adjustment-factor"],
        },
        {
            "id": "baostock-hfq",
            "name": "update-baostock-daily hfq",
            "when": ["friday_to_sunday"],
            "command": [
                "{qdc}",
                "update-baostock-daily",
                "--dataset",
                "baostock_cn_stock_daily_bar_hfq",
                "--no-build-duckdb-views",
            ],
            "depends_on": ["baostock-unadjusted", "baostock-adjustment-factor"],
        },
        {
            "id": "akshare-valuation-full",
            "name": "akshare update valuation full",
            "when": ["friday_to_sunday"],
            "command": [
                "{qdc}",
                "akshare",
                "update",
                "--target",
                "valuation",
                "--mode",
                "full",
                "--no-build-duckdb-views",
            ],
        },
        {
            "id": "akshare-report-disclosure",
            "name": "akshare update report_disclosure",
            "when": ["friday_to_sunday"],
            "command": ["{qdc}", "akshare", "update", "--target", "report_disclosure", "--no-build-duckdb-views"],
        },
        {
            "id": "akshare-yysj-em",
            "name": "akshare update yysj_em",
            "when": ["friday_to_sunday"],
            "command": ["{qdc}", "akshare", "update", "--target", "yysj_em", "--no-build-duckdb-views"],
        },
        {
            "id": "akshare-daily-bar",
            "name": "akshare update daily_bar incremental all from {hist_start}",
            "when": ["friday_to_sunday"],
            "command": [
                "{qdc}",
                "akshare",
                "update",
                "--target",
                "daily_bar",
                "--mode",
                "incremental",
                "--adjustment",
                "all",
                "--start",
                "{hist_start}",
                "--no-build-duckdb-views",
            ],
        },
        {
            "id": "sync-qlib",
            "name": "sync-qlib",
            "when": ["friday_to_sunday"],
            "command": ["{qdc}", "sync-qlib", "--no-build-duckdb-views", "--max-runtime-seconds", "7200"],
        },
        {
            "id": "financial-report",
            "name": "akshare update financial_report incremental",
            "command": [
                "{qdc}",
                "akshare",
                "update",
                "--target",
                "financial_report",
                "--mode",
                "incremental",
                "--no-build-duckdb-views",
            ],
        },
        {
            "id": "build-derived",
            "name": "build canonical derived datasets",
            "command": [
                "{qdc}",
                "build-derived",
                "--target",
                "all",
                "--mode",
                "incremental",
                "--no-build-duckdb-views",
            ],
            "depends_on": [
                "akshare-spot-quote",
                "baostock-unadjusted",
                "baostock-basic",
                "baostock-valuation-percentile",
                "financial-report",
                "akshare-delist",
                "baostock-adjustment-factor",
                "baostock-qfq",
                "baostock-hfq",
                "akshare-valuation-full",
                "akshare-daily-bar",
                "sync-qlib",
            ],
        },
        {
            "id": "build-duckdb-views",
            "name": "build-duckdb-views",
            "command": ["{qdc}", "build-duckdb-views"],
            "depends_on": ["build-derived"],
        },
    ]
}


def daily_steps(
    today: date | None = None,
    root: Path | None = None,
    effective_dates: DailyEffectiveDates | None = None,
) -> list[DailyStep]:
    resolved_today = today or date.today()
    resolved_effective_dates = effective_dates or _calendar_free_effective_dates(resolved_today)
    config = _load_daily_workflow_config((root or paths.ROOT).resolve())
    return _steps_from_workflow_config(config, resolved_effective_dates)


def _daily_steps_for_root(effective_dates: DailyEffectiveDates, root: Path) -> list[DailyStep]:
    try:
        return daily_steps(today=effective_dates.natural_date, root=root, effective_dates=effective_dates)
    except TypeError:
        return daily_steps(effective_dates.natural_date)


def _load_daily_workflow_config(root: Path) -> dict[str, object]:
    path = root / "config" / DAILY_WORKFLOW_CONFIG
    if not path.exists():
        raise DailyWorkflowConfigError(
            f"Daily workflow config is missing: {path}. "
            f"Restore or create config/{DAILY_WORKFLOW_CONFIG} before running the daily workflow."
        )
    try:
        loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise DailyWorkflowConfigError(f"Invalid daily workflow YAML: {path}: {exc}") from exc
    if not isinstance(loaded, dict):
        raise DailyWorkflowConfigError(f"Daily workflow config root must be a mapping: {path}")
    return loaded


def _steps_from_workflow_config(config: dict[str, object], effective_dates: DailyEffectiveDates) -> list[DailyStep]:
    raw_steps = config.get("steps")
    if not isinstance(raw_steps, list):
        raise DailyWorkflowConfigError("Daily workflow config missing required list field: steps")

    context = _workflow_context(effective_dates)
    steps: list[DailyStep] = []
    for index, raw_step in enumerate(raw_steps):
        if not isinstance(raw_step, dict):
            raise DailyWorkflowConfigError(f"Daily workflow step #{index + 1} must be a mapping")
        if not bool(raw_step.get("enabled", True)):
            continue
        schedule_policy = _enum_value(
            raw_step,
            "schedule_policy",
            "legacy_when" if "when" in raw_step else "daily",
            SCHEDULE_POLICIES,
            index,
        )
        if not _schedule_policy_matches(schedule_policy, raw_step.get("when", ["all"]), effective_dates):
            continue
        steps.append(_daily_step_from_config(raw_step, context, index))

    step_ids = {step.id for step in steps}
    return [
        DailyStep(
            id=step.id,
            name=step.name,
            command=step.command,
            optional=step.optional,
            timeout_seconds=step.timeout_seconds,
            depends_on=tuple(
                dependency for dependency in step.depends_on if _dependency_step_id(dependency) in step_ids
            ),
            schedule_policy=step.schedule_policy,
            state_key_policy=step.state_key_policy,
            resume_policy=step.resume_policy,
            data_freshness_policy=step.data_freshness_policy,
        )
        for step in steps
    ]


def _calendar_free_effective_dates(today: date) -> DailyEffectiveDates:
    return DailyEffectiveDates(
        natural_date=today,
        candidate_date=today,
        market_date=today,
        hist_start=today - timedelta(days=30),
    )


def _workflow_context(effective_dates: DailyEffectiveDates) -> dict[str, str]:
    return {
        "python": sys.executable,
        "today": effective_dates.natural_date.isoformat(),
        "natural_date": effective_dates.natural_date.isoformat(),
        "candidate_date": effective_dates.candidate_date.isoformat(),
        "market_date": effective_dates.market_date.isoformat(),
        "hist_start": effective_dates.hist_start.isoformat(),
    }


def _daily_step_from_config(raw_step: dict[str, object], context: dict[str, str], index: int) -> DailyStep:
    step_id = _required_string(raw_step, "id", index)
    name = _render_text(str(raw_step.get("name", step_id)), context)
    command = _render_command(raw_step.get("command"), context, step_id)
    optional = bool(raw_step.get("optional", False))
    timeout_seconds = _optional_timeout(raw_step.get("timeout_seconds"), step_id)
    depends_on = _dependencies(raw_step.get("depends_on", []), step_id)
    schedule_policy = _enum_value(
        raw_step,
        "schedule_policy",
        "legacy_when" if "when" in raw_step else "daily",
        SCHEDULE_POLICIES,
        index,
    )
    state_key_policy = _enum_value(raw_step, "state_key_policy", "natural_date", STATE_KEY_POLICIES, index)
    resume_policy = _enum_value(raw_step, "resume_policy", "skip_if_success", RESUME_POLICIES, index)
    data_freshness_policy = _enum_value(
        raw_step,
        "data_freshness_policy",
        "natural_daily",
        DATA_FRESHNESS_POLICIES,
        index,
    )
    return DailyStep(
        id=step_id,
        name=name,
        command=command,
        optional=optional,
        timeout_seconds=timeout_seconds,
        depends_on=tuple(depends_on),
        schedule_policy=schedule_policy,
        state_key_policy=state_key_policy,
        resume_policy=resume_policy,
        data_freshness_policy=data_freshness_policy,
    )


def _required_string(raw_step: dict[str, object], field: str, index: int) -> str:
    value = raw_step.get(field)
    if not isinstance(value, str) or not value.strip():
        raise DailyWorkflowConfigError(f"Daily workflow step #{index + 1} missing required string field: {field}")
    return value.strip()


def _optional_timeout(value: object, step_id: str) -> int | None:
    if value is None:
        return None
    if not isinstance(value, str | int):
        raise DailyWorkflowConfigError(f"Daily workflow step {step_id} has invalid timeout_seconds: {value!r}")
    try:
        timeout = int(value)
    except (TypeError, ValueError) as exc:
        raise DailyWorkflowConfigError(f"Daily workflow step {step_id} has invalid timeout_seconds: {value!r}") from exc
    if timeout <= 0:
        raise DailyWorkflowConfigError(f"Daily workflow step {step_id} timeout_seconds must be positive")
    return timeout


def _enum_value(
    raw_step: dict[str, object],
    field: str,
    default: str,
    allowed: set[str],
    index: int,
) -> str:
    value = raw_step.get(field, default)
    step_label = str(raw_step.get("id", f"#{index + 1}"))
    if not isinstance(value, str) or value.strip() not in allowed:
        allowed_text = ", ".join(sorted(allowed))
        raise DailyWorkflowConfigError(
            f"Daily workflow step {step_label} has invalid {field}: {value!r}; allowed: {allowed_text}"
        )
    return value.strip()


def _dependencies(value: object, step_id: str) -> list[DailyDependency]:
    if value is None:
        return []
    if isinstance(value, str):
        return [DailyDependency(value.strip())]
    if not isinstance(value, list):
        raise DailyWorkflowConfigError(
            f"Daily workflow field {step_id}.depends_on must be a string, list of strings, or list of mappings"
        )
    output: list[DailyDependency] = []
    for item in value:
        if isinstance(item, str):
            if not item.strip():
                raise DailyWorkflowConfigError(f"Daily workflow field {step_id}.depends_on contains an empty step id")
            output.append(DailyDependency(item.strip()))
            continue
        if isinstance(item, dict):
            raw_dependency_step = item.get("step")
            if not isinstance(raw_dependency_step, str) or not raw_dependency_step.strip():
                raise DailyWorkflowConfigError(
                    f"Daily workflow field {step_id}.depends_on mapping missing required string field: step"
                )
            raw_policy = item.get("state_key_policy")
            if raw_policy is not None and (
                not isinstance(raw_policy, str) or raw_policy.strip() not in STATE_KEY_POLICIES
            ):
                allowed_text = ", ".join(sorted(STATE_KEY_POLICIES))
                raise DailyWorkflowConfigError(
                    f"Daily workflow dependency {step_id}.{raw_dependency_step} has invalid "
                    f"state_key_policy: {raw_policy!r}; allowed: {allowed_text}"
                )
            output.append(
                DailyDependency(
                    raw_dependency_step.strip(),
                    raw_policy.strip() if isinstance(raw_policy, str) else None,
                )
            )
            continue
        raise DailyWorkflowConfigError(
            f"Daily workflow field {step_id}.depends_on must contain only strings or mappings"
        )
    return output


def _dependency_step_id(dependency: DailyDependency | str) -> str:
    return dependency.step_id if isinstance(dependency, DailyDependency) else str(dependency)


def _string_list(value: object, field_name: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if not isinstance(value, list):
        raise DailyWorkflowConfigError(f"Daily workflow field {field_name} must be a string or list of strings")
    output: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise DailyWorkflowConfigError(f"Daily workflow field {field_name} must contain only strings")
        output.append(item.strip())
    return output


def _render_command(command: object, context: dict[str, str], step_id: str) -> tuple[str, ...]:
    if isinstance(command, str):
        command_items = [command]
    elif isinstance(command, list):
        command_items = command
    else:
        raise DailyWorkflowConfigError(f"Daily workflow step {step_id} missing required command string/list")

    rendered: list[str] = []
    for item in command_items:
        if not isinstance(item, str):
            raise DailyWorkflowConfigError(f"Daily workflow step {step_id} command must contain only strings")
        if item == "{qdc}":
            rendered.extend(_cli())
        else:
            rendered.append(_render_text(item, context))
    if not rendered:
        raise DailyWorkflowConfigError(f"Daily workflow step {step_id} command must not be empty")
    return tuple(rendered)


def _render_text(value: str, context: dict[str, str]) -> str:
    return value.format(**context)


def _day_rule_matches(raw_rules: object, today: date) -> bool:
    rules = _string_list(raw_rules, "when")
    if not rules or "all" in rules:
        return True
    weekday = today.weekday()
    for rule in rules:
        normalized = rule.strip().lower()
        if normalized == "weekday" and weekday < 5:
            return True
        if normalized == "weekend" and weekday in {5, 6}:
            return True
        if normalized in {"friday_to_sunday", "weekend_window"} and weekday in {4, 5, 6}:
            return True
        if normalized == today.strftime("%A").lower():
            return True
    return False


def resolve_daily_effective_dates(
    *,
    root: Path,
    today: date | None = None,
    now: Callable[[], datetime] | None = None,
    as_of_date: str | date | None = None,
    market_date: str | date | None = None,
) -> DailyEffectiveDates:
    natural_date = today or date.today()
    config = ConfigManager(root)
    try:
        if as_of_date is not None:
            candidate = _parse_iso_date(as_of_date, "as_of_date")
        else:
            candidate = _parse_iso_date(default_candidate_date(config, (now or datetime.now)()), "candidate_date")
    except ConfigError:
        candidate = _parse_iso_date(as_of_date, "as_of_date") if as_of_date is not None else natural_date

    if market_date is not None:
        resolved_market = _parse_iso_date(market_date, "market_date")
        if resolved_market > candidate:
            raise ValueError(
                f"market_date {resolved_market.isoformat()} must be on or before candidate_date {candidate.isoformat()}"
            )
        return DailyEffectiveDates(
            natural_date=natural_date,
            candidate_date=candidate,
            market_date=resolved_market,
            hist_start=resolved_market - timedelta(days=30),
            market_date_overridden=True,
        )

    if not (config.config_dir / "settings.yaml").exists():
        return DailyEffectiveDates(
            natural_date=natural_date,
            candidate_date=candidate,
            market_date=candidate,
            hist_start=candidate - timedelta(days=30),
        )

    store = ParquetStore(root=config.root)
    store.ensure_layout()
    try:
        calendar = store.read_dataset("baostock_cn_trading_calendar")
        if not baostock_cn_trading_calendar_covers_range(calendar, candidate, candidate):
            _refresh_baostock_cn_trading_calendar(config.root, candidate)
            calendar = store.read_dataset("baostock_cn_trading_calendar")
        if not baostock_cn_trading_calendar_covers_range(calendar, candidate, candidate):
            raise ValueError(
                "baostock_cn_trading_calendar does not cover candidate_date "
                f"{candidate.isoformat()} after preflight refresh"
            )
        resolved_market = _parse_iso_date(latest_trading_day_on_or_before(calendar, candidate), "market_date")
    finally:
        store.close()

    return DailyEffectiveDates(
        natural_date=natural_date,
        candidate_date=candidate,
        market_date=resolved_market,
        hist_start=resolved_market - timedelta(days=30),
    )


def _refresh_baostock_cn_trading_calendar(root: Path, candidate: date) -> None:
    from src.sources.baostock.update_daily import update_daily

    update_daily(
        dataset="baostock_cn_trading_calendar",
        start=baostock_cn_trading_calendar_fetch_start(candidate.isoformat()),
        end=candidate.isoformat(),
        root=root,
        build_views=False,
        resume=True,
        force=False,
    )


def _parse_iso_date(value: str | date, field_name: str) -> date:
    try:
        return datetime.strptime(date_iso(value), "%Y-%m-%d").date()
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid {field_name}: {value!r}; expected YYYY-MM-DD") from exc


def _schedule_policy_matches(
    schedule_policy: str,
    raw_rules: object,
    effective_dates: DailyEffectiveDates,
) -> bool:
    if schedule_policy == "daily":
        return True
    if schedule_policy == "market_window":
        return (
            effective_dates.natural_date.weekday() in {4, 5, 6}
            or effective_dates.candidate_date != effective_dates.market_date
            or effective_dates.market_date_overridden
        )
    if schedule_policy == "legacy_when":
        return _day_rule_matches(raw_rules, effective_dates.natural_date)
    raise DailyWorkflowConfigError(f"Unsupported schedule_policy: {schedule_policy}")


def run_daily_update(
    *,
    root: Path | None = None,
    state_file: Path | None = None,
    run_log: Path | None = None,
    today: date | None = None,
    now: Callable[[], datetime] | None = None,
    force: bool = False,
    ignore_state: bool = False,
    start_at: str | None = None,
    as_of_date: str | date | None = None,
    market_date: str | date | None = None,
    command_runner: CommandRunner | None = None,
) -> int:
    base = (root or Path.cwd()).resolve()
    effective_dates = resolve_daily_effective_dates(
        root=base,
        today=today,
        now=now,
        as_of_date=as_of_date,
        market_date=market_date,
    )
    run_instance_key = f"run_instance:{(now or datetime.now)().strftime('%Y%m%d_%H%M%S')}"
    resolved_state_file = state_file or base / "data" / "metadata" / "run_update_daily_state.json"
    resolved_log = run_log or _default_run_log(base, now)
    runner = command_runner or (lambda step, log_path: _run_subprocess(step, log_path, base))
    steps = _daily_steps_for_root(effective_dates, base)
    steps_by_id = {step.id: step for step in steps}
    step_ids = [step.id for step in steps]

    if start_at is not None and start_at not in step_ids:
        raise ValueError(f"Unknown daily update step id: {start_at}")

    lock_dir = base / "data" / "metadata" / "locks" / "run-update-daily.lock"
    lock_cm = acquire_process_lock(
        lock_dir,
        lock_name="run-update-daily",
        purpose="run-update-daily",
        stale_after_seconds=RUN_UPDATE_DAILY_LOCK_STALE_AFTER_SECONDS,
        extra_owner={
            "natural_date": effective_dates.natural_date.isoformat(),
            "candidate_date": effective_dates.candidate_date.isoformat(),
            "market_date": effective_dates.market_date.isoformat(),
        },
    )
    try:
        active_lock = lock_cm.__enter__()
    except ProcessLockError as exc:
        raise RunDailyUpdateLockError(f"run-update-daily is already running; {exc}") from exc

    exc_info: tuple[type[BaseException] | None, BaseException | None, object | None] = (None, None, None)
    try:
        state = _read_state_for_run(resolved_state_file, reset_if_corrupt=force)
        state_needs_write = state.get("version") != 2
        state["version"] = 2
        state.setdefault("runs", {})
        resolved_log.parent.mkdir(parents=True, exist_ok=True)
        if _mark_abandoned_running_steps_by_key(
            steps,
            state,
            effective_dates,
            run_instance_key,
            resolved_log,
            now,
            active_lock.owner,
        ):
            state_needs_write = False
            _write_state(resolved_state_file, state)

        start_seen = start_at is None
        failed_exit_code: int | None = None
        for step in steps:
            step_state = _step_state_for_step(state, step, effective_dates, run_instance_key)
            if not start_seen:
                if step.id == start_at:
                    start_seen = True
                else:
                    _record_step(step_state, step, "skipped", 0, resolved_log, now)
                    _emit(resolved_log, now, f"Skipped {step.id} before start-at {start_at}", console=True)
                    continue

            current_status = str(
                _step_row_for_policy(
                    state,
                    step.id,
                    step.state_key_policy,
                    effective_dates,
                    run_instance_key,
                ).get("status", "pending")
            )
            should_skip_success = (
                start_at is None
                and not force
                and not ignore_state
                and step.resume_policy == "skip_if_success"
                and current_status == "success"
            )
            if should_skip_success:
                _emit(resolved_log, now, f"Skipped {step.id}; already successful", console=True)
                continue

            blocked_by = _blocked_dependencies(
                step,
                state,
                steps_by_id=steps_by_id,
                effective_dates=effective_dates,
                run_instance_key=run_instance_key,
            )
            if blocked_by:
                _record_step(step_state, step, "blocked", 1, resolved_log, now, blocked_by=blocked_by)
                _write_state(resolved_state_file, state)
                _emit(
                    resolved_log,
                    now,
                    f"Blocked {step.id}; dependency failed: {', '.join(blocked_by)}",
                    console=True,
                )
                continue

            _record_step(step_state, step, "running", None, resolved_log, now)
            _write_state(resolved_state_file, state)
            _emit(resolved_log, now, f"Running {step.id} ({step.name})... log={resolved_log}", console=True)
            _emit(resolved_log, now, f"Command: {step.command_text}", console=False)

            exit_code = int(runner(step, resolved_log))
            if exit_code != 0:
                timed_out = exit_code == TIMEOUT_EXIT_CODE and step.timeout_seconds is not None
                timeout_cleanup_failed = (
                    exit_code == TIMEOUT_CLEANUP_FAILED_EXIT_CODE and step.timeout_seconds is not None
                )
                if timed_out or timeout_cleanup_failed:
                    if timeout_cleanup_failed:
                        if step.optional:
                            _record_step(
                                step_state,
                                step,
                                "failed_timeout_cleanup",
                                exit_code,
                                resolved_log,
                                now,
                            )
                        else:
                            _record_step(step_state, step, "failed", exit_code, resolved_log, now)
                        _write_state(resolved_state_file, state)
                        _emit(
                            resolved_log,
                            now,
                            f"{step.name} timed out and process tree cleanup failed; stopping",
                            console=True,
                        )
                        failed_exit_code = failed_exit_code or exit_code
                        break

                    if not _wait_for_duckdb_available(base):
                        locked_paths = _LAST_LOCKED_DUCKDB_PATHS
                        locked_text = ", ".join(str(path) for path in locked_paths) or "unknown DuckDB file"
                        if step.optional:
                            _record_step(
                                step_state,
                                step,
                                "failed_resource_locked",
                                exit_code,
                                resolved_log,
                                now,
                            )
                        else:
                            _record_step(step_state, step, "failed", exit_code, resolved_log, now)
                        _write_state(resolved_state_file, state)
                        _emit(
                            resolved_log,
                            now,
                            f"{step.name} timed out and DuckDB is still locked ({locked_text}); stopping",
                            console=True,
                        )
                        failed_exit_code = failed_exit_code or exit_code
                        break

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
                if failed_exit_code is None:
                    failed_exit_code = exit_code
                continue

            _emit(resolved_log, now, f"Completed {step.id} ({step.name})", console=True)
            _record_step(step_state, step, "success", 0, resolved_log, now)
            _write_state(resolved_state_file, state)

        final_exit_code = _final_exit_code_for_state(
            steps,
            state,
            effective_dates,
            run_instance_key,
            failed_exit_code,
        )
        if final_exit_code == 0:
            _emit(resolved_log, now, "All updates completed successfully", console=True)
        else:
            _emit(resolved_log, now, f"Daily update completed with failures; exit code {final_exit_code}", console=True)
        if state_needs_write:
            _write_state(resolved_state_file, state)
        return final_exit_code
    except BaseException:
        exc_info = sys.exc_info()
        raise
    finally:
        lock_cm.__exit__(*exc_info)


def _cli(*args: str) -> tuple[str, ...]:
    return (sys.executable, "-m", "src.cli", *args)


def _cmd(module: str, *args: str) -> tuple[str, ...]:
    return (sys.executable, "-m", module, *args)


def _default_run_log(root: Path, now: Callable[[], datetime] | None) -> Path:
    stamp = (now or datetime.now)().strftime("%Y%m%d_%H%M%S")
    return root / "logs" / f"run_update_daily_{stamp}.log"


def _run_subprocess(step: DailyStep, log_path: Path, root: Path) -> int:
    env = {**os.environ, "QDC_DISABLE_FILE_LOG": "1"}
    with log_path.open("a", encoding="utf-8") as log:
        popen_kwargs: dict[str, Any] = {
            "cwd": root,
            "stdout": log,
            "stderr": subprocess.STDOUT,
            "env": env,
            "text": True,
        }
        if os.name != "nt":
            popen_kwargs["start_new_session"] = True
        proc = subprocess.Popen(step.command, **popen_kwargs)
        try:
            return int(proc.wait(timeout=step.timeout_seconds))
        except subprocess.TimeoutExpired:
            log.write(f"Step timed out after {step.timeout_seconds} seconds; terminating process tree\n")
            if _terminate_process_tree(proc, log):
                return TIMEOUT_EXIT_CODE
            log.write("Process tree cleanup failed after timeout\n")
            return TIMEOUT_CLEANUP_FAILED_EXIT_CODE


def _terminate_process_tree(proc: subprocess.Popen[Any], log: TextIO) -> bool:
    if proc.poll() is not None:
        return True

    if os.name == "nt":
        try:
            completed = subprocess.run(
                ("taskkill", "/PID", str(proc.pid), "/T", "/F"),
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
        except OSError as exc:
            log.write(f"Failed to run taskkill for timed out process {proc.pid}: {exc}\n")
            return False
        if completed.returncode != 0 and proc.poll() is None:
            log.write(f"taskkill failed for timed out process {proc.pid}: exit code {completed.returncode}\n")
            return False
    else:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            return True
        except OSError as exc:
            log.write(f"Failed to terminate process group {proc.pid}: {exc}\n")
            return False

    try:
        proc.wait(timeout=PROCESS_CLEANUP_WAIT_SECONDS)
        return True
    except subprocess.TimeoutExpired:
        if os.name != "nt":
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                return True
            except OSError as exc:
                log.write(f"Failed to kill process group {proc.pid}: {exc}\n")
                return False
            try:
                proc.wait(timeout=PROCESS_CLEANUP_WAIT_SECONDS)
                return True
            except subprocess.TimeoutExpired:
                pass
        log.write(f"Timed out waiting for process tree {proc.pid} to exit after cleanup\n")
        return False


def _wait_for_duckdb_available(root: Path, timeout_seconds: int = 30) -> bool:
    global _LAST_LOCKED_DUCKDB_PATHS
    db_paths = _duckdb_files_to_check(root)
    _LAST_LOCKED_DUCKDB_PATHS = ()
    existing_paths = tuple(path for path in db_paths if path.exists())
    if not existing_paths:
        return True

    deadline = time.monotonic() + timeout_seconds
    while True:
        locked = _locked_duckdb_files(existing_paths)
        if not locked:
            _LAST_LOCKED_DUCKDB_PATHS = ()
            return True
        _LAST_LOCKED_DUCKDB_PATHS = locked
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.5)


def _duckdb_files_to_check(root: Path) -> tuple[Path, ...]:
    return (
        (root / "data" / "duckdb" / "quant.duckdb").resolve(),
        default_metadata_duckdb_file(root).resolve(),
    )


def _locked_duckdb_files(paths: tuple[Path, ...]) -> tuple[Path, ...]:
    locked: list[Path] = []
    for path in paths:
        try:
            import duckdb

            connection = duckdb.connect(str(path))
            connection.close()
        except Exception:
            locked.append(path)
    return tuple(locked)


def _read_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 2, "runs": {}}
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise StateFileError(f"Daily update state file is corrupt: {path}. Re-run with --force to reset it.") from exc
    if not isinstance(state, dict):
        raise StateFileError(f"Daily update state file is invalid: {path}. Re-run with --force to reset it.")
    if not isinstance(state.get("runs", {}), dict):
        raise StateFileError(f"Daily update state file is invalid: {path}. Re-run with --force to reset it.")
    return state


def _read_state_for_run(path: Path, *, reset_if_corrupt: bool) -> dict[str, Any]:
    try:
        return _read_state(path)
    except StateFileError:
        if not reset_if_corrupt:
            raise
        return {"version": 2, "runs": {}}


def _write_state(path: Path, state: dict[str, Any]) -> None:
    state["version"] = 2
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with tmp_path.open("w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            with suppress(OSError):
                tmp_path.unlink()


def _state_key_for_policy(
    policy: str,
    effective_dates: DailyEffectiveDates,
    run_instance_key: str,
) -> str:
    if policy == "natural_date":
        return f"natural_date:{effective_dates.natural_date.isoformat()}"
    if policy == "market_date":
        return f"market_date:{effective_dates.market_date.isoformat()}"
    if policy == "run_instance":
        return run_instance_key
    raise DailyWorkflowConfigError(f"Unsupported state_key_policy: {policy}")


def _state_key_for_step(step: DailyStep, effective_dates: DailyEffectiveDates, run_instance_key: str) -> str:
    return _state_key_for_policy(step.state_key_policy, effective_dates, run_instance_key)


def _run_state_for_key(
    state: dict[str, Any],
    state_key: str,
    *,
    create: bool,
) -> dict[str, Any]:
    runs = state.setdefault("runs", {})
    if not isinstance(runs, dict):
        raise StateFileError("Daily update state file is invalid: runs must be a mapping")
    run_state = runs.get(state_key)
    if run_state is None and not create and state_key.startswith("natural_date:"):
        legacy_key = state_key.split(":", 1)[1]
        run_state = runs.get(legacy_key)
    if run_state is None:
        if not create:
            return {"steps": {}}
        run_state = {"steps": {}}
        runs[state_key] = run_state
    if not isinstance(run_state, dict):
        if not create:
            return {"steps": {}}
        run_state = {"steps": {}}
        runs[state_key] = run_state
    run_state.setdefault("steps", {})
    if not isinstance(run_state["steps"], dict):
        if not create:
            return {"steps": {}}
        run_state["steps"] = {}
    return run_state


def _step_state_for_step(
    state: dict[str, Any],
    step: DailyStep,
    effective_dates: DailyEffectiveDates,
    run_instance_key: str,
) -> dict[str, Any]:
    state_key = _state_key_for_step(step, effective_dates, run_instance_key)
    return _run_state_for_key(state, state_key, create=True).setdefault("steps", {})


def _step_row_for_policy(
    state: dict[str, Any],
    step_id: str,
    policy: str,
    effective_dates: DailyEffectiveDates,
    run_instance_key: str,
) -> dict[str, Any]:
    state_key = _state_key_for_policy(policy, effective_dates, run_instance_key)
    run_state = _run_state_for_key(state, state_key, create=False)
    steps = run_state.get("steps", {})
    if not isinstance(steps, dict):
        return {}
    row = steps.get(step_id, {})
    if isinstance(row, dict) and row:
        return row
    if policy == "natural_date":
        legacy_key = effective_dates.natural_date.isoformat()
        legacy_run_state = _run_state_for_key(state, legacy_key, create=False)
        legacy_steps = legacy_run_state.get("steps", {})
        if isinstance(legacy_steps, dict):
            legacy_row = legacy_steps.get(step_id, {})
            if isinstance(legacy_row, dict):
                return legacy_row
    return {}


def _mark_abandoned_running_steps_by_key(
    steps: list[DailyStep],
    state: dict[str, Any],
    effective_dates: DailyEffectiveDates,
    run_instance_key: str,
    log_path: Path,
    now: Callable[[], datetime] | None,
    active_lock_owner: dict[str, object],
) -> bool:
    changed = False
    seen_state_keys: set[str] = set()
    for step in steps:
        state_key = _state_key_for_step(step, effective_dates, run_instance_key)
        if state_key in seen_state_keys:
            continue
        seen_state_keys.add(state_key)
        step_state = _run_state_for_key(state, state_key, create=True).setdefault("steps", {})
        steps_for_key = [
            item for item in steps if _state_key_for_step(item, effective_dates, run_instance_key) == state_key
        ]
        changed = _mark_abandoned_running_steps(
            steps_for_key,
            step_state,
            log_path,
            now,
            active_lock_owner,
        ) or changed
    return changed


def _record_step(
    step_state: dict[str, Any],
    step: DailyStep,
    status: str,
    exit_code: int | None,
    log_path: Path,
    now: Callable[[], datetime] | None,
    *,
    blocked_by: tuple[str, ...] | list[str] | None = None,
    reason: str | None = None,
) -> None:
    previous = step_state.get(step.id, {})
    timestamp = _timestamp(now)
    started_at = previous.get("started_at") if status != "running" else timestamp
    row = {
        "status": status,
        "command": step.command_text,
        "started_at": started_at,
        "updated_at": timestamp,
        "ended_at": None if status == "running" else timestamp,
        "exit_code": exit_code,
        "log_path": str(log_path),
    }
    if status == "running":
        row["pid"] = os.getpid()
        row["orchestrator_pid"] = os.getpid()
    if blocked_by is not None:
        row["blocked_by"] = list(blocked_by)
    if reason is not None:
        row["reason"] = reason
    step_state[step.id] = row


def _mark_abandoned_running_steps(
    steps: list[DailyStep],
    step_state: dict[str, Any],
    log_path: Path,
    now: Callable[[], datetime] | None,
    active_lock_owner: dict[str, object],
) -> bool:
    changed = False
    for step in steps:
        current = step_state.get(step.id)
        if not isinstance(current, dict) or str(current.get("status")) != "running":
            continue
        reason = _running_abandoned_reason(current, now, active_lock_owner)
        if reason is None:
            continue
        _record_step(step_state, step, "abandoned", 1, log_path, now, reason=reason)
        changed = True
    return changed


def _running_abandoned_reason(
    row: dict[str, Any],
    now: Callable[[], datetime] | None,
    active_lock_owner: dict[str, object],
) -> str | None:
    pid = _int_or_none(row.get("orchestrator_pid", row.get("pid")))
    if pid is None:
        return "running step has no orchestrator pid"
    if not is_pid_alive(pid):
        return f"orchestrator pid {pid} is not alive"

    started_at = _parse_timestamp(row.get("started_at"))
    if started_at is None:
        return "running step has no valid started_at"
    current_time = (now or datetime.now)()
    if current_time - started_at <= timedelta(seconds=RUNNING_ABANDONED_AFTER_SECONDS):
        return None

    active_pid = _int_or_none(active_lock_owner.get("pid"))
    if active_pid == pid:
        return None
    return f"running step exceeded {RUNNING_ABANDONED_AFTER_SECONDS} seconds"


def _int_or_none(value: object) -> int | None:
    if value is None or not isinstance(value, str | int):
        return None
    try:
        resolved = int(value)
    except (TypeError, ValueError):
        return None
    return resolved if resolved > 0 else None


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _blocked_dependencies(
    step: DailyStep,
    state_or_step_state: dict[str, Any],
    *,
    steps_by_id: dict[str, DailyStep] | None = None,
    effective_dates: DailyEffectiveDates | None = None,
    run_instance_key: str | None = None,
) -> tuple[str, ...]:
    blocked: list[str] = []
    for dependency in step.depends_on:
        dependency_id = _dependency_step_id(dependency)
        if steps_by_id is None or effective_dates is None or run_instance_key is None:
            dependency_status = str(state_or_step_state.get(dependency_id, {}).get("status", "pending"))
        else:
            dependency_step = steps_by_id.get(dependency_id)
            dependency_policy = (
                dependency.state_key_policy
                if isinstance(dependency, DailyDependency) and dependency.state_key_policy is not None
                else dependency_step.state_key_policy
                if dependency_step is not None
                else "natural_date"
            )
            dependency_status = str(
                _step_row_for_policy(
                    state_or_step_state,
                    dependency_id,
                    dependency_policy,
                    effective_dates,
                    run_instance_key,
                ).get("status", "pending")
            )
        if dependency_status in FAILED_DEPENDENCY_STATUSES or dependency_status not in SATISFIED_DEPENDENCY_STATUSES:
            blocked.append(dependency_id)
    return tuple(blocked)


def _final_exit_code(steps: list[DailyStep], step_state: dict[str, Any], failed_exit_code: int | None) -> int:
    if failed_exit_code is not None:
        return failed_exit_code
    for step in steps:
        current = step_state.get(step.id, {})
        if str(current.get("status")) in {"failed", "failed_resource_locked", "failed_timeout_cleanup", "abandoned"}:
            raw_exit_code = current.get("exit_code")
            return int(raw_exit_code) if raw_exit_code is not None else 1
    for step in steps:
        if str(step_state.get(step.id, {}).get("status")) == "blocked":
            return 1
    return 0


def _final_exit_code_for_state(
    steps: list[DailyStep],
    state: dict[str, Any],
    effective_dates: DailyEffectiveDates,
    run_instance_key: str,
    failed_exit_code: int | None,
) -> int:
    step_state = {
        step.id: _step_row_for_policy(
            state,
            step.id,
            step.state_key_policy,
            effective_dates,
            run_instance_key,
        )
        for step in steps
    }
    return _final_exit_code(steps, step_state, failed_exit_code)


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
