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
from dataclasses import dataclass, replace
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
from src.pipeline.step_health import (
    FAILURE_STATUSES,
    SKIPPED_STATUSES,
    SUCCESS_STATUSES,
    DEGRADED_SUCCESS_STATUSES,
    StepHealthSummary,
    is_failure_status,
    read_step_health_summary,
)
from src.storage.metadata_store import default_metadata_duckdb_file
from src.storage.parquet_store import ParquetStore
from src.tools.run_logging import (
    RunLogContext,
    RunLogContextError,
    adopt_run_log_context,
    create_run_log_context,
)
from src.utils import paths
from src.utils.config_mgr import ConfigError, ConfigManager
from src.utils.logging import logger
from src.utils.network_policy import NETWORK_PROFILE_DIRECT, NETWORK_PROFILES, build_network_env
from src.utils.process_lock import ProcessLockError, acquire_process_lock, is_pid_alive

# Module-level scratch field updated by ``_run_subprocess`` immediately after
# ``Popen`` returns. ``run_daily_update`` reads it after the runner call to
# record the actual subprocess pid in the state file (in addition to the
# orchestrator pid). Tests that supply a synthetic ``command_runner`` do not
# touch this field, so ``child_pid`` stays ``None`` for them.
_LAST_CHILD_PID: int | None = None

# Critical derived datasets whose parquet existence authorizes a stale-aware
# DuckDB view rebuild when upstream steps failed. If ANY of these is missing,
# build-duckdb-views must not pretend to succeed.
CRITICAL_DERIVED_DATASETS_FOR_VIEWS: tuple[str, ...] = (
    "cn_security_master",
    "cn_stock_daily_bar",
    "cn_stock_valuation",
)
STEP_RESULT_PATH_ENV = "QDC_STEP_RESULT_PATH"


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
    soft: bool = False


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
    network_profile: str = NETWORK_PROFILE_DIRECT

    @property
    def command_text(self) -> str:
        return " ".join(self.command)


CommandRunner = Callable[[DailyStep, Path], int]
SCHEDULE_POLICIES = {"daily", "market_window", "legacy_when"}
STATE_KEY_POLICIES = {"natural_date", "market_date", "run_instance"}
RESUME_POLICIES = {"skip_if_success", "always_run"}
DATA_FRESHNESS_POLICIES = {"market_session", "natural_daily", "disclosure_calendar", "maintenance"}
# Dependency-status sets. These MUST stay aligned with the unified authority
# in :mod:`src.pipeline.step_health` (FAILURE_STATUSES / SKIPPED_STATUSES /
# SUCCESS_STATUSES). ``partial``, ``cancelled``, ``stalled`` and ``timed_out``
# are all fatal terminal statuses: a downstream step must NOT run when an
# upstream dependency ended in any of them.
FAILED_DEPENDENCY_STATUSES = {
    "failed",
    "failed_resource_locked",
    "failed_timeout_cleanup",
    "stalled",
    "timed_out",
    "partial",
    "cancelled",
    "blocked",
    "abandoned",
}
SATISFIED_DEPENDENCY_STATUSES = {"success", "success_degraded", "skipped", "skipped_checkpoint"}
TIMEOUT_EXIT_CODE = 124
TIMEOUT_CLEANUP_FAILED_EXIT_CODE = 125
STALLED_EXIT_CODE = 126  # Distinct from timed_out (124) so the state file can
                         # record ``stalled`` vs ``timed_out`` vs ``failed``.
PROCESS_CLEANUP_WAIT_SECONDS = 30
RUNNING_ABANDONED_AFTER_SECONDS = 24 * 60 * 60
RUN_UPDATE_DAILY_LOCK_STALE_AFTER_SECONDS = 24 * 60 * 60
DAILY_WORKFLOW_CONFIG = "daily_workflow.yaml"
LEGACY_START_AT_ALIASES = {"build-derived": "build-derived-security-master"}
# Derived build steps use stall detection instead of a fixed timeout.
# The orchestrator polls the child's progress state file every
# ``DERIVED_STALL_POLL_SECONDS`` and declares the build stalled when the
# heartbeat is older than ``DERIVED_STALL_HEARTBEAT_SECONDS`` AND
# ``processed`` is unchanged. A safety timeout of
# ``DERIVED_SAFETY_TIMEOUT_SECONDS`` (18h, well above historical P99)
# acts as a last-resort fallback.
DERIVED_STALL_POLL_SECONDS = 60
# Default heartbeat staleness threshold. The effective value is resolved from
# ``config/settings.yaml`` (``derived.stall_seconds``) via
# :func:`derived_runtime_config`; this constant is only the fallback when the
# config cannot be read (e.g. tests without a settings.yaml).
DERIVED_STALL_HEARTBEAT_SECONDS = 25 * 60
DERIVED_SAFETY_TIMEOUT_SECONDS = 18 * 60 * 60
# Only ``daily_bar`` is wired into the unified derived progress contract
# (BuildRunContext + journal heartbeat + StreamingBuildCoordinator +
# orchestrator stall detector). ``valuation`` does NOT use this contract yet:
# it lacks BuildRunContext, journal heartbeat, and the streaming coordinator,
# so the orchestrator must not pretend it does. If/when valuation is migrated,
# add it here in a separate change with its own progress path contract.
DERIVED_STALL_TARGETS = {"daily_bar"}
# Environment variables that form the single authoritative progress-path
# contract between the orchestrator and a derived build subprocess. The
# orchestrator generates a unique path per (run_instance, step_id) BEFORE
# spawning the child, passes it via env, and reads only that path — never a
# directory scan. The child reads it via
# :func:`src.sources.derived.stock_daily_bar._progress_path_override_from_env`.
QDC_DERIVED_PROGRESS_PATH_ENV = "QDC_DERIVED_PROGRESS_PATH"
QDC_DERIVED_RUN_ID_ENV = "QDC_DERIVED_RUN_ID"
# Environment variable that propagates the orchestrator's current run-id so
# that the cleanup subprocess can protect the in-flight run log from deletion.
QDC_ACTIVE_RUN_ID_ENV = "QDC_ACTIVE_RUN_ID"
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
            "id": "baostock-market-session",
            "name": "update-baostock-market-session",
            "schedule_policy": "daily",
            "state_key_policy": "market_date",
            "resume_policy": "always_run",
            "data_freshness_policy": "market_session",
            "command": [
                "{qdc}",
                "update-baostock-market-session",
                "--end",
                "{market_date}",
                "--natural-date",
                "{natural_date}",
                "--candidate-date",
                "{candidate_date}",
                "--market-date",
                "{market_date}",
                "--no-build-duckdb-views",
            ],
            "depends_on": ["baostock-basic"],
        },
        {
            "id": "baostock-valuation-percentile",
            "name": "update-baostock-valuation-percentile",
            "command": ["{qdc}", "update-baostock-valuation-percentile", "--no-build-duckdb-views"],
            "depends_on": ["baostock-market-session"],
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
            "network_profile": "inherit",
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
            "id": "build-derived-security-master",
            "name": "build derived security_master",
            "command": [
                "{qdc}",
                "build-derived",
                "--target",
                "security_master",
                "--mode",
                "incremental",
                "--no-build-duckdb-views",
            ],
            "depends_on": ["baostock-basic", "akshare-delist"],
        },
        {
            "id": "build-derived-daily-bar",
            "name": "build derived daily_bar",
            "command": [
                "{qdc}",
                "build-derived",
                "--target",
                "daily_bar",
                "--mode",
                "incremental",
                "--no-include-security-master",
                "--no-build-duckdb-views",
            ],
            "depends_on": [
                "build-derived-security-master",
                "akshare-spot-quote",
                "baostock-market-session",
                {"step": "akshare-daily-bar", "soft": True},
            ],
        },
        {
            "id": "build-derived-valuation",
            "name": "build derived valuation",
            "command": [
                "{qdc}",
                "build-derived",
                "--target",
                "valuation",
                "--mode",
                "incremental",
                "--no-include-security-master",
                "--no-build-duckdb-views",
            ],
            "depends_on": ["build-derived-security-master", "baostock-valuation-percentile", "akshare-valuation-full"],
        },
        {
            "id": "build-duckdb-views",
            "name": "build-duckdb-views",
            "command": ["{qdc}", "build-duckdb-views"],
            "depends_on": [
                "build-derived-security-master",
                "build-derived-daily-bar",
                "build-derived-valuation",
                {"step": "sync-qlib", "soft": True},
            ],
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
            network_profile=step.network_profile,
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
    network_profile = _enum_value(
        raw_step,
        "network_profile",
        NETWORK_PROFILE_DIRECT,
        NETWORK_PROFILES,
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
        network_profile=network_profile,
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
            raw_soft = item.get("soft")
            if raw_soft is not None and not isinstance(raw_soft, bool):
                raise DailyWorkflowConfigError(
                    f"Daily workflow dependency {step_id}.{raw_dependency_step} has invalid "
                    f"soft field: {raw_soft!r}; must be a boolean"
                )
            output.append(
                DailyDependency(
                    raw_dependency_step.strip(),
                    raw_policy.strip() if isinstance(raw_policy, str) else None,
                    bool(raw_soft) if raw_soft is not None else False,
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
    run_log_context = _resolve_run_log_context(base=base, run_log=run_log, now=now)
    resolved_log = run_log_context.path

    # ``_on_child_spawn`` is invoked by ``_run_subprocess`` immediately after
    # ``Popen`` returns, so the child pid lands in the state file *while* the
    # step is still running. Synthetic test runners do not call it, so
    # ``child_pid`` stays absent for them. The callback also records the
    # orchestrator-pinned derived progress path so crash recovery can find
    # the exact file the stall detector is reading.
    def _on_child_spawn(child_pid: int, step_id: str, step_state_view: dict[str, Any]) -> None:
        row = step_state_view.get(step_id)
        if isinstance(row, dict):
            row["child_pid"] = child_pid
            _write_state(resolved_state_file, state)

    def _default_runner(step: DailyStep, log_path: Path) -> int:
        return _run_subprocess(
            step,
            log_path,
            base,
            run_log_context=run_log_context,
            run_instance_key=run_instance_key,
            on_spawn=lambda pid: _on_child_spawn(pid, step.id, step_state),
        )

    runner = command_runner or _default_runner
    steps = _daily_steps_for_root(effective_dates, base)
    steps_by_id = {step.id: step for step in steps}
    step_ids = [step.id for step in steps]
    original_start_at = start_at
    mapped_start_at = LEGACY_START_AT_ALIASES.get(start_at)
    if start_at is not None and start_at not in step_ids and mapped_start_at in step_ids:
        start_at = mapped_start_at

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
            "run_id": run_log_context.run_id,
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
        # Record the per-run log context (run_id, path, log_status) so that
        # crash recovery and external monitors can tell whether the log is
        # still alive. ``log_status`` is recomputed on every state write via
        # ``RunLogContext.status_payload`` (which stats the file), so a missing
        # log is reported as ``"missing"`` instead of silently keeping a stale
        # path reference.
        state["run_log"] = run_log_context.status_payload(now=(now or datetime.now)())
        state["orchestrator_pid"] = os.getpid()
        resolved_log.parent.mkdir(parents=True, exist_ok=True)
        if original_start_at is not None and original_start_at != start_at:
            _emit(
                resolved_log,
                now,
                f"Mapped legacy start-at {original_start_at} to {start_at}",
                console=True,
            )
        if _mark_abandoned_running_steps_in_state(
            state,
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
            stale_parquet_reason: str | None = None
            if blocked_by and step.id == "build-duckdb-views":
                # Stale-aware rebuild: if upstream derived steps failed but the
                # critical derived parquet files already exist, we may still
                # rebuild DuckDB views against the stale parquet and record the
                # step as success_degraded. If any critical parquet is missing,
                # we must NOT pretend to succeed.
                if _critical_derived_parquet_available(base):
                    stale_parquet_reason = (
                        f"degraded: upstream failed/blocked ({', '.join(blocked_by)}) "
                        "but critical derived parquet exists; rebuilt views from stale parquet"
                    )
                    _emit(
                        resolved_log,
                        now,
                        f"Warning: {step.id} upstream dependency failed: {', '.join(blocked_by)}; "
                        "critical derived parquet exists, proceeding with stale-aware view rebuild",
                        console=False,
                    )
                    blocked_by = ()
                else:
                    _emit(
                        resolved_log,
                        now,
                        f"Warning: {step.id} upstream dependency failed and critical derived "
                        "parquet is missing; cannot rebuild views",
                        console=False,
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

            effective_step = step
            degraded_success_reason: str | None = None
            blocked_reason: str | None = None
            upstream_degraded_reason = _upstream_degraded_success_reason(
                step,
                state,
                steps_by_id=steps_by_id,
                effective_dates=effective_dates,
                run_instance_key=run_instance_key,
            )

            # Log soft dependencies that are not satisfied (they don't block but indicate degraded input)
            for dep in step.depends_on:
                if not (isinstance(dep, DailyDependency) and dep.soft):
                    continue
                dep_id, dep_status, readable_state_key = _resolved_dependency_status(
                    dep,
                    state,
                    steps_by_id=steps_by_id,
                    effective_dates=effective_dates,
                    run_instance_key=run_instance_key,
                )
                if dep_status not in SATISFIED_DEPENDENCY_STATUSES:
                    degraded_step = _degraded_build_derived_step_for_akshare_daily_bar(
                        step,
                        dep_id,
                        dep_status,
                        readable_state_key,
                    )
                    if degraded_step is not None:
                        if degraded_step == "blocked":
                            blocked_reason = (
                                "degraded: cannot build target=daily_bar because "
                                f"{dep_id} status={dep_status} for {readable_state_key}"
                            )
                            _emit(
                                resolved_log,
                                now,
                                f"Warning: {step.id} soft dependency {dep_id} has status {dep_status} "
                                f"for {readable_state_key}; cannot run explicit target=daily_bar while "
                                "AkShare daily_bar input is degraded",
                                console=False,
                            )
                        else:
                            effective_step = degraded_step
                            degraded_success_reason = (
                                "degraded: excluded target=daily_bar because "
                                f"{dep_id} status={dep_status} for {readable_state_key}"
                            )
                            _emit(
                                resolved_log,
                                now,
                                f"Warning: {step.id} soft dependency {dep_id} has status {dep_status} "
                                f"for {readable_state_key}; proceeding in degraded mode and excluding "
                                "target=daily_bar dataset=cn_stock_daily_bar",
                                console=False,
                            )
                        continue
                    _emit(
                        resolved_log,
                        now,
                        f"Warning: {step.id} soft dependency {dep_id} has status {dep_status} "
                        f"for {readable_state_key}; proceeding anyway",
                        console=False,
                    )
                    if degraded_success_reason is None:
                        degraded_success_reason = (
                            "degraded: soft dependency "
                            f"{dep_id} status={dep_status} for {readable_state_key}"
                        )

            if blocked_reason is not None:
                _record_step(step_state, effective_step, "blocked", 1, resolved_log, now, reason=blocked_reason)
                _write_state(resolved_state_file, state)
                _emit(resolved_log, now, f"Blocked {step.id}; {blocked_reason}", console=True)
                continue

            _record_step(step_state, effective_step, "running", None, resolved_log, now)
            # For derived build steps that use the unified progress contract,
            # record the orchestrator-pinned progress path and derived run id
            # in the state file so crash recovery and external monitors can
            # find the exact file the stall detector is reading. This is the
            # state-file half of the contract; the env-var half is in
            # ``_run_subprocess``.
            if _step_uses_derived_stall_detection(effective_step):
                pinned_progress_path = _derived_progress_path_for_step(
                    base, run_instance_key, effective_step.id
                )
                pinned_derived_run_id = f"{run_instance_key}:{effective_step.id}"
                step_state[effective_step.id]["progress_path"] = str(pinned_progress_path)
                step_state[effective_step.id]["derived_run_id"] = pinned_derived_run_id
            _write_state(resolved_state_file, state)
            _emit(
                resolved_log,
                now,
                f"Running {effective_step.id} ({effective_step.name})... log={resolved_log}",
                console=True,
            )
            _emit(resolved_log, now, f"Command: {effective_step.command_text}", console=False)
            _emit(resolved_log, now, f"Network profile: {effective_step.network_profile}", console=False)

            # Resolve per-step health summary path and expose it to the runner
            # (and the real subprocess) via QDC_STEP_RESULT_PATH. The runner
            # closure (default or test) inherits this from os.environ. After
            # the runner returns we read the summary, if any, to decide whether
            # a exit_code==0 step should be recorded as success or
            # success_degraded based on the records the command itself wrote.
            step_result_path = _step_result_path(base, run_instance_key, effective_step)
            prior_env_result_path = os.environ.get(STEP_RESULT_PATH_ENV)
            os.environ[STEP_RESULT_PATH_ENV] = str(step_result_path)
            # Clear any stale summary from a previous run so we never read it
            # by mistake if the current command does not write one.
            with suppress(OSError):
                if step_result_path.exists():
                    step_result_path.unlink()
            try:
                exit_code = int(runner(effective_step, resolved_log))
            finally:
                if prior_env_result_path is None:
                    os.environ.pop(STEP_RESULT_PATH_ENV, None)
                else:
                    os.environ[STEP_RESULT_PATH_ENV] = prior_env_result_path
            step_health_summary = read_step_health_summary(step_result_path)
            if exit_code != 0:
                timed_out = exit_code == TIMEOUT_EXIT_CODE and step.timeout_seconds is not None
                timeout_cleanup_failed = (
                    exit_code == TIMEOUT_CLEANUP_FAILED_EXIT_CODE and step.timeout_seconds is not None
                )
                stalled = exit_code == STALLED_EXIT_CODE
                if stalled:
                    # Stall detection (derived builds only): the child's
                    # heartbeat was stale and ``processed`` was unchanged.
                    # The child was sent SIGINT for cooperative cancellation;
                    # whatever it committed before exiting is preserved. Record
                    # a distinct ``stalled`` status so the state file clearly
                    # distinguishes this from ``timed_out`` / ``failed``.
                    _record_step(
                        step_state,
                        effective_step,
                        "stalled",
                        exit_code,
                        resolved_log,
                        now,
                        reason="derived build stalled: heartbeat stale and processed unchanged",
                        health_summary=step_health_summary,
                        health_summary_path=step_result_path if step_result_path.exists() else None,
                    )
                    _write_state(resolved_state_file, state)
                    _emit(
                        resolved_log,
                        now,
                        f"{step.name} declared stalled (heartbeat stale, processed unchanged); stopping",
                        console=True,
                    )
                    failed_exit_code = failed_exit_code or exit_code
                    continue
                if timed_out or timeout_cleanup_failed:
                    if timeout_cleanup_failed:
                        if step.optional:
                            _record_step(
                                step_state,
                                effective_step,
                                "failed_timeout_cleanup",
                                exit_code,
                                resolved_log,
                                now,
                                health_summary=step_health_summary,
                                health_summary_path=step_result_path if step_result_path.exists() else None,
                            )
                        else:
                            _record_step(
                                step_state,
                                effective_step,
                                "failed",
                                exit_code,
                                resolved_log,
                                now,
                                health_summary=step_health_summary,
                                health_summary_path=step_result_path if step_result_path.exists() else None,
                            )
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
                                effective_step,
                                "failed_resource_locked",
                                exit_code,
                                resolved_log,
                                now,
                                health_summary=step_health_summary,
                                health_summary_path=step_result_path if step_result_path.exists() else None,
                            )
                        else:
                            _record_step(
                                step_state,
                                effective_step,
                                "failed",
                                exit_code,
                                resolved_log,
                                now,
                                health_summary=step_health_summary,
                                health_summary_path=step_result_path if step_result_path.exists() else None,
                            )
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
                    _record_step(
                        step_state,
                        effective_step,
                        status,
                        exit_code,
                        resolved_log,
                        now,
                        health_summary=step_health_summary,
                        health_summary_path=step_result_path if step_result_path.exists() else None,
                    )
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
                _record_step(
                    step_state,
                    effective_step,
                    "failed",
                    exit_code,
                    resolved_log,
                    now,
                    health_summary=step_health_summary,
                    health_summary_path=step_result_path if step_result_path.exists() else None,
                )
                _write_state(resolved_state_file, state)
                if failed_exit_code is None:
                    failed_exit_code = exit_code
                continue

            _emit(resolved_log, now, f"Completed {effective_step.id} ({effective_step.name})", console=True)
            # If the command wrote a StepHealthSummary, use its status to decide
            # the step verdict. The summary takes precedence over the
            # orchestrator-derived degraded reasons because the command has the
            # most accurate view of its own records.
            summary_status = step_health_summary.status if step_health_summary is not None else None
            if summary_status == "failed":
                # The command reported a fatal/threshold failure even though
                # the sub-process returned exit code 0 (e.g. tolerant policy
                # threshold exceeded). Treat it as a real failure.
                _emit(
                    resolved_log,
                    now,
                    f"{step.name} reported status=failed via health summary: "
                    f"{step_health_summary.reason if step_health_summary else 'unknown'}",
                    console=True,
                )
                _record_step(
                    step_state,
                    effective_step,
                    "failed",
                    exit_code,
                    resolved_log,
                    now,
                    health_summary=step_health_summary,
                    health_summary_path=step_result_path if step_result_path.exists() else None,
                )
                _write_state(resolved_state_file, state)
                if failed_exit_code is None:
                    failed_exit_code = exit_code if exit_code != 0 else 1
                continue
            if summary_status == "success_degraded":
                _record_step(
                    step_state,
                    effective_step,
                    "success_degraded",
                    0,
                    resolved_log,
                    now,
                    reason=step_health_summary.reason if step_health_summary else None,
                    health_summary=step_health_summary,
                    health_summary_path=step_result_path if step_result_path.exists() else None,
                )
            elif degraded_success_reason is not None:
                _record_step(
                    step_state,
                    effective_step,
                    "success_degraded",
                    0,
                    resolved_log,
                    now,
                    reason=degraded_success_reason,
                    health_summary=step_health_summary,
                    health_summary_path=step_result_path if step_result_path.exists() else None,
                )
            elif upstream_degraded_reason is not None:
                _record_step(
                    step_state,
                    effective_step,
                    "success_degraded",
                    0,
                    resolved_log,
                    now,
                    reason=upstream_degraded_reason,
                    health_summary=step_health_summary,
                    health_summary_path=step_result_path if step_result_path.exists() else None,
                )
            elif stale_parquet_reason is not None:
                _record_step(
                    step_state,
                    effective_step,
                    "success_degraded",
                    0,
                    resolved_log,
                    now,
                    reason=stale_parquet_reason,
                    health_summary=step_health_summary,
                    health_summary_path=step_result_path if step_result_path.exists() else None,
                )
            else:
                _record_step(
                    step_state,
                    effective_step,
                    "success",
                    0,
                    resolved_log,
                    now,
                    health_summary=step_health_summary,
                    health_summary_path=step_result_path if step_result_path.exists() else None,
                )
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
        # Refresh log_status one final time so the state file reflects whether
        # the run log survived (e.g. ``available`` vs ``missing`` after an
        # external cleanup ran in parallel).
        state["run_log"] = run_log_context.status_payload(now=(now or datetime.now)())
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
    """Deprecated: kept for backwards compatibility with code that imports it.

    New code should use ``RunLogContext`` via ``_resolve_run_log_context`` so
    that the per-run log lives outside the Git workspace by default.
    """

    stamp = (now or datetime.now)().strftime("%Y%m%d_%H%M%S")
    return root / "logs" / f"run_update_daily_{stamp}.log"


def _resolve_run_log_context(
    *,
    base: Path,
    run_log: Path | None,
    now: Callable[[], datetime] | None,
) -> RunLogContext:
    """Resolve the per-run log context used by ``run_daily_update``.

    When ``run_log`` is provided (CLI ``--run-log`` or test fixture), the
    orchestrator adopts that path so existing entrypoints (BAT script, tests)
    keep working. Otherwise it creates a fresh run log under the resolved
    ``RuntimePaths.run_logs_dir`` (outside the Git workspace by default).
    """

    timestamp = (now or datetime.now)()
    if run_log is not None:
        try:
            return adopt_run_log_context(path=run_log, now=timestamp)
        except RunLogContextError as exc:
            raise RunDailyUpdateLockError(f"Failed to adopt run log path {run_log}: {exc}") from exc
    runtime_paths = paths.resolve_runtime_paths(root=base)
    try:
        return create_run_log_context(runtime_paths=runtime_paths, now=timestamp)
    except RunLogContextError as exc:
        raise RunDailyUpdateLockError(f"Failed to create run log: {exc}") from exc


def _step_result_path(base: Path, run_instance_key: str, step: DailyStep) -> Path:
    """Resolve the per-run JSON path where the step writes its StepHealthSummary.

    Layout: ``data/metadata/step_results/<run_instance>/<step_id>.json``

    The run_instance key looks like ``run_instance:YYYYMMDD_HHMMSS``; we strip
    the ``run_instance:`` prefix for the on-disk directory name to keep paths
    readable while still being unique per orchestrator invocation.
    """

    safe_key = run_instance_key
    if safe_key.startswith("run_instance:"):
        safe_key = safe_key[len("run_instance:") :]
    return base / "data" / "metadata" / "step_results" / safe_key / f"{step.id}.json"


def _critical_derived_parquet_available(base: Path) -> bool:
    """Return True iff every critical derived dataset has at least one parquet file.

    Used by build-duckdb-views stale-aware rebuild: when upstream derived
    steps failed but their prior parquet output still exists, the view layer
    can be rebuilt against the stale parquet. If any critical parquet is
    missing, the view rebuild must NOT pretend to succeed.
    """

    parquet_root = base / "data" / "parquet"
    for dataset_id in CRITICAL_DERIVED_DATASETS_FOR_VIEWS:
        dataset_dir = parquet_root / dataset_id
        if not dataset_dir.exists():
            return False
        if not any(
            file.name.endswith(".parquet") and ".tmp.parquet" not in file.name
            for file in dataset_dir.rglob("*.parquet")
        ):
            return False
    return True


def _run_subprocess(
    step: DailyStep,
    log_path: Path,
    root: Path,
    *,
    run_log_context: RunLogContext | None = None,
    run_instance_key: str | None = None,
    on_spawn: Callable[[int], None] | None = None,
) -> int:
    """Spawn a step's command as a subprocess and wait for it (with stall detection).

    For derived build steps in :data:`DERIVED_STALL_TARGETS`, the orchestrator
    pins a unique progress-state path per (run_instance, step_id) BEFORE spawn
    and passes it to the child via :data:`QDC_DERIVED_PROGRESS_PATH_ENV`. The
    stall detector reads only that path — never a directory scan.

    For the cleanup step (and any step that respects ``--active-run-id``), the
    orchestrator's current run-id is propagated via
    :data:`QDC_ACTIVE_RUN_ID_ENV` so the cleanup subprocess can protect the
    in-flight run log from deletion.
    """

    global _LAST_CHILD_PID
    env = build_network_env(os.environ, profile=step.network_profile)
    # ``QDC_DISABLE_FILE_LOG=1`` is preserved for backwards compatibility. Its
    # actual semantics is "subprocess only echoes to the per-run log captured
    # by the orchestrator via stdout/stderr redirection"; it does not silence
    # the application log entirely. See ``run_logging`` for details.
    env["QDC_DISABLE_FILE_LOG"] = "1"
    # Share the per-run log path with the child so that any subprocess that
    # wants to write a structured progress line (e.g. build-derived heartbeat)
    # appends to the same run log instead of opening a parallel file.
    env["QDC_RUN_LOG_PATH"] = str(log_path)
    # The orchestrator sets QDC_STEP_RESULT_PATH in os.environ before invoking
    # the runner; propagate it into the child process so that CLI commands
    # (baostock / akshare / build-derived) can write their StepHealthSummary.
    step_result_path = os.environ.get(STEP_RESULT_PATH_ENV)
    if step_result_path:
        env[STEP_RESULT_PATH_ENV] = step_result_path
    # Propagate the orchestrator's current run-id to every child so that the
    # cleanup subprocess (and any other tool that respects QDC_ACTIVE_RUN_ID)
    # can protect the in-flight run log. We do NOT read this from os.environ
    # because the orchestrator is the authoritative source of the run id;
    # relying on a stale env var would defeat the protection.
    if run_log_context is not None:
        env[QDC_ACTIVE_RUN_ID_ENV] = run_log_context.run_id
    # For derived build steps that use the unified progress contract, pin a
    # unique progress-state path per (run_instance, step_id) and pass it to
    # the child. The child (build_cn_stock_daily_bar) reads this env var via
    # ``_progress_path_override_from_env`` and writes its ProgressReporter
    # state to exactly this file. The stall detector below reads the same
    # path — no directory scan is involved.
    derived_target = _extract_derived_target(step) if _step_uses_derived_stall_detection(step) else None
    progress_path: Path | None = None
    derived_run_id: str | None = None
    if derived_target is not None and run_instance_key is not None:
        progress_path = _derived_progress_path_for_step(root, run_instance_key, step.id)
        derived_run_id = f"{run_instance_key}:{step.id}"
        env[QDC_DERIVED_PROGRESS_PATH_ENV] = str(progress_path)
        env[QDC_DERIVED_RUN_ID_ENV] = derived_run_id
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
        else:
            # On Windows, create the child in its own process group so the
            # orchestrator can send ``CTRL_BREAK_EVENT`` for cooperative
            # cancellation when stall detection triggers. Without this flag,
            # ``send_signal(CTRL_BREAK_EVENT)`` would also interrupt the
            # orchestrator itself.
            popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore[attr-defined]
        proc = subprocess.Popen(step.command, **popen_kwargs)
        # Capture the child pid immediately so the orchestrator (and the
        # ``on_spawn`` callback) can surface it in the state file *while* the
        # step is still running, instead of recording it only post-mortem.
        _LAST_CHILD_PID = proc.pid
        if on_spawn is not None:
            try:
                on_spawn(proc.pid)
            except Exception:  # pragma: no cover - state-file writes must never kill the step
                logger.exception("Failed to record child_pid for step {}", step.id)
        try:
            # Derived build steps in DERIVED_STALL_TARGETS have no fixed
            # ``timeout_seconds`` because a normal full rebuild can legitimately
            # exceed 4 hours. Instead of a hard timeout, we use stall detection:
            # the orchestrator polls the child's pinned progress state file and
            # declares the build stalled when the heartbeat is stale AND
            # ``processed`` is unchanged. A high safety timeout (18h) acts as
            # a last-resort fallback. See ``progress.check_stall`` for details.
            if step.timeout_seconds is not None:
                return int(proc.wait(timeout=step.timeout_seconds))
            if derived_target is not None:
                return _wait_with_stall_detection(
                    proc,
                    step,
                    log,
                    root,
                    progress_path=progress_path,
                    derived_run_id=derived_run_id,
                )
            return int(proc.wait())
        except subprocess.TimeoutExpired:
            log.write(f"Step timed out after {step.timeout_seconds} seconds; terminating process tree\n")
            if _terminate_process_tree(proc, log):
                return TIMEOUT_EXIT_CODE
            log.write("Process tree cleanup failed after timeout\n")
            return TIMEOUT_CLEANUP_FAILED_EXIT_CODE
        finally:
            _LAST_CHILD_PID = None


def _step_uses_derived_stall_detection(step: DailyStep) -> bool:
    """Return True if the step is a derived build that should use stall detection."""

    return step.id.startswith("build-derived-") and any(
        f"--target {target}" in step.command_text or f"--target={target}" in step.command_text
        for target in DERIVED_STALL_TARGETS
    )


def _wait_with_stall_detection(
    proc: subprocess.Popen[Any],
    step: DailyStep,
    log: TextIO,
    root: Path,
    *,
    progress_path: Path | None = None,
    derived_run_id: str | None = None,
) -> int:
    """Poll the child's pinned progress state and terminate if stalled.

    Replaces ``proc.wait()`` for derived build steps. Polls every
    :data:`DERIVED_STALL_POLL_SECONDS` and checks:

    1. Is the child PID still alive? If not, return its exit code.
    2. Has the safety timeout (18h) been exceeded? If so, terminate → ``timed_out``.
    3. Is the progress heartbeat stale AND ``processed`` unchanged? If so,
       terminate → ``stalled``.

    Stall verdict preservation (P0):

    Once the orchestrator declares the build stalled, the final exit code is
    fixed to :data:`STALLED_EXIT_CODE` (126) regardless of what the child
    returns after SIGINT. The child's exit code (0, 1, ...) CANNOT override
    the stall verdict — that was the original bug. The only exception is when
    the process tree cannot be cleaned up at all, in which case the exit code
    is :data:`TIMEOUT_CLEANUP_FAILED_EXIT_CODE` (125) to signal a more severe
    failure.

    The four terminal states are distinguishable by exit code:
    - normal completion → child's exit code
    - stalled (clean shutdown or force-kill succeeded) → ``STALLED_EXIT_CODE`` (126)
    - stalled AND process-tree cleanup failed → ``TIMEOUT_CLEANUP_FAILED_EXIT_CODE`` (125)
    - timed_out (safety timeout) → ``TIMEOUT_EXIT_CODE`` (124)
    - failed → child's non-zero exit code
    """

    from src.sources.derived.progress import check_stall

    # Resolve the pinned progress path. The orchestrator MUST pass the path it
    # pinned before spawn; we never scan the derived-runs directory in
    # production. Falling back to ``latest_progress_state_path`` is forbidden
    # because a stale or unrelated progress file could mask a real stall.
    if progress_path is None:
        # Defensive: if a caller forgot to pin a path (e.g. an old test), we
        # refuse to invent one. Treat the build as non-stallable and just wait.
        log.write(
            "Stall detection skipped: no pinned progress_path was provided; "
            "waiting for child without stall detection\n"
        )
        return int(proc.wait())

    metadata_dir = root / "data" / "metadata"
    del metadata_dir  # Kept for clarity; not used in the pinned-path model.
    start_monotonic = time.monotonic()
    previous_processed: int | None = None
    stall_threshold_seconds = _resolve_derived_stall_seconds(root)
    while True:
        # Check if the child has exited.
        exit_code = proc.poll()
        if exit_code is not None:
            return int(exit_code)
        # Check safety timeout (18h, well above historical P99).
        elapsed = time.monotonic() - start_monotonic
        if elapsed > DERIVED_SAFETY_TIMEOUT_SECONDS:
            log.write(
                f"Step exceeded safety timeout of {DERIVED_SAFETY_TIMEOUT_SECONDS} seconds "
                f"(elapsed {elapsed:.0f}s); terminating process tree\n"
            )
            if _terminate_process_tree(proc, log):
                return TIMEOUT_EXIT_CODE
            log.write("Process tree cleanup failed after safety timeout\n")
            return TIMEOUT_CLEANUP_FAILED_EXIT_CODE
        # Check stall via the PINNED progress state file. We never call
        # ``latest_progress_state_path`` here: a directory scan could pick up
        # an unrelated or stale progress file and either mask a real stall or
        # stall a healthy build that writes to a different path.
        if progress_path.exists():
            report = check_stall(
                progress_path,
                stall_heartbeat_seconds=stall_threshold_seconds,
                previous_processed=previous_processed,
            )
            if report.stalled:
                log.write(
                    f"Step declared stalled: {report.reason}; "
                    f"terminating process tree (child will commit "
                    f"whatever was completed via cooperative cancellation)\n"
                )
                # Send SIGINT first to allow cooperative cancellation
                # (the child's signal handler sets the cancel event,
                # running workers finish, and the coordinator commits).
                _send_interrupt(proc, log)
                # Wait up to 60 seconds for graceful shutdown. Regardless of
                # the child's exit code (0, 1, ...), the orchestrator's
                # verdict is ``stalled`` — the child cannot override it.
                try:
                    proc.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    log.write(
                        "Child did not respond to SIGINT within 60s; force-terminating\n"
                    )
                # Force-kill whatever is left. If cleanup succeeds, the stall
                # verdict stands (126). If cleanup fails, escalate to 125.
                if _terminate_process_tree(proc, log):
                    log.write(
                        "Stall verdict preserved: returning STALLED_EXIT_CODE (126); "
                        "child exit code is ignored per the stall contract.\n"
                    )
                    return STALLED_EXIT_CODE
                log.write(
                    "Process tree cleanup failed after stall; returning "
                    "TIMEOUT_CLEANUP_FAILED_EXIT_CODE (125)\n"
                )
                return TIMEOUT_CLEANUP_FAILED_EXIT_CODE
            previous_processed = report.processed
        # Sleep before next poll. Use a short wait so we don't miss a
        # quick exit by more than the poll interval.
        try:
            exit_code = proc.wait(timeout=DERIVED_STALL_POLL_SECONDS)
            return int(exit_code)
        except subprocess.TimeoutExpired:
            continue


def _resolve_derived_stall_seconds(root: Path) -> float:
    """Resolve the stall heartbeat threshold from settings.yaml.

    Priority: settings.yaml (``derived.stall_seconds``) > code default
    (:data:`DERIVED_STALL_HEARTBEAT_SECONDS`). Configuration errors fall back
    to the code default rather than aborting the build — a missing or invalid
    settings.yaml must not prevent stall detection from running at all.
    """

    try:
        from src.sources.derived.config import load_derived_runtime_config

        config = load_derived_runtime_config(root=root)
        return float(config.stall_seconds)
    except Exception:
        return float(DERIVED_STALL_HEARTBEAT_SECONDS)


def _derived_progress_path_for_step(root: Path, run_instance_key: str, step_id: str) -> Path:
    """Resolve the pinned progress-state path for one derived build step.

    Layout: ``data/metadata/derived-step-progress/<run_instance>/<step_id>.state.json``

    The path is unique per (run_instance, step_id), so concurrent orchestrator
    invocations never collide, and a resumed run writes to a fresh path
    (the previous run's path is left in place for forensic inspection). The
    path is generated BEFORE spawn and communicated to the child via
    :data:`QDC_DERIVED_PROGRESS_PATH_ENV`.
    """

    safe_key = run_instance_key
    if safe_key.startswith("run_instance:"):
        safe_key = safe_key[len("run_instance:") :]
    return (
        root
        / "data"
        / "metadata"
        / "derived-step-progress"
        / safe_key
        / f"{step_id}.state.json"
    )


def _extract_derived_target(step: DailyStep) -> str | None:
    """Extract the ``--target`` value from a build-derived step's command."""

    command = step.command
    for i, arg in enumerate(command):
        if arg == "--target" and i + 1 < len(command):
            return command[i + 1]
        if arg.startswith("--target="):
            return arg.split("=", 1)[1]
    return None


def _send_interrupt(proc: subprocess.Popen[Any], log: TextIO) -> None:
    """Send a cooperative interrupt signal to the child process.

    On POSIX we send SIGINT to the process group (since the child was started
    with ``start_new_session=True``). On Windows we send CTRL_BREAK_EVENT to
    the child's process group; the child's SIGINT handler translates this to
    a cancel event.

    We intentionally send SIGINT (not SIGTERM/SIGKILL) so the child can
    cooperatively cancel: stop submitting new work, let running workers
    finish, and commit whatever was completed.
    """

    if os.name == "nt":
        try:
            proc.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
        except (OSError, ValueError, AttributeError) as exc:
            log.write(f"Failed to send CTRL_BREAK_EVENT to child {proc.pid}: {exc}\n")
    else:
        try:
            os.killpg(proc.pid, signal.SIGINT)
        except (ProcessLookupError, OSError) as exc:
            log.write(f"Failed to send SIGINT to process group {proc.pid}: {exc}\n")


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
        changed = (
            _mark_abandoned_running_steps(
                steps_for_key,
                step_state,
                log_path,
                now,
                active_lock_owner,
            )
            or changed
        )
    return changed


def _mark_abandoned_running_steps_in_state(
    state: dict[str, Any],
    log_path: Path,
    now: Callable[[], datetime] | None,
    active_lock_owner: dict[str, object],
) -> bool:
    del log_path
    runs = state.get("runs", {})
    if not isinstance(runs, dict):
        return False
    changed = False
    for run_state in runs.values():
        if not isinstance(run_state, dict):
            continue
        step_state = run_state.get("steps", {})
        if not isinstance(step_state, dict):
            continue
        for current in step_state.values():
            if not isinstance(current, dict) or str(current.get("status")) != "running":
                continue
            reason = _running_abandoned_reason(current, now, active_lock_owner)
            if reason is None:
                continue
            _mark_running_row_abandoned(current, now, reason)
            changed = True
    return changed


def _mark_running_row_abandoned(
    row: dict[str, Any],
    now: Callable[[], datetime] | None,
    reason: str,
) -> None:
    timestamp = _timestamp(now)
    row["status"] = "abandoned"
    row["updated_at"] = timestamp
    row["ended_at"] = timestamp
    row["exit_code"] = 1
    row["reason"] = reason


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
    health_summary: StepHealthSummary | None = None,
    health_summary_path: str | Path | None = None,
) -> None:
    previous = step_state.get(step.id, {})
    timestamp = _timestamp(now)
    started_at = previous.get("started_at") if status != "running" else timestamp
    row = {
        "status": status,
        "command": step.command_text,
        "network_profile": step.network_profile,
        "started_at": started_at,
        "updated_at": timestamp,
        "ended_at": None if status == "running" else timestamp,
        "exit_code": exit_code,
        "log_path": str(log_path),
    }
    if status == "running":
        row["pid"] = os.getpid()
        row["orchestrator_pid"] = os.getpid()
    # Preserve the recorded child_pid across status transitions. ``_run_subprocess``
    # populates this via the ``on_spawn`` callback after ``Popen`` returns; the
    # value must survive the ``running`` → ``success`` / ``failed`` row rewrite
    # so that forensic inspection of a completed step can still find the actual
    # subprocess that produced the result.
    previous_child_pid = previous.get("child_pid") if isinstance(previous, dict) else None
    if previous_child_pid is not None:
        row["child_pid"] = previous_child_pid
    # Preserve the orchestrator-pinned derived progress path and run id across
    # status transitions (running → stalled/failed/success). These are written
    # once before spawn and must survive the row rewrite so crash recovery can
    # locate the exact progress file the stall detector was reading.
    for preserve_key in ("progress_path", "derived_run_id"):
        previous_value = previous.get(preserve_key) if isinstance(previous, dict) else None
        if previous_value is not None:
            row[preserve_key] = previous_value
    if blocked_by is not None:
        row["blocked_by"] = list(blocked_by)
    if reason is not None:
        row["reason"] = reason
    if health_summary is not None:
        row["record_count"] = health_summary.total_records
        row["success_count"] = health_summary.success_records
        row["failed_count"] = health_summary.failed_records
        row["failed_ratio"] = health_summary.failed_record_ratio
        row["failed_codes_sample"] = list(health_summary.failed_codes[:10])
        row["failed_datasets_sample"] = list(health_summary.failed_datasets[:10])
        if health_summary.reason and not reason:
            row["reason"] = health_summary.reason
    if health_summary_path is not None:
        row["health_summary_path"] = str(health_summary_path)
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
    active_pid = _int_or_none(active_lock_owner.get("pid"))
    if active_pid == pid:
        return None
    if not is_pid_alive(pid):
        return f"orchestrator pid {pid} is not alive"

    started_at = _parse_timestamp(row.get("started_at"))
    if started_at is None:
        return "running step has no valid started_at"
    current_time = (now or datetime.now)()
    if current_time - started_at <= timedelta(seconds=RUNNING_ABANDONED_AFTER_SECONDS):
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


def _resolved_dependency_status(
    dependency: DailyDependency | str,
    state: dict[str, Any],
    *,
    steps_by_id: dict[str, DailyStep],
    effective_dates: DailyEffectiveDates,
    run_instance_key: str,
) -> tuple[str, str, str]:
    dependency_id = _dependency_step_id(dependency)
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
            state,
            dependency_id,
            dependency_policy,
            effective_dates,
            run_instance_key,
        ).get("status", "pending")
    )
    return (
        dependency_id,
        dependency_status,
        _state_key_for_policy(dependency_policy, effective_dates, run_instance_key),
    )


def _upstream_degraded_success_reason(
    step: DailyStep,
    state: dict[str, Any],
    *,
    steps_by_id: dict[str, DailyStep],
    effective_dates: DailyEffectiveDates,
    run_instance_key: str,
) -> str | None:
    for dependency in step.depends_on:
        dependency_id, dependency_status, _ = _resolved_dependency_status(
            dependency,
            state,
            steps_by_id=steps_by_id,
            effective_dates=effective_dates,
            run_instance_key=run_instance_key,
        )
        if dependency_status == "success_degraded":
            return f"degraded: upstream dependency {dependency_id} completed with success_degraded"
    return None


def _degraded_build_derived_step_for_akshare_daily_bar(
    step: DailyStep,
    dependency_id: str,
    dependency_status: str,
    readable_state_key: str,
) -> DailyStep | str | None:
    del dependency_status, readable_state_key
    if step.id not in {"build-derived", "build-derived-daily-bar"} or dependency_id != "akshare-daily-bar":
        return None
    if step.id == "build-derived-daily-bar":
        return "blocked"

    requested_targets = _build_derived_requested_targets(step.command)
    if "all" not in requested_targets and "daily_bar" not in requested_targets:
        return None
    if _build_derived_command_excludes_daily_bar(step.command):
        return step
    if "daily_bar" in requested_targets and "all" not in requested_targets:
        return "blocked"
    return replace(step, command=_append_build_derived_exclude_daily_bar(step.command))


def _build_derived_requested_targets(command: tuple[str, ...]) -> tuple[str, ...]:
    targets: list[str] = []
    index = 0
    while index < len(command):
        arg = command[index]
        if arg == "--target" and index + 1 < len(command):
            targets.append(command[index + 1])
            index += 2
            continue
        if arg.startswith("--target="):
            targets.append(arg.split("=", 1)[1])
        index += 1
    return tuple(targets or ["all"])


def _build_derived_command_excludes_daily_bar(command: tuple[str, ...]) -> bool:
    index = 0
    while index < len(command):
        arg = command[index]
        if arg == "--exclude-target" and index + 1 < len(command) and command[index + 1] == "daily_bar":
            return True
        if arg == "--exclude-target=daily_bar":
            return True
        index += 1
    return False


def _append_build_derived_exclude_daily_bar(command: tuple[str, ...]) -> tuple[str, ...]:
    addition = ("--exclude-target", "daily_bar")
    try:
        insert_at = command.index("--no-build-duckdb-views")
    except ValueError:
        return (*command, *addition)
    return (*command[:insert_at], *addition, *command[insert_at:])


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
        is_soft = isinstance(dependency, DailyDependency) and dependency.soft
        if steps_by_id is None or effective_dates is None or run_instance_key is None:
            dependency_status = str(state_or_step_state.get(dependency_id, {}).get("status", "pending"))
        else:
            _, dependency_status, _ = _resolved_dependency_status(
                dependency,
                state_or_step_state,
                steps_by_id=steps_by_id,
                effective_dates=effective_dates,
                run_instance_key=run_instance_key,
            )
        if dependency_status in FAILED_DEPENDENCY_STATUSES or dependency_status not in SATISFIED_DEPENDENCY_STATUSES:
            if is_soft:
                pass  # Soft dependencies never block; warning emitted by caller
            else:
                blocked.append(dependency_id)
    return tuple(blocked)


def _final_exit_code(steps: list[DailyStep], step_state: dict[str, Any], failed_exit_code: int | None) -> int:
    if failed_exit_code is not None:
        return failed_exit_code
    # Any fatal terminal status (failed / failed_* / partial / cancelled /
    # stalled / timed_out / abandoned) on any step forces a non-zero final
    # exit code. This MUST stay aligned with the unified authority in
    # :func:`src.pipeline.step_health.is_failure_status`.
    for step in steps:
        current = step_state.get(step.id, {})
        if is_failure_status(current.get("status")):
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
