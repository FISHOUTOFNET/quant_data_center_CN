"""Unified step health evaluation for daily workflow pipeline commands.

This module is the single source of truth for converting a list of pipeline
records (success / failed / skipped) into a step-level verdict:

* ``success``           - no failures and at least one success record
* ``success_degraded``  - some records failed but below policy thresholds
* ``failed``            - fatal failure or thresholds exceeded

It is intentionally generic and lives outside the baostock / akshare modules
so that every source command can share the same degraded-success semantics
without bespoke per-source patches.

The orchestrator (``src/tools/run_update_daily.py``) reads the serialized
``StepHealthSummary`` produced by :func:`write_step_health_summary` to decide
whether a step should be recorded as ``success``, ``success_degraded``, or
``failed`` even when the CLI sub-process returned exit code 0.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable

STEP_HEALTH_SCHEMA_VERSION = 1
STEP_HEALTH_STATUSES: tuple[str, ...] = ("success", "success_degraded", "failed")

# Records whose dataset is "__metadata__" describe metadata writes; they are
# always fatal because metadata is what the rest of the workflow relies on.
METADATA_DATASET = "__metadata__"

# Records emitted by the timeout/cleanup path of the orchestrator itself are
# always fatal: they indicate a process tree that may still be holding locks.
DEFAULT_FATAL_STATUSES: tuple[str, ...] = ("failed_resource_locked", "failed_timeout_cleanup")

# ---------------------------------------------------------------------------
# Unified terminal-status classification (single authority).
#
# These sets and helpers are the ONLY place that decides whether a pipeline
# status string is success, skipped, or failure. ``records.py``,
# ``run_update_daily.py``, and any other module MUST import from here instead
# of redefining private ``_is_failed_status`` copies.
#
# Semantics (per the task spec):
# * ``success`` is the only true success.
# * ``success_degraded`` is success-with-warnings (tolerant policy); it is
#   NOT produced from ``partial`` — partial is a fatal terminal status.
# * ``skipped`` / ``skipped_checkpoint`` are neutral (not failure, not success).
# * ``partial`` means a derived target had partition failures — fatal terminal.
# * ``cancelled`` / ``stalled`` / ``timed_out`` are always fatal terminal
#   statuses; they cannot be downgraded by any tolerant policy.
# * ``failed`` and any ``failed_*`` (e.g. ``failed_resource_locked``,
#   ``failed_timeout_cleanup``) are fatal.
# ---------------------------------------------------------------------------

SUCCESS_STATUSES: frozenset[str] = frozenset({"success"})
DEGRADED_SUCCESS_STATUSES: frozenset[str] = frozenset({"success_degraded"})
SKIPPED_STATUSES: frozenset[str] = frozenset({"skipped", "skipped_checkpoint"})
# Fatal terminal statuses that can NEVER be downgraded to success_degraded by
# a tolerant policy. ``partial`` is here because it means a derived target
# already had partition failures — that is a data-integrity failure, not a
# threshold question.
FAILURE_STATUSES: frozenset[str] = frozenset({
    "failed",
    "partial",
    "cancelled",
    "stalled",
    "timed_out",
    "failed_resource_locked",
    "failed_timeout_cleanup",
    # ``abandoned`` is an orchestrator-level terminal status set when a prior
    # run's ``running`` step is found orphaned (orchestrator pid dead or
    # exceeded RUNNING_ABANDONED_AFTER_SECONDS). It is a fatal failure: a
    # downstream step must not proceed as if the abandoned step had succeeded.
    "abandoned",
})
# Statuses that are always fatal regardless of tolerant-policy thresholds.
# These propagate up as ``failed`` even when a source step has a tolerant
# policy that would otherwise allow a small number of ordinary ``failed``
# records to degrade to ``success_degraded``.
FATAL_TERMINAL_STATUSES: frozenset[str] = frozenset({
    "partial",
    "cancelled",
    "stalled",
    "timed_out",
})


def is_success_status(value: object) -> bool:
    """Return True for ``success`` (the only true success status)."""

    return str(value or "") in SUCCESS_STATUSES


def is_degraded_success_status(value: object) -> bool:
    """Return True for ``success_degraded``."""

    return str(value or "") in DEGRADED_SUCCESS_STATUSES


def is_skipped_status(value: object) -> bool:
    """Return True for ``skipped`` / ``skipped_checkpoint``."""

    status = str(value or "")
    return status in SKIPPED_STATUSES or status.startswith("skipped")


def is_failure_status(value: object) -> bool:
    """Return True for any fatal terminal or ``failed*`` status.

    Covers ``failed``, ``failed_*`` (e.g. ``failed_resource_locked``),
    ``partial``, ``cancelled``, ``stalled``, ``timed_out``.
    """

    status = str(value or "")
    if status in FAILURE_STATUSES:
        return True
    return status.startswith("failed_")


def is_fatal_terminal_status(value: object) -> bool:
    """Return True for statuses that can never be downgraded by a tolerant policy.

    These are ``partial``, ``cancelled``, ``stalled``, ``timed_out``. A
    source step with a tolerant policy may still report ``success_degraded``
    for a small number of ordinary ``failed`` records, but a single
    ``partial`` / ``cancelled`` / ``stalled`` / ``timed_out`` record must
    always escalate to ``failed``.
    """

    return str(value or "") in FATAL_TERMINAL_STATUSES


def _is_failed_status(value: object) -> bool:
    # Delegate to the public authority so there is exactly one classification.
    return is_failure_status(value)


def _is_success_status(value: object) -> bool:
    return is_success_status(value)


def _is_skipped_status(value: object) -> bool:
    return is_skipped_status(value)


@dataclass(frozen=True)
class StepHealthPolicy:
    """Thresholds and fatal-failure rules for evaluating step health.

    Defaults are intentionally strict: any failed record yields ``failed``.
    Source commands that opt in to degraded-success must construct a tolerant
    policy explicitly (see :func:`baostock_market_session_policy` and friends).
    """

    require_any_success: bool = True
    max_failed_records: int = 0
    max_failed_record_ratio: float = 0.0
    max_failed_codes: int = 0
    max_failed_code_ratio: float = 0.0
    fatal_datasets: tuple[str, ...] = (METADATA_DATASET,)
    fatal_statuses: tuple[str, ...] = DEFAULT_FATAL_STATUSES

    def to_summary_dict(self) -> dict[str, Any]:
        return {
            "require_any_success": self.require_any_success,
            "max_failed_records": self.max_failed_records,
            "max_failed_record_ratio": self.max_failed_record_ratio,
            "max_failed_codes": self.max_failed_codes,
            "max_failed_code_ratio": self.max_failed_code_ratio,
            "fatal_datasets": list(self.fatal_datasets),
            "fatal_statuses": list(self.fatal_statuses),
        }


@dataclass(frozen=True)
class StepHealthSummary:
    """Result of evaluating records under a :class:`StepHealthPolicy`."""

    status: str
    reason: str
    total_records: int
    success_records: int
    failed_records: int
    skipped_records: int
    failed_record_ratio: float
    failed_codes: tuple[str, ...] = ()
    failed_code_ratio: float = 0.0
    failed_datasets: tuple[str, ...] = ()
    fatal_datasets: tuple[str, ...] = ()
    fatal_statuses: tuple[str, ...] = ()
    examples: tuple[dict[str, Any], ...] = ()
    policy: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["failed_codes"] = list(self.failed_codes)
        payload["failed_datasets"] = list(self.failed_datasets)
        payload["fatal_datasets"] = list(self.fatal_datasets)
        payload["fatal_statuses"] = list(self.fatal_statuses)
        payload["examples"] = list(self.examples)
        return payload


def baostock_market_session_policy() -> StepHealthPolicy:
    """Tolerant policy for the ``update-baostock-market-session`` step.

    A small number of per-code failures must not block valuation-percentile,
    derived-daily-bar, derived-valuation, or duckdb-views. Calendar / basic /
    metadata failures are still fatal.
    """

    return StepHealthPolicy(
        require_any_success=True,
        max_failed_records=50,
        max_failed_record_ratio=0.01,
        max_failed_codes=10,
        max_failed_code_ratio=0.005,
        fatal_datasets=(
            METADATA_DATASET,
            "baostock_cn_trading_calendar",
            "baostock_cn_stock_basic",
        ),
        fatal_statuses=DEFAULT_FATAL_STATUSES,
    )


def akshare_valuation_full_policy() -> StepHealthPolicy:
    """Tolerant policy for ``akshare update --target valuation --mode full``.

    Eastmoney valuation fetches occasionally time out for a handful of codes;
    those should not block ``build-derived-valuation``.
    """

    return StepHealthPolicy(
        require_any_success=True,
        max_failed_records=20,
        max_failed_record_ratio=0.005,
        max_failed_codes=10,
        max_failed_code_ratio=0.005,
        fatal_datasets=(METADATA_DATASET,),
        fatal_statuses=DEFAULT_FATAL_STATUSES,
    )


def akshare_daily_bar_policy() -> StepHealthPolicy:
    """Tolerant policy for ``akshare update --target daily_bar``.

    A single code may produce multiple adjustment tasks (unadjusted / qfq /
    hfq). The policy keeps record-level thresholds but also enforces a code
    ratio so that widespread per-code failures still hard-fail.
    """

    return StepHealthPolicy(
        require_any_success=True,
        max_failed_records=50,
        max_failed_record_ratio=0.01,
        max_failed_codes=10,
        max_failed_code_ratio=0.005,
        fatal_datasets=(METADATA_DATASET,),
        fatal_statuses=DEFAULT_FATAL_STATUSES,
    )


def strict_policy() -> StepHealthPolicy:
    """Default strict policy: any failed record yields ``failed``."""

    return StepHealthPolicy()


def failed_record_examples(
    records: Iterable[dict[str, object]],
    limit: int = 5,
) -> list[dict[str, Any]]:
    """Return up to ``limit`` failed records, truncated for log friendliness."""

    examples: list[dict[str, Any]] = []
    for record in records:
        if not _is_failed_status(record.get("status")):
            continue
        dataset = str(record.get("dataset", "") or "")
        code = str(record.get("code", "") or "")
        status = str(record.get("status", "") or "")
        stack = str(record.get("error_stack", "") or "")
        detail_lines = stack.splitlines()[:3]
        examples.append(
            {
                "dataset": dataset,
                "code": code,
                "status": status,
                "detail": " | ".join(detail_lines) if detail_lines else "",
            }
        )
        if len(examples) >= limit:
            break
    return examples


def _aggregate_failed_codes(records: Iterable[dict[str, object]]) -> tuple[list[str], int]:
    """Return (sorted failed codes, total processed codes for ratio)."""

    failed_codes: set[str] = set()
    all_codes: set[str] = set()
    for record in records:
        code = str(record.get("code", "") or "")
        if not code or code == "*":
            continue
        all_codes.add(code)
        if _is_failed_status(record.get("status")):
            failed_codes.add(code)
    return sorted(failed_codes), len(all_codes)


def evaluate_step_health(
    records: Iterable[dict[str, object]],
    policy: StepHealthPolicy,
) -> StepHealthSummary:
    """Evaluate records against ``policy`` and return a :class:`StepHealthSummary`.

    The evaluation order is:

    1. Count records by status (success / failed / skipped).
    2. Detect fatal datasets / fatal statuses -> ``failed``.
    3. If ``require_any_success`` and there is no success record -> ``failed``.
    4. If failed-record count or ratio exceeds thresholds -> ``failed``.
    5. If failed-code count or ratio exceeds thresholds -> ``failed``.
    6. If no failed records -> ``success``.
    7. Otherwise -> ``success_degraded``.
    """

    record_list = list(records)
    total = len(record_list)
    success_count = sum(1 for r in record_list if _is_success_status(r.get("status")))
    failed_count = sum(1 for r in record_list if _is_failed_status(r.get("status")))
    skipped_count = sum(1 for r in record_list if _is_skipped_status(r.get("status")))

    failed_datasets_ordered: list[str] = []
    seen_datasets: set[str] = set()
    fatal_datasets_hit: list[str] = []
    fatal_statuses_hit: list[str] = []
    for record in record_list:
        status_value = record.get("status")
        if not _is_failed_status(status_value):
            continue
        dataset = str(record.get("dataset", "") or "")
        status = str(status_value or "")
        if dataset and dataset not in seen_datasets:
            seen_datasets.add(dataset)
            failed_datasets_ordered.append(dataset)
        if dataset and dataset in policy.fatal_datasets and dataset not in fatal_datasets_hit:
            fatal_datasets_hit.append(dataset)
        # Fatal terminal statuses (partial/cancelled/stalled/timed_out) are
        # ALWAYS fatal regardless of policy.fatal_statuses — they represent
        # data-integrity failures that no tolerant threshold can absorb.
        if is_fatal_terminal_status(status) and status not in fatal_statuses_hit:
            fatal_statuses_hit.append(status)
        elif status and status in policy.fatal_statuses and status not in fatal_statuses_hit:
            fatal_statuses_hit.append(status)

    failed_codes, total_codes = _aggregate_failed_codes(record_list)
    failed_record_ratio = (failed_count / total) if total else 0.0
    failed_code_ratio = (len(failed_codes) / total_codes) if total_codes else 0.0

    examples = tuple(failed_record_examples(record_list))

    if fatal_datasets_hit:
        reason = (
            f"fatal dataset failure: {', '.join(fatal_datasets_hit)} "
            f"({failed_count} failed record(s) of {total})"
        )
        return StepHealthSummary(
            status="failed",
            reason=reason,
            total_records=total,
            success_records=success_count,
            failed_records=failed_count,
            skipped_records=skipped_count,
            failed_record_ratio=failed_record_ratio,
            failed_codes=tuple(failed_codes),
            failed_code_ratio=failed_code_ratio,
            failed_datasets=tuple(failed_datasets_ordered),
            fatal_datasets=tuple(fatal_datasets_hit),
            fatal_statuses=tuple(fatal_statuses_hit),
            examples=examples,
            policy=policy.to_summary_dict(),
        )

    if fatal_statuses_hit:
        reason = (
            f"fatal status: {', '.join(fatal_statuses_hit)} "
            f"({failed_count} failed record(s) of {total})"
        )
        return StepHealthSummary(
            status="failed",
            reason=reason,
            total_records=total,
            success_records=success_count,
            failed_records=failed_count,
            skipped_records=skipped_count,
            failed_record_ratio=failed_record_ratio,
            failed_codes=tuple(failed_codes),
            failed_code_ratio=failed_code_ratio,
            failed_datasets=tuple(failed_datasets_ordered),
            fatal_datasets=tuple(fatal_datasets_hit),
            fatal_statuses=tuple(fatal_statuses_hit),
            examples=examples,
            policy=policy.to_summary_dict(),
        )

    if policy.require_any_success and success_count == 0:
        reason = (
            f"no successful records among {total} record(s); require_any_success=True"
        )
        return StepHealthSummary(
            status="failed",
            reason=reason,
            total_records=total,
            success_records=success_count,
            failed_records=failed_count,
            skipped_records=skipped_count,
            failed_record_ratio=failed_record_ratio,
            failed_codes=tuple(failed_codes),
            failed_code_ratio=failed_code_ratio,
            failed_datasets=tuple(failed_datasets_ordered),
            fatal_datasets=tuple(fatal_datasets_hit),
            fatal_statuses=tuple(fatal_statuses_hit),
            examples=examples,
            policy=policy.to_summary_dict(),
        )

    if failed_count > policy.max_failed_records:
        reason = (
            f"failed record count {failed_count} exceeds max_failed_records={policy.max_failed_records}"
        )
        return StepHealthSummary(
            status="failed",
            reason=reason,
            total_records=total,
            success_records=success_count,
            failed_records=failed_count,
            skipped_records=skipped_count,
            failed_record_ratio=failed_record_ratio,
            failed_codes=tuple(failed_codes),
            failed_code_ratio=failed_code_ratio,
            failed_datasets=tuple(failed_datasets_ordered),
            fatal_datasets=tuple(fatal_datasets_hit),
            fatal_statuses=tuple(fatal_statuses_hit),
            examples=examples,
            policy=policy.to_summary_dict(),
        )

    if total > 0 and failed_record_ratio > policy.max_failed_record_ratio:
        reason = (
            f"failed record ratio {failed_record_ratio:.4f} exceeds "
            f"max_failed_record_ratio={policy.max_failed_record_ratio}"
        )
        return StepHealthSummary(
            status="failed",
            reason=reason,
            total_records=total,
            success_records=success_count,
            failed_records=failed_count,
            skipped_records=skipped_count,
            failed_record_ratio=failed_record_ratio,
            failed_codes=tuple(failed_codes),
            failed_code_ratio=failed_code_ratio,
            failed_datasets=tuple(failed_datasets_ordered),
            fatal_datasets=tuple(fatal_datasets_hit),
            fatal_statuses=tuple(fatal_statuses_hit),
            examples=examples,
            policy=policy.to_summary_dict(),
        )

    if len(failed_codes) > policy.max_failed_codes:
        reason = (
            f"failed code count {len(failed_codes)} exceeds max_failed_codes={policy.max_failed_codes}"
        )
        return StepHealthSummary(
            status="failed",
            reason=reason,
            total_records=total,
            success_records=success_count,
            failed_records=failed_count,
            skipped_records=skipped_count,
            failed_record_ratio=failed_record_ratio,
            failed_codes=tuple(failed_codes),
            failed_code_ratio=failed_code_ratio,
            failed_datasets=tuple(failed_datasets_ordered),
            fatal_datasets=tuple(fatal_datasets_hit),
            fatal_statuses=tuple(fatal_statuses_hit),
            examples=examples,
            policy=policy.to_summary_dict(),
        )

    if total_codes > 0 and failed_code_ratio > policy.max_failed_code_ratio:
        reason = (
            f"failed code ratio {failed_code_ratio:.4f} exceeds "
            f"max_failed_code_ratio={policy.max_failed_code_ratio}"
        )
        return StepHealthSummary(
            status="failed",
            reason=reason,
            total_records=total,
            success_records=success_count,
            failed_records=failed_count,
            skipped_records=skipped_count,
            failed_record_ratio=failed_record_ratio,
            failed_codes=tuple(failed_codes),
            failed_code_ratio=failed_code_ratio,
            failed_datasets=tuple(failed_datasets_ordered),
            fatal_datasets=tuple(fatal_datasets_hit),
            fatal_statuses=tuple(fatal_statuses_hit),
            examples=examples,
            policy=policy.to_summary_dict(),
        )

    if failed_count == 0:
        reason = f"all {total} record(s) succeeded"
        return StepHealthSummary(
            status="success",
            reason=reason,
            total_records=total,
            success_records=success_count,
            failed_records=failed_count,
            skipped_records=skipped_count,
            failed_record_ratio=failed_record_ratio,
            failed_codes=tuple(failed_codes),
            failed_code_ratio=failed_code_ratio,
            failed_datasets=tuple(failed_datasets_ordered),
            fatal_datasets=tuple(fatal_datasets_hit),
            fatal_statuses=tuple(fatal_statuses_hit),
            examples=examples,
            policy=policy.to_summary_dict(),
        )

    reason = (
        f"degraded: {failed_count} of {total} record(s) failed "
        f"({failed_record_ratio:.4f} ratio, {len(failed_codes)} code(s)); "
        f"within policy tolerance"
    )
    return StepHealthSummary(
        status="success_degraded",
        reason=reason,
        total_records=total,
        success_records=success_count,
        failed_records=failed_count,
        skipped_records=skipped_count,
        failed_record_ratio=failed_record_ratio,
        failed_codes=tuple(failed_codes),
        failed_code_ratio=failed_code_ratio,
        failed_datasets=tuple(failed_datasets_ordered),
        fatal_datasets=tuple(fatal_datasets_hit),
        fatal_statuses=tuple(fatal_statuses_hit),
        examples=examples,
        policy=policy.to_summary_dict(),
    )


def write_step_health_summary(path: Path, summary: StepHealthSummary) -> None:
    """Serialize ``summary`` to ``path`` as JSON."""

    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": STEP_HEALTH_SCHEMA_VERSION,
        **summary.to_dict(),
    }
    tmp_path = path.with_name(f".{path.name}.{path.stem}.tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp_path.replace(path)


def read_step_health_summary(path: Path) -> StepHealthSummary | None:
    """Read a :class:`StepHealthSummary` previously written by :func:`write_step_health_summary`.

    Returns ``None`` if the file does not exist or is unreadable. Callers
    should treat a missing summary as "no health information" and fall back
    to the sub-process exit code.
    """

    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    status = str(payload.get("status", "") or "")
    if status not in STEP_HEALTH_STATUSES:
        return None
    return StepHealthSummary(
        status=status,
        reason=str(payload.get("reason", "") or ""),
        total_records=int(payload.get("total_records", 0) or 0),
        success_records=int(payload.get("success_records", 0) or 0),
        failed_records=int(payload.get("failed_records", 0) or 0),
        skipped_records=int(payload.get("skipped_records", 0) or 0),
        failed_record_ratio=float(payload.get("failed_record_ratio", 0.0) or 0.0),
        failed_codes=tuple(str(item) for item in payload.get("failed_codes", []) or []),
        failed_code_ratio=float(payload.get("failed_code_ratio", 0.0) or 0.0),
        failed_datasets=tuple(str(item) for item in payload.get("failed_datasets", []) or []),
        fatal_datasets=tuple(str(item) for item in payload.get("fatal_datasets", []) or []),
        fatal_statuses=tuple(str(item) for item in payload.get("fatal_statuses", []) or []),
        examples=tuple(dict(item) for item in payload.get("examples", []) or []),
        policy=dict(payload.get("policy", {}) or {}),
    )


__all__ = [
    "DEFAULT_FATAL_STATUSES",
    "DEGRADED_SUCCESS_STATUSES",
    "FATAL_TERMINAL_STATUSES",
    "FAILURE_STATUSES",
    "METADATA_DATASET",
    "SKIPPED_STATUSES",
    "STEP_HEALTH_STATUSES",
    "STEP_HEALTH_SCHEMA_VERSION",
    "SUCCESS_STATUSES",
    "StepHealthPolicy",
    "StepHealthSummary",
    "akshare_daily_bar_policy",
    "akshare_valuation_full_policy",
    "baostock_market_session_policy",
    "evaluate_step_health",
    "failed_record_examples",
    "is_degraded_success_status",
    "is_failure_status",
    "is_fatal_terminal_status",
    "is_skipped_status",
    "is_success_status",
    "read_step_health_summary",
    "strict_policy",
    "write_step_health_summary",
]
