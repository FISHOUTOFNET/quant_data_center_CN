"""Helpers for CLI commands that print pipeline records."""

from __future__ import annotations

import os
from pathlib import Path

import click

from src.pipeline.step_health import (
    StepHealthPolicy,
    StepHealthSummary,
    evaluate_step_health,
    is_failure_status,
    read_step_health_summary,
    strict_policy,
    write_step_health_summary,
)

STEP_RESULT_PATH_ENV = "QDC_STEP_RESULT_PATH"


def echo_pipeline_records(records: list[dict[str, object]]) -> None:
    """Print records defensively without hiding failed statuses."""

    for item in records:
        click.echo(
            f"{item.get('dataset', '<unknown>')} {item.get('code', '*')} "
            f"status={item.get('status', '<unknown>')} rows={item.get('row_count', item.get('rows', 0))}"
        )


def raise_for_failed_records(records: list[dict[str, object]], *, label: str) -> None:
    """Raise ClickException if any record has a failed or failed_* status.

    Kept for backward compatibility with callers that have not migrated to
    :func:`finalize_pipeline_records`. New code should call
    :func:`finalize_pipeline_records` with an explicit :class:`StepHealthPolicy`.
    """

    failed = [item for item in records if _is_failed_status(item.get("status"))]
    if not failed:
        return

    examples = _format_failed_examples(failed)
    raise click.ClickException(f"{label} completed with {len(failed)} failed record(s): " + "; ".join(examples))


def finalize_pipeline_records(
    records: list[dict[str, object]],
    *,
    label: str,
    policy: StepHealthPolicy | None = None,
    result_path: str | Path | None = None,
) -> StepHealthSummary:
    """Evaluate records under ``policy`` and apply CLI exit semantics.

    Behavior:

    * ``policy=None`` (default) uses :func:`strict_policy`, so any failed
      record raises :class:`click.ClickException` and the CLI exits non-zero.
      This preserves the historical strict behavior for build-derived, plain
      baostock daily updates, and other unconfigured commands.
    * For tolerant policies, a small number of failed records (below the
      policy thresholds) is converted to ``success_degraded`` and the CLI
      exits 0. Records are still printed and the summary is written so the
      orchestrator can capture the degraded reason.
    * Fatal dataset / fatal status / threshold-exceeded failures always
      raise :class:`click.ClickException`.
    * The summary is written to ``result_path`` if provided, or to the path
      referenced by the ``QDC_STEP_RESULT_PATH`` environment variable when
      set. The orchestrator reads this JSON to decide the step status.

    Returns the :class:`StepHealthSummary` for callers (e.g. manifests).
    """

    resolved_policy = policy or strict_policy()
    summary = evaluate_step_health(records, resolved_policy)
    resolved_result_path = _resolve_result_path(result_path)
    if resolved_result_path is not None:
        try:
            write_step_health_summary(resolved_result_path, summary)
        except OSError:
            # The summary file is a hint for the orchestrator; if it cannot
            # be written we must not abort the command. The orchestrator
            # will fall back to the sub-process exit code.
            pass

    if summary.status == "failed":
        examples = _format_failed_examples(records)
        suffix = f": {'; '.join(examples)}" if examples else ""
        raise click.ClickException(
            f"{label} completed with status=failed: {summary.reason}{suffix}"
        )
    if summary.status == "success_degraded":
        click.echo(
            f"{label} completed with status=success_degraded "
            f"({summary.failed_records}/{summary.total_records} failed records, "
            f"{len(summary.failed_codes)} failed code(s)): {summary.reason}"
        )
        if summary.failed_codes:
            sample = ", ".join(summary.failed_codes[:10])
            click.echo(f"Failed codes sample: {sample}")
        if resolved_result_path is not None:
            click.echo(f"Step health summary: {resolved_result_path}")
    return summary


def raise_for_unhealthy_records(
    records: list[dict[str, object]],
    *,
    label: str,
    policy: StepHealthPolicy | None = None,
    result_path: str | Path | None = None,
) -> StepHealthSummary:
    """Alias for :func:`finalize_pipeline_records` for callers that prefer the
    "raise if unhealthy" naming.
    """

    return finalize_pipeline_records(records, label=label, policy=policy, result_path=result_path)


def load_step_health_summary(path: str | Path | None = None) -> StepHealthSummary | None:
    """Read a step health summary previously written by :func:`finalize_pipeline_records`.

    Mostly useful for tests that exercise the orchestrator's read path.
    """

    resolved = _resolve_result_path(path)
    if resolved is None or not resolved.exists():
        return None
    return read_step_health_summary(resolved)


def _resolve_result_path(path: str | Path | None) -> Path | None:
    if path is not None:
        return Path(path)
    env_value = os.environ.get(STEP_RESULT_PATH_ENV)
    if env_value:
        return Path(env_value)
    return None


def _format_failed_examples(records: list[dict[str, object]]) -> list[str]:
    examples: list[str] = []
    for item in records:
        if not _is_failed_status(item.get("status")):
            continue
        dataset = item.get("dataset", "<unknown>")
        code = item.get("code", "*")
        stack = str(item.get("error_stack", "") or "").splitlines()
        detail = stack[:3]
        suffix = f": {' | '.join(detail)}" if detail else ""
        examples.append(f"{dataset}/{code} status={item.get('status')}{suffix}")
        if len(examples) >= 5:
            break
    failed_total = sum(1 for item in records if _is_failed_status(item.get("status")))
    if failed_total > len(examples):
        examples.append(f"... {failed_total - len(examples)} more")
    return examples


def _is_failed_status(value: object) -> bool:
    """Delegate to the unified authority in :mod:`src.pipeline.step_health`.

    Kept as a private alias so existing call sites in this module do not need
    to change. New code should import :func:`is_failure_status` directly from
    :mod:`src.pipeline.step_health`.
    """

    return is_failure_status(value)
