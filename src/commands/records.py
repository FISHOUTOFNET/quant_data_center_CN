"""Helpers for CLI commands that print pipeline records."""

from __future__ import annotations

import click


def echo_pipeline_records(records: list[dict[str, object]]) -> None:
    """Print records defensively without hiding failed statuses."""

    for item in records:
        click.echo(
            f"{item.get('dataset', '<unknown>')} {item.get('code', '*')} "
            f"status={item.get('status', '<unknown>')} rows={item.get('row_count', item.get('rows', 0))}"
        )


def raise_for_failed_records(records: list[dict[str, object]], *, label: str) -> None:
    """Raise ClickException if any record has a failed or failed_* status."""

    failed = [item for item in records if _is_failed_status(item.get("status"))]
    if not failed:
        return

    examples = []
    for item in failed[:5]:
        dataset = item.get("dataset", "<unknown>")
        code = item.get("code", "*")
        stack = str(item.get("error_stack", "") or "").splitlines()
        detail = stack[:3]
        suffix = f": {' | '.join(detail)}" if detail else ""
        examples.append(f"{dataset}/{code} status={item.get('status')}{suffix}")
    if len(failed) > len(examples):
        examples.append(f"... {len(failed) - len(examples)} more")
    raise click.ClickException(f"{label} completed with {len(failed)} failed record(s): " + "; ".join(examples))


def _is_failed_status(value: object) -> bool:
    status = str(value or "")
    return status == "failed" or status.startswith("failed_")
