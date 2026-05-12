"""Shared helpers for non-mutating pipeline dry-run planning."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, TypeVar


T = TypeVar("T")


def apply_limit(items: Iterable[T], limit: int | None, label: str) -> list[T]:
    """Return items capped by a non-negative optional limit."""

    values = list(items)
    if limit is None:
        return values
    resolved = int(limit)
    if resolved < 0:
        raise ValueError(f"{label} limit must be non-negative")
    return values[:resolved]


def dry_run_record(
    dataset: str,
    code: str,
    start_date: str | None = None,
    end_date: str | None = None,
    output_path: str | Path | None = None,
    operation: str = "write",
    message: str = "",
    **extra: object,
) -> dict[str, object]:
    return _record(
        "dry_run",
        dataset,
        code,
        start_date,
        end_date,
        output_path,
        operation,
        message,
        **extra,
    )


def blocked_record(
    dataset: str,
    code: str,
    start_date: str | None = None,
    end_date: str | None = None,
    output_path: str | Path | None = None,
    operation: str = "write",
    message: str = "",
    **extra: object,
) -> dict[str, object]:
    return _record(
        "dry_run_blocked",
        dataset,
        code,
        start_date,
        end_date,
        output_path,
        operation,
        message,
        **extra,
    )


def _record(
    status: str,
    dataset: str,
    code: str,
    start_date: str | None,
    end_date: str | None,
    output_path: str | Path | None,
    operation: str,
    message: str,
    **extra: object,
) -> dict[str, object]:
    record: dict[str, object] = {
        "dataset": dataset,
        "code": code,
        "status": status,
        "row_count": 0,
        "start_date": start_date,
        "end_date": end_date,
        "output_path": "" if output_path is None else str(output_path),
        "operation": operation,
        "message": message,
    }
    record.update(extra)
    return record
