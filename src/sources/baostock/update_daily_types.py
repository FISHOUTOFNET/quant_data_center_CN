"""Shared types for the daily update pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class DailyTargetPlan:
    dataset: str
    code: str
    checkpoint_start_date: str
    end_date: str
    output_path: Path
    start_time: datetime


@dataclass(frozen=True)
class ApiFetchRequest:
    """Background-to-main request for provider calls.

    Provider APIs must stay on the main dispatcher thread. New per-code APIs
    should add request kinds here instead of calling providers from workers.
    """

    kind: str
    code: str
    start_date: str
    end_date: str
    datasets: tuple[str, ...]
    reason: str


@dataclass
class BackgroundTaskResult:
    run_records: list[dict[str, object]] = field(default_factory=list)
    api_requests: list[ApiFetchRequest] = field(default_factory=list)
