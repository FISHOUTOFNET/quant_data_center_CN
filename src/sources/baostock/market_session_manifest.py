"""Manifest helpers for the Baostock market-session command."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from src.pipeline.common import DAILY_BAR_DATASETS, date_iso
from src.pipeline.step_health import StepHealthSummary, read_step_health_summary
from src.sources.baostock.adjustments import (
    BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET,
    UNADJUSTED_DAILY_DATASET,
)
from src.utils.config_mgr import ConfigManager

PIPELINE_BAOSTOCK_MARKET_SESSION = "baostock_market_session"
MARKET_SESSION_MANIFEST_SCHEMA_VERSION = 2


def write_baostock_market_session_manifest(
    records: list[dict[str, object]],
    *,
    market_date: str,
    session_mode: str,
    started_at: datetime | str,
    ended_at: datetime | str,
    root: Path | None = None,
    health_summary: StepHealthSummary | None = None,
    health_summary_path: Path | None = None,
) -> dict[str, object]:
    """Write and return a compact manifest derived from update_daily records.

    Schema v2 adds step-level health verdict fields (``verdict``, ``reason``,
    ``failed_record_ratio``, ``failed_code_ratio``, ``failed_codes``,
    ``failed_datasets``, ``health_summary_path``, ``policy``). When
    ``health_summary`` is omitted the function still emits the legacy v1
    fields so older readers continue to work, and the new fields default to
    neutral values.
    """

    manifest = build_baostock_market_session_manifest(
        records,
        market_date=market_date,
        session_mode=session_mode,
        started_at=started_at,
        ended_at=ended_at,
        health_summary=health_summary,
        health_summary_path=health_summary_path,
    )
    config = ConfigManager(root)
    output_dir = config.root / "data" / "metadata" / "manifests" / PIPELINE_BAOSTOCK_MARKET_SESSION
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{manifest['market_date']}.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return manifest


def build_baostock_market_session_manifest(
    records: list[dict[str, object]],
    *,
    market_date: str,
    session_mode: str,
    started_at: datetime | str,
    ended_at: datetime | str,
    health_summary: StepHealthSummary | None = None,
    health_summary_path: Path | None = None,
) -> dict[str, object]:
    datasets = _manifest_datasets(records, session_mode)
    success_by_code: dict[str, list[str]] = {}
    failed_by_code: dict[str, list[str]] = {}
    skipped_by_code: dict[str, list[str]] = {}

    for record in records:
        code = str(record.get("code", "*") or "*")
        if code == "*":
            continue
        dataset = str(record.get("dataset", "") or "")
        if not dataset:
            continue
        status = str(record.get("status", "") or "")
        if status == "success":
            _append_dataset(success_by_code, code, dataset)
        elif _is_failed_status(status):
            _append_dataset(failed_by_code, code, dataset)
        elif status.startswith("skipped"):
            _append_dataset(skipped_by_code, code, dataset)

    processed_codes = sorted(set(success_by_code) | set(failed_by_code) | set(skipped_by_code))
    succeeded_codes = sorted(success_by_code)
    failed_codes = sorted(failed_by_code)
    skipped_codes = sorted(
        code for code in skipped_by_code if code not in success_by_code and code not in failed_by_code
    )

    summary = health_summary
    summary_path_text = _str_path(health_summary_path)
    if summary is None and summary_path_text is not None:
        summary = read_step_health_summary(Path(summary_path_text))

    manifest: dict[str, object] = {
        "schema_version": MARKET_SESSION_MANIFEST_SCHEMA_VERSION,
        "pipeline": PIPELINE_BAOSTOCK_MARKET_SESSION,
        "market_date": date_iso(market_date),
        "session_mode": session_mode,
        "started_at": _timestamp_text(started_at),
        "ended_at": _timestamp_text(ended_at),
        "datasets": datasets,
        "processed_codes": processed_codes,
        "succeeded_codes": succeeded_codes,
        "failed_codes": failed_codes,
        "skipped_codes": skipped_codes,
        "changed_codes": [],
        "success_datasets_by_code": _sorted_mapping(success_by_code),
        "failed_datasets_by_code": _sorted_mapping(failed_by_code),
        "skipped_datasets_by_code": _sorted_mapping(skipped_by_code),
        "record_count": len(records),
        "verdict": summary.status if summary is not None else "unknown",
        "reason": summary.reason if summary is not None else "",
        "failed_record_ratio": summary.failed_record_ratio if summary is not None else 0.0,
        "failed_code_ratio": summary.failed_code_ratio if summary is not None else 0.0,
        "failed_codes_step": list(summary.failed_codes) if summary is not None else [],
        "failed_datasets": list(summary.failed_datasets) if summary is not None else [],
        "health_summary_path": summary_path_text or "",
        "policy": summary.policy if summary is not None else {},
    }
    return manifest


def _manifest_datasets(records: list[dict[str, object]], session_mode: str) -> list[str]:
    if session_mode == "unadjusted_only":
        base = [UNADJUSTED_DAILY_DATASET]
    elif session_mode == "adjusted_market_session":
        base = list(DAILY_BAR_DATASETS)
    else:
        raise ValueError(f"Unsupported Baostock market-session mode: {session_mode}")

    actual = [str(record.get("dataset", "") or "") for record in records]
    ordered = list(base)
    for dataset in actual:
        if (dataset == BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET and dataset not in ordered) or (
            dataset in DAILY_BAR_DATASETS and dataset not in ordered
        ):
            ordered.append(dataset)
    return ordered


def _append_dataset(mapping: dict[str, list[str]], code: str, dataset: str) -> None:
    values = mapping.setdefault(code, [])
    if dataset not in values:
        values.append(dataset)


def _sorted_mapping(mapping: dict[str, list[str]]) -> dict[str, list[str]]:
    return {code: sorted(datasets) for code, datasets in sorted(mapping.items())}


def _str_path(path: Path | None) -> str | None:
    if path is None:
        return None
    return str(path)


def _timestamp_text(value: datetime | str) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _is_failed_status(value: object) -> bool:
    status = str(value or "")
    return status == "failed" or status.startswith("failed_")
