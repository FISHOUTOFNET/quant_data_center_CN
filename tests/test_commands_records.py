"""Tests for the policy-aware records finalization in src.commands.records."""

from __future__ import annotations

import json
import os
from pathlib import Path

import click
import pytest

from src.commands.records import (
    STEP_RESULT_PATH_ENV,
    finalize_pipeline_records,
    load_step_health_summary,
    raise_for_failed_records,
    raise_for_unhealthy_records,
)
from src.pipeline.step_health import (
    METADATA_DATASET,
    baostock_market_session_policy,
    strict_policy,
)


def _success(dataset: str = "baostock_cn_stock_daily_bar_unadjusted", code: str = "sh.600000") -> dict[str, object]:
    return {"dataset": dataset, "code": code, "status": "success", "row_count": 1, "error_stack": ""}


def _failed(
    dataset: str = "baostock_cn_stock_daily_bar_unadjusted", code: str = "sh.600000", status: str = "failed"
) -> dict[str, object]:
    return {"dataset": dataset, "code": code, "status": status, "row_count": 0, "error_stack": "boom"}


def test_finalize_strict_policy_raises_on_any_failed_record() -> None:
    records = [_success(), _failed(code="sh.699999")]
    with pytest.raises(click.ClickException, match="status=failed"):
        finalize_pipeline_records(records, label="test")


def test_finalize_tolerant_policy_returns_success_degraded(tmp_path: Path) -> None:
    records = [_success(code=f"sh.60000{i}") for i in range(200)]
    records.append(_failed(code="sh.699999"))
    summary = finalize_pipeline_records(
        records,
        label="test",
        policy=baostock_market_session_policy(),
        result_path=tmp_path / "summary.json",
    )
    assert summary.status == "success_degraded"
    assert summary.failed_records == 1


def test_finalize_tolerant_policy_fatal_dataset_still_raises(tmp_path: Path) -> None:
    records = [_success(), _failed(dataset=METADATA_DATASET, code="*")]
    with pytest.raises(click.ClickException, match="fatal dataset failure"):
        finalize_pipeline_records(
            records,
            label="test",
            policy=baostock_market_session_policy(),
            result_path=tmp_path / "summary.json",
        )


def test_finalize_tolerant_policy_threshold_exceeded_raises(tmp_path: Path) -> None:
    records = [_success(code=f"sh.60000{i}") for i in range(100)]
    for i in range(51):
        records.append(_failed(code=f"sh.6999{i:02d}"))
    with pytest.raises(click.ClickException, match="exceeds max_failed_records"):
        finalize_pipeline_records(
            records,
            label="test",
            policy=baostock_market_session_policy(),
            result_path=tmp_path / "summary.json",
        )


def test_finalize_writes_summary_to_result_path(tmp_path: Path) -> None:
    records = [_success(code=f"sh.60000{i}") for i in range(200)]
    records.append(_failed(code="sh.699999"))
    path = tmp_path / "summary.json"
    finalize_pipeline_records(
        records,
        label="test",
        policy=baostock_market_session_policy(),
        result_path=path,
    )
    assert path.exists()
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["status"] == "success_degraded"
    assert payload["failed_records"] == 1


def test_finalize_writes_summary_to_env_var_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    records = [_success(code=f"sh.60000{i}") for i in range(200)]
    records.append(_failed(code="sh.699999"))
    path = tmp_path / "env_summary.json"
    monkeypatch.setenv(STEP_RESULT_PATH_ENV, str(path))
    finalize_pipeline_records(records, label="test", policy=baostock_market_session_policy())
    assert path.exists()
    assert load_step_health_summary() is not None


def test_finalize_explicit_result_path_overrides_env_var(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    records = [_success(code=f"sh.60000{i}") for i in range(200)]
    records.append(_failed(code="sh.699999"))
    explicit_path = tmp_path / "explicit.json"
    env_path = tmp_path / "env.json"
    monkeypatch.setenv(STEP_RESULT_PATH_ENV, str(env_path))
    finalize_pipeline_records(
        records,
        label="test",
        policy=baostock_market_session_policy(),
        result_path=explicit_path,
    )
    assert explicit_path.exists()
    assert not env_path.exists()


def test_finalize_success_does_not_write_summary_unless_path_provided(tmp_path: Path) -> None:
    records = [_success(), _success(code="sh.600001")]
    path = tmp_path / "summary.json"
    summary = finalize_pipeline_records(
        records,
        label="test",
        policy=baostock_market_session_policy(),
        result_path=path,
    )
    assert summary.status == "success"
    # Summary is written even for success so orchestrator can confirm
    assert path.exists()


def test_raise_for_unhealthy_records_is_alias(tmp_path: Path) -> None:
    records = [_success(code=f"sh.60000{i}") for i in range(200)]
    records.append(_failed(code="sh.699999"))
    summary = raise_for_unhealthy_records(
        records,
        label="test",
        policy=baostock_market_session_policy(),
        result_path=tmp_path / "summary.json",
    )
    assert summary.status == "success_degraded"


def test_raise_for_failed_records_kept_for_backward_compat() -> None:
    records = [_success(), _failed(code="sh.699999")]
    with pytest.raises(click.ClickException, match="1 failed record"):
        raise_for_failed_records(records, label="legacy")


def test_raise_for_failed_records_passes_when_no_failures() -> None:
    records = [_success(), _success(code="sh.600001")]
    # Should not raise
    raise_for_failed_records(records, label="legacy")


def test_finalize_failure_message_contains_at_most_five_examples(tmp_path: Path) -> None:
    records = [_success(code="sh.600000")]
    for i in range(10):
        records.append(_failed(code=f"sh.6999{i}"))
    with pytest.raises(click.ClickException) as exc_info:
        finalize_pipeline_records(
            records,
            label="test",
            policy=baostock_market_session_policy(),
            result_path=tmp_path / "summary.json",
        )
    message = str(exc_info.value)
    # Should not contain all 10 codes in the message
    assert "more" in message


def test_finalize_clears_env_var_after_use(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    records = [_success(), _success(code="sh.600001")]
    monkeypatch.delenv(STEP_RESULT_PATH_ENV, raising=False)
    finalize_pipeline_records(records, label="test", policy=strict_policy())
    # Env var should not be set after call when it wasn't set before
    assert STEP_RESULT_PATH_ENV not in os.environ


def test_finalize_preserves_existing_env_var_after_use(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    records = [_success(), _success(code="sh.600001")]
    existing_path = tmp_path / "existing.json"
    monkeypatch.setenv(STEP_RESULT_PATH_ENV, str(existing_path))
    finalize_pipeline_records(records, label="test", policy=strict_policy())
    assert os.environ.get(STEP_RESULT_PATH_ENV) == str(existing_path)
