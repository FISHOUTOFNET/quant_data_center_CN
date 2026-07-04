"""Tests for the unified step health evaluation layer."""

from __future__ import annotations

import json
from pathlib import Path

from src.pipeline.step_health import (
    METADATA_DATASET,
    StepHealthPolicy,
    akshare_daily_bar_policy,
    akshare_valuation_full_policy,
    baostock_market_session_policy,
    evaluate_step_health,
    failed_record_examples,
    read_step_health_summary,
    strict_policy,
    write_step_health_summary,
)


def _success(dataset: str = "baostock_cn_stock_daily_bar_unadjusted", code: str = "sh.600000") -> dict[str, object]:
    return {"dataset": dataset, "code": code, "status": "success", "row_count": 1, "error_stack": ""}


def _failed(dataset: str = "baostock_cn_stock_daily_bar_unadjusted", code: str = "sh.600000", status: str = "failed") -> dict[str, object]:
    return {"dataset": dataset, "code": code, "status": status, "row_count": 0, "error_stack": "boom"}


def _skipped(dataset: str = "baostock_cn_stock_daily_bar_unadjusted", code: str = "sh.600000") -> dict[str, object]:
    return {"dataset": dataset, "code": code, "status": "skipped", "row_count": 0, "error_stack": ""}


def test_all_success_records_yield_success() -> None:
    records = [_success(code=f"sh.60000{i}") for i in range(5)]
    summary = evaluate_step_health(records, baostock_market_session_policy())
    assert summary.status == "success"
    assert summary.failed_records == 0
    assert summary.success_records == 5
    assert summary.total_records == 5
    assert summary.failed_codes == ()
    assert summary.reason.startswith("all 5 record(s) succeeded")


def test_few_failed_records_below_threshold_yield_success_degraded() -> None:
    records = [_success(code=f"sh.60000{i}") for i in range(200)]
    records.append(_failed(code="sh.699999"))
    summary = evaluate_step_health(records, baostock_market_session_policy())
    assert summary.status == "success_degraded"
    assert summary.failed_records == 1
    assert summary.success_records == 200
    assert summary.failed_codes == ("sh.699999",)
    assert "within policy tolerance" in summary.reason


def test_failed_records_exceeding_max_failed_records_yields_failed() -> None:
    records = [_success(code=f"sh.60000{i}") for i in range(100)]
    for i in range(51):
        records.append(_failed(code=f"sh.6999{i:02d}"))
    summary = evaluate_step_health(records, baostock_market_session_policy())
    assert summary.status == "failed"
    assert summary.failed_records == 51
    assert "exceeds max_failed_records=50" in summary.reason


def test_failed_record_ratio_exceeding_threshold_yields_failed() -> None:
    records = [_success(code=f"sh.60000{i}") for i in range(100)]
    # 5 failed out of 105 = ~4.76% > 1% threshold
    for i in range(5):
        records.append(_failed(code=f"sh.6999{i}"))
    summary = evaluate_step_health(records, baostock_market_session_policy())
    assert summary.status == "failed"
    assert "failed record ratio" in summary.reason
    assert "max_failed_record_ratio=0.01" in summary.reason


def test_failed_code_count_exceeding_threshold_yields_failed() -> None:
    policy = StepHealthPolicy(
        require_any_success=True,
        max_failed_records=1000,
        max_failed_record_ratio=1.0,
        max_failed_codes=5,
        max_failed_code_ratio=1.0,
    )
    records = [_success(code=f"sh.60000{i}") for i in range(100)]
    for i in range(6):
        records.append(_failed(code=f"sh.6999{i}"))
    summary = evaluate_step_health(records, policy)
    assert summary.status == "failed"
    assert "exceeds max_failed_codes=5" in summary.reason


def test_failed_code_ratio_exceeding_threshold_yields_failed() -> None:
    policy = StepHealthPolicy(
        require_any_success=True,
        max_failed_records=1000,
        max_failed_record_ratio=1.0,
        max_failed_codes=1000,
        max_failed_code_ratio=0.01,
    )
    records = [_success(code=f"sh.60000{i}") for i in range(100)]
    # 3 failed codes out of 103 total codes = ~2.91% > 1%
    for i in range(3):
        records.append(_failed(code=f"sh.6999{i}"))
    summary = evaluate_step_health(records, policy)
    assert summary.status == "failed"
    assert "failed code ratio" in summary.reason
    assert "max_failed_code_ratio=0.01" in summary.reason


def test_metadata_dataset_failure_is_fatal() -> None:
    records = [_success(code="sh.600000"), _failed(dataset=METADATA_DATASET, code="*")]
    summary = evaluate_step_health(records, baostock_market_session_policy())
    assert summary.status == "failed"
    assert "fatal dataset failure" in summary.reason
    assert METADATA_DATASET in summary.fatal_datasets


def test_baostock_calendar_failure_is_fatal() -> None:
    records = [
        _success(code="sh.600000"),
        _failed(dataset="baostock_cn_trading_calendar", code="*"),
    ]
    summary = evaluate_step_health(records, baostock_market_session_policy())
    assert summary.status == "failed"
    assert "baostock_cn_trading_calendar" in summary.fatal_datasets


def test_baostock_stock_basic_failure_is_fatal() -> None:
    records = [
        _success(code="sh.600000"),
        _failed(dataset="baostock_cn_stock_basic", code="*"),
    ]
    summary = evaluate_step_health(records, baostock_market_session_policy())
    assert summary.status == "failed"
    assert "baostock_cn_stock_basic" in summary.fatal_datasets


def test_fatal_status_failure_is_fatal() -> None:
    records = [
        _success(code="sh.600000"),
        _failed(code="sh.699999", status="failed_resource_locked"),
    ]
    summary = evaluate_step_health(records, baostock_market_session_policy())
    assert summary.status == "failed"
    assert "failed_resource_locked" in summary.fatal_statuses


def test_failed_timeout_cleanup_is_fatal() -> None:
    records = [
        _success(code="sh.600000"),
        _failed(code="sh.699999", status="failed_timeout_cleanup"),
    ]
    summary = evaluate_step_health(records, baostock_market_session_policy())
    assert summary.status == "failed"
    assert "failed_timeout_cleanup" in summary.fatal_statuses


def test_require_any_success_with_no_success_yields_failed() -> None:
    records = [_failed(code="sh.600000"), _failed(code="sh.600001")]
    summary = evaluate_step_health(records, baostock_market_session_policy())
    assert summary.status == "failed"
    assert "no successful records" in summary.reason
    assert "require_any_success=True" in summary.reason


def test_strict_policy_any_failed_record_yields_failed() -> None:
    records = [_success(code="sh.600000"), _failed(code="sh.600001")]
    summary = evaluate_step_health(records, strict_policy())
    assert summary.status == "failed"
    # strict policy has max_failed_records=0
    assert "exceeds max_failed_records=0" in summary.reason


def test_strict_policy_all_success_yields_success() -> None:
    records = [_success(code="sh.600000"), _success(code="sh.600001")]
    summary = evaluate_step_health(records, strict_policy())
    assert summary.status == "success"


def test_empty_records_with_require_any_success_yields_failed() -> None:
    summary = evaluate_step_health([], baostock_market_session_policy())
    assert summary.status == "failed"
    assert "no successful records" in summary.reason


def test_empty_records_without_require_any_success_yields_success() -> None:
    policy = StepHealthPolicy(require_any_success=False)
    summary = evaluate_step_health([], policy)
    assert summary.status == "success"


def test_failed_record_examples_limits_to_five() -> None:
    records = [_failed(code=f"sh.60000{i}") for i in range(10)]
    examples = failed_record_examples(records, limit=5)
    assert len(examples) == 5
    assert all("dataset" in ex and "code" in ex and "status" in ex for ex in examples)


def test_failed_record_examples_truncates_stack() -> None:
    long_stack = "\n".join(f"line {i}" for i in range(20))
    records = [{"dataset": "ds", "code": "c", "status": "failed", "error_stack": long_stack}]
    examples = failed_record_examples(records, limit=5)
    assert len(examples) == 1
    # detail should contain at most 3 lines
    assert examples[0]["detail"].count(" | ") <= 2


def test_akshare_valuation_full_policy_thresholds() -> None:
    policy = akshare_valuation_full_policy()
    assert policy.max_failed_records == 20
    assert policy.max_failed_record_ratio == 0.005
    assert policy.max_failed_codes == 10
    assert policy.max_failed_code_ratio == 0.005
    assert METADATA_DATASET in policy.fatal_datasets


def test_akshare_daily_bar_policy_thresholds() -> None:
    policy = akshare_daily_bar_policy()
    assert policy.max_failed_records == 50
    assert policy.max_failed_record_ratio == 0.01
    assert policy.max_failed_codes == 10
    assert policy.max_failed_code_ratio == 0.005
    assert METADATA_DATASET in policy.fatal_datasets


def test_baostock_market_session_policy_thresholds() -> None:
    policy = baostock_market_session_policy()
    assert policy.max_failed_records == 50
    assert policy.max_failed_record_ratio == 0.01
    assert policy.max_failed_codes == 10
    assert policy.max_failed_code_ratio == 0.005
    assert "baostock_cn_trading_calendar" in policy.fatal_datasets
    assert "baostock_cn_stock_basic" in policy.fatal_datasets


def test_write_and_read_step_health_summary_roundtrip(tmp_path: Path) -> None:
    records = [_success(code="sh.600000"), _failed(code="sh.699999")]
    summary = evaluate_step_health(records, baostock_market_session_policy())
    path = tmp_path / "summary.json"
    write_step_health_summary(path, summary)
    assert path.exists()
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 1
    assert payload["status"] == summary.status

    restored = read_step_health_summary(path)
    assert restored is not None
    assert restored.status == summary.status
    assert restored.failed_records == summary.failed_records
    assert restored.failed_codes == summary.failed_codes
    assert restored.reason == summary.reason


def test_read_step_health_summary_missing_file_returns_none(tmp_path: Path) -> None:
    assert read_step_health_summary(tmp_path / "nope.json") is None


def test_read_step_health_summary_corrupt_file_returns_none(tmp_path: Path) -> None:
    path = tmp_path / "corrupt.json"
    path.write_text("{not json", encoding="utf-8")
    assert read_step_health_summary(path) is None


def test_read_step_health_summary_invalid_status_returns_none(tmp_path: Path) -> None:
    path = tmp_path / "bad.json"
    path.write_text(json.dumps({"status": "bogus", "reason": ""}), encoding="utf-8")
    assert read_step_health_summary(path) is None


def test_skipped_records_do_not_count_as_failed() -> None:
    records = [_success(code="sh.600000"), _skipped(code="sh.600001")]
    summary = evaluate_step_health(records, baostock_market_session_policy())
    assert summary.status == "success"
    assert summary.skipped_records == 1
    assert summary.failed_records == 0


def test_failed_prefix_status_treated_as_failed() -> None:
    records = [
        _success(code="sh.600000"),
        {"dataset": "ds", "code": "sh.699999", "status": "failed_validation", "error_stack": "x"},
    ]
    summary = evaluate_step_health(records, strict_policy())
    assert summary.status == "failed"
    assert summary.failed_records == 1


def test_policy_to_summary_dict_is_serializable() -> None:
    policy = baostock_market_session_policy()
    payload = policy.to_summary_dict()
    assert isinstance(payload["fatal_datasets"], list)
    assert isinstance(payload["fatal_statuses"], list)
    # Should be JSON serializable
    json.dumps(payload)


def test_summary_to_dict_is_serializable() -> None:
    records = [_success(code="sh.600000"), _failed(code="sh.699999")]
    summary = evaluate_step_health(records, baostock_market_session_policy())
    payload = summary.to_dict()
    assert isinstance(payload["failed_codes"], list)
    assert isinstance(payload["examples"], list)
    json.dumps(payload)
