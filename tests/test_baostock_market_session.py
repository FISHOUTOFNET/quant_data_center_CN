from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import yaml
from click.testing import CliRunner

import src.cli as cli_module
import src.commands.baostock as baostock_commands
from src.pipeline.common import DAILY_BAR_DATASETS
from src.sources.baostock.adjustments import (
    BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET,
    UNADJUSTED_DAILY_DATASET,
)
from src.sources.baostock.market_session import should_run_adjusted_market_session
from src.sources.baostock.market_session_manifest import build_baostock_market_session_manifest
from src.sources.baostock.update_daily_frames import _needs_baostock_cn_stock_adjustment_factors
from src.sources.baostock.update_daily_targets import (
    BAOSTOCK_MARKET_SESSION_DAILY_TARGET,
    _dataset_targets,
)
from src.tools import run_update_daily

REPO_ROOT = Path(__file__).resolve().parents[1]
OLD_BAOSTOCK_STEP_IDS = {
    "baostock-unadjusted",
    "baostock-adjustment-factor",
    "baostock-qfq",
    "baostock-hfq",
}


def test_baostock_market_session_target_updates_daily_bars_without_metadata_targets() -> None:
    include_calendar, include_basic, include_factor, daily_targets = _dataset_targets(
        BAOSTOCK_MARKET_SESSION_DAILY_TARGET
    )

    assert include_calendar is False
    assert include_basic is False
    assert include_factor is False
    assert set(daily_targets) == set(DAILY_BAR_DATASETS)
    assert UNADJUSTED_DAILY_DATASET in daily_targets
    assert "baostock_cn_stock_daily_bar_qfq" in daily_targets
    assert "baostock_cn_stock_daily_bar_hfq" in daily_targets


def test_baostock_market_session_target_still_needs_factors_for_adjusted_daily_bars() -> None:
    _, _, include_factor, daily_targets = _dataset_targets(BAOSTOCK_MARKET_SESSION_DAILY_TARGET)

    assert include_factor is False
    assert _needs_baostock_cn_stock_adjustment_factors(daily_targets) is True


def test_should_run_adjusted_market_session_matches_orchestrator_market_window_policy() -> None:
    cases = [
        (date(2026, 6, 8), date(2026, 6, 8), date(2026, 6, 8), False, False),
        (date(2026, 6, 9), date(2026, 6, 9), date(2026, 6, 9), False, False),
        (date(2026, 6, 10), date(2026, 6, 10), date(2026, 6, 10), False, False),
        (date(2026, 6, 11), date(2026, 6, 11), date(2026, 6, 11), False, False),
        (date(2026, 6, 12), date(2026, 6, 12), date(2026, 6, 12), False, True),
        (date(2026, 6, 13), date(2026, 6, 12), date(2026, 6, 12), False, True),
        (date(2026, 6, 14), date(2026, 6, 12), date(2026, 6, 12), False, True),
        (date(2026, 6, 10), date(2026, 6, 11), date(2026, 6, 10), False, True),
        (date(2026, 6, 10), date(2026, 6, 10), date(2026, 6, 10), True, True),
    ]
    for natural, candidate, market, overridden, expected in cases:
        effective_dates = run_update_daily.DailyEffectiveDates(
            natural_date=natural,
            candidate_date=candidate,
            market_date=market,
            hist_start=market,
            market_date_overridden=overridden,
        )

        assert should_run_adjusted_market_session(natural, candidate, market, overridden) is expected
        assert (
            should_run_adjusted_market_session(natural, candidate, market, overridden)
            == run_update_daily._schedule_policy_matches("market_window", ["all"], effective_dates)
        )


def test_update_baostock_market_session_cli_uses_unadjusted_dataset_on_regular_trading_day(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_update_daily(**kwargs):
        captured.update(kwargs)
        return [{"dataset": UNADJUSTED_DAILY_DATASET, "code": "sh.600000", "status": "success", "row_count": 1}]

    monkeypatch.setattr(baostock_commands, "run_update_daily", fake_update_daily)
    monkeypatch.setattr(baostock_commands, "write_baostock_market_session_manifest", lambda *args, **kwargs: {})

    result = CliRunner().invoke(
        cli_module.cli,
        [
            "update-baostock-market-session",
            "--end",
            "2026-06-10",
            "--natural-date",
            "2026-06-10",
            "--candidate-date",
            "2026-06-10",
            "--market-date",
            "2026-06-10",
            "--code",
            "sh.600000",
            "--no-build-duckdb-views",
        ],
    )

    assert result.exit_code == 0
    assert captured["dataset"] == UNADJUSTED_DAILY_DATASET
    assert captured["dataset"] != BAOSTOCK_MARKET_SESSION_DAILY_TARGET
    assert captured["dataset"] != "all"
    assert captured["code"] == ("sh.600000",)
    assert captured["build_views"] is False


def test_update_baostock_market_session_cli_uses_market_session_target_in_market_window(monkeypatch) -> None:
    captured: dict[str, object] = {}
    manifest_args: dict[str, object] = {}

    def fake_update_daily(**kwargs):
        captured.update(kwargs)
        return [
            {
                "dataset": "baostock_cn_stock_daily_bar_qfq",
                "code": "sh.600000",
                "status": "success",
                "row_count": 1,
            }
        ]

    def fake_manifest(records, **kwargs):
        manifest_args.update(kwargs)
        manifest_args["records"] = records
        return {}

    monkeypatch.setattr(baostock_commands, "run_update_daily", fake_update_daily)
    monkeypatch.setattr(baostock_commands, "write_baostock_market_session_manifest", fake_manifest)

    result = CliRunner().invoke(
        cli_module.cli,
        [
            "update-baostock-market-session",
            "--end",
            "2026-06-12",
            "--natural-date",
            "2026-06-12",
            "--candidate-date",
            "2026-06-12",
            "--market-date",
            "2026-06-12",
            "--code",
            "sh.600000",
            "--no-build-duckdb-views",
        ],
    )

    assert result.exit_code == 0
    assert captured["dataset"] == BAOSTOCK_MARKET_SESSION_DAILY_TARGET
    assert captured["dataset"] != "all"
    assert manifest_args["session_mode"] == "adjusted_market_session"
    assert manifest_args["market_date"] == "2026-06-12"


def test_daily_workflow_uses_single_baostock_market_session_step() -> None:
    raw = yaml.safe_load((REPO_ROOT / "config" / "daily_workflow.yaml").read_text(encoding="utf-8"))
    steps = raw["steps"]
    by_id = {step["id"]: step for step in steps}

    assert not (OLD_BAOSTOCK_STEP_IDS & set(by_id))
    assert "baostock-market-session" in by_id
    market_session = by_id["baostock-market-session"]
    assert market_session["schedule_policy"] == "daily"
    assert market_session["state_key_policy"] == "market_date"
    assert market_session["resume_policy"] == "always_run"
    assert [item["step"] for item in market_session["depends_on"]] == ["baostock-basic"]
    assert list(by_id).index("baostock-basic") < list(by_id).index("baostock-market-session")

    valuation_deps = [item["step"] for item in by_id["baostock-valuation-percentile"]["depends_on"]]
    build_deps = [item["step"] for item in by_id["build-derived"]["depends_on"]]
    assert valuation_deps == ["baostock-market-session"]
    assert "baostock-market-session" in build_deps
    assert not (OLD_BAOSTOCK_STEP_IDS & set(build_deps))


def test_default_daily_workflow_config_matches_market_session_shape() -> None:
    steps = run_update_daily.DEFAULT_DAILY_WORKFLOW_CONFIG["steps"]
    by_id = {step["id"]: step for step in steps}

    assert not (OLD_BAOSTOCK_STEP_IDS & set(by_id))
    assert json.dumps(run_update_daily.DEFAULT_DAILY_WORKFLOW_CONFIG, ensure_ascii=False).find(
        "baostock-unadjusted"
    ) == -1
    market_session = by_id["baostock-market-session"]
    assert market_session["schedule_policy"] == "daily"
    assert market_session["resume_policy"] == "always_run"
    assert market_session["depends_on"] == ["baostock-basic"]
    assert by_id["baostock-valuation-percentile"]["depends_on"] == ["baostock-market-session"]
    assert "baostock-market-session" in by_id["build-derived"]["depends_on"]


def test_baostock_market_session_manifest_aggregates_records_without_changed_alias() -> None:
    records = [
        {"dataset": UNADJUSTED_DAILY_DATASET, "code": "sh.600000", "status": "success", "row_count": 3},
        {"dataset": "baostock_cn_stock_daily_bar_qfq", "code": "sh.600000", "status": "skipped_checkpoint"},
        {"dataset": UNADJUSTED_DAILY_DATASET, "code": "sz.000001", "status": "skipped_existing"},
        {"dataset": "baostock_cn_stock_daily_bar_hfq", "code": "sz.000002", "status": "failed_api"},
        {"dataset": BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET, "code": "sz.000002", "status": "failed"},
        {"dataset": BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET, "code": "sh.600000", "status": "success"},
        {"dataset": "baostock_cn_trading_calendar", "code": "*", "status": "success"},
    ]

    manifest = build_baostock_market_session_manifest(
        records,
        market_date="2026-06-12",
        session_mode="adjusted_market_session",
        started_at="2026-06-12T18:00:00",
        ended_at="2026-06-12T18:01:00",
    )

    assert manifest["session_mode"] == "adjusted_market_session"
    assert BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET in manifest["datasets"]
    assert manifest["processed_codes"] == ["sh.600000", "sz.000001", "sz.000002"]
    assert manifest["succeeded_codes"] == ["sh.600000"]
    assert manifest["failed_codes"] == ["sz.000002"]
    assert manifest["skipped_codes"] == ["sz.000001"]
    assert manifest["changed_codes"] == []
    assert "*" not in manifest["processed_codes"]
    assert BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET in manifest["success_datasets_by_code"]["sh.600000"]
    assert BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET in manifest["failed_datasets_by_code"]["sz.000002"]
    assert manifest["skipped_datasets_by_code"]["sz.000001"] == [UNADJUSTED_DAILY_DATASET]
    assert manifest["record_count"] == len(records)
