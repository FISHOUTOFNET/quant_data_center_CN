import pytest
from click.testing import CliRunner

import src.cli as cli_module
from src.sources.akshare.pipeline import AkShareUpdateRequest
from src.sources.akshare.pipeline.registry import (
    modules_for_target,
    target_choices,
    validate_request_target_options,
)

EXPECTED_TARGETS = [
    "valuation",
    "capital_structure",
    "delist",
    "spot_quote",
    "report_disclosure",
    "yysj_em",
    "yjyg_em",
    "financial_report",
    "daily_bar",
]


def test_akshare_target_choices_are_ordered_and_include_all_only_when_requested() -> None:
    assert target_choices(include_all=True) == [*EXPECTED_TARGETS, "all"]
    assert target_choices(include_all=False) == EXPECTED_TARGETS


def test_akshare_cli_target_choices_are_read_from_registry() -> None:
    result = CliRunner().invoke(cli_module.cli, ["akshare", "update", "--help"])

    assert result.exit_code == 0
    positions = [result.output.index(choice) for choice in target_choices(include_all=True)]
    assert positions == sorted(positions)


def test_modules_for_all_preserves_execution_order() -> None:
    modules = list(modules_for_target("all"))

    assert [module.target for module in modules] == EXPECTED_TARGETS


def test_modules_for_target_rejects_unknown_target() -> None:
    with pytest.raises(ValueError, match="Unsupported AkShare update target: unknown"):
        list(modules_for_target("unknown"))


@pytest.mark.parametrize("target", ["daily_bar"])
def test_adjustment_is_allowed_for_daily_bar(target: str) -> None:
    validate_request_target_options(AkShareUpdateRequest(target=target, adjustment="qfq"))


@pytest.mark.parametrize("target", ["valuation", "all"])
def test_adjustment_is_rejected_for_other_targets(target: str) -> None:
    with pytest.raises(ValueError, match="--adjustment is only valid for --target daily_bar"):
        validate_request_target_options(AkShareUpdateRequest(target=target, adjustment="qfq"))


@pytest.mark.parametrize("target", ["report_disclosure", "yysj_em", "yjyg_em"])
def test_period_is_allowed_for_period_targets(target: str) -> None:
    validate_request_target_options(AkShareUpdateRequest(target=target, period=("2025年报",)))


@pytest.mark.parametrize("target", ["valuation", "daily_bar", "all"])
def test_period_is_rejected_for_other_targets(target: str) -> None:
    with pytest.raises(
        ValueError,
        match="--period is only valid for --target report_disclosure, yysj_em, or yjyg_em",
    ):
        validate_request_target_options(AkShareUpdateRequest(target=target, period=("2025年报",)))


@pytest.mark.parametrize("target", ["delist", "report_disclosure", "yysj_em"])
def test_market_is_allowed_for_market_targets(target: str) -> None:
    validate_request_target_options(AkShareUpdateRequest(target=target, market="沪深京"))


@pytest.mark.parametrize("target", ["valuation", "spot_quote", "yjyg_em", "all"])
def test_market_is_rejected_for_other_targets(target: str) -> None:
    with pytest.raises(
        ValueError,
        match="--market is only valid for --target delist, report_disclosure, or yysj_em",
    ):
        validate_request_target_options(AkShareUpdateRequest(target=target, market="沪深京"))


def test_validate_request_rejects_unknown_target() -> None:
    with pytest.raises(ValueError, match="Unsupported AkShare update target: unknown"):
        validate_request_target_options(AkShareUpdateRequest(target="unknown"))
