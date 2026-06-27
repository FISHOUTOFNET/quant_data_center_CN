from __future__ import annotations

import pytest
from click.testing import CliRunner

import src.cli as cli_module
import src.commands.derived as derived_commands


def test_build_derived_cli_passes_targets(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_build_derived(**kwargs):
        captured.update(kwargs)
        return [{"dataset": "cn_security_master", "status": "success", "rows": 1, "active": 1, "delisted": 0}]

    monkeypatch.setattr(derived_commands, "run_build_derived", fake_build_derived)

    result = CliRunner().invoke(
        cli_module.cli,
        ["build-derived", "--target", "security_master", "--no-build-duckdb-views"],
    )

    assert result.exit_code == 0
    assert captured["targets"] == ("security_master",)
    assert captured["exclude_targets"] == ()
    assert captured["include_security_master"] is True
    assert captured["mode"] == "incremental"
    assert captured["security_ids"] == ()
    assert captured["build_views"] is False
    assert "cn_security_master status=success rows=1 active=1 delisted=0" in result.output


def test_build_derived_cli_accepts_all_targets(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_build_derived(**kwargs):
        captured.update(kwargs)
        return [
            {"dataset": "cn_security_master", "status": "success", "rows": 1},
            {"dataset": "cn_stock_daily_bar", "status": "success", "rows": 2, "partitions": 1},
            {"dataset": "cn_stock_valuation", "status": "success", "rows": 2, "partitions": 1},
        ]

    monkeypatch.setattr(derived_commands, "run_build_derived", fake_build_derived)

    result = CliRunner().invoke(cli_module.cli, ["build-derived", "--target", "all", "--no-build-duckdb-views"])

    assert result.exit_code == 0
    assert captured["targets"] == ("all",)
    assert captured["exclude_targets"] == ()
    assert captured["mode"] == "incremental"
    assert "cn_stock_daily_bar status=success rows=2 partitions=1" in result.output


def test_build_derived_cli_accepts_mode_and_security_id(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_build_derived(**kwargs):
        captured.update(kwargs)
        return [{"dataset": "cn_stock_daily_bar", "status": "success", "rows": 2, "partitions": 1}]

    monkeypatch.setattr(derived_commands, "run_build_derived", fake_build_derived)

    result = CliRunner().invoke(
        cli_module.cli,
        [
            "build-derived",
            "--target",
            "daily_bar",
            "--security-id",
            "SH.600000",
            "--mode",
            "incremental",
            "--no-build-duckdb-views",
        ],
    )

    assert result.exit_code == 0
    assert captured["targets"] == ("daily_bar",)
    assert captured["exclude_targets"] == ()
    assert captured["mode"] == "incremental"
    assert captured["include_security_master"] is True
    assert captured["security_ids"] == ("SH.600000",)
    assert captured["build_views"] is False


def test_build_derived_cli_passes_exclude_targets(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_build_derived(**kwargs):
        captured.update(kwargs)
        return [{"dataset": "cn_security_master", "status": "success", "rows": 1}]

    monkeypatch.setattr(derived_commands, "run_build_derived", fake_build_derived)

    result = CliRunner().invoke(
        cli_module.cli,
        ["build-derived", "--target", "all", "--exclude-target", "daily_bar", "--no-build-duckdb-views"],
    )

    assert result.exit_code == 0
    assert captured["targets"] == ("all",)
    assert captured["exclude_targets"] == ("daily_bar",)


def test_build_derived_cli_accepts_no_include_security_master(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_build_derived(**kwargs):
        captured.update(kwargs)
        return [{"dataset": "cn_stock_daily_bar", "status": "success", "rows": 1}]

    monkeypatch.setattr(derived_commands, "run_build_derived", fake_build_derived)

    result = CliRunner().invoke(
        cli_module.cli,
        [
            "build-derived",
            "--target",
            "daily_bar",
            "--no-include-security-master",
            "--no-build-duckdb-views",
        ],
    )

    assert result.exit_code == 0
    assert captured["targets"] == ("daily_bar",)
    assert captured["include_security_master"] is False


def test_build_derived_cli_rejects_dataset_name_as_exclude_target() -> None:
    result = CliRunner().invoke(
        cli_module.cli,
        ["build-derived", "--exclude-target", "cn_stock_daily_bar"],
    )

    assert result.exit_code != 0
    assert "cn_stock_daily_bar" in result.output


@pytest.mark.parametrize("target", ["security_master", "valuation"])
def test_build_derived_cli_only_accepts_daily_bar_as_exclude_target(target: str) -> None:
    result = CliRunner().invoke(
        cli_module.cli,
        ["build-derived", "--exclude-target", target],
    )

    assert result.exit_code != 0
    assert target in result.output


def test_build_security_master_cli_uses_derived_builder(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_build_derived(**kwargs):
        captured.update(kwargs)
        return [{"dataset": "cn_security_master", "status": "success", "rows": 1}]

    monkeypatch.setattr(derived_commands, "run_build_derived", fake_build_derived)

    result = CliRunner().invoke(cli_module.cli, ["build-security-master", "--no-build-duckdb-views"])

    assert result.exit_code == 0
    assert captured["targets"] == ("security_master",)
    assert captured["mode"] == "full"
    assert captured["build_views"] is False
