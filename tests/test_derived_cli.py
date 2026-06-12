from __future__ import annotations

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
    assert captured["mode"] == "incremental"
    assert captured["security_ids"] == ("SH.600000",)
    assert captured["build_views"] is False


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
