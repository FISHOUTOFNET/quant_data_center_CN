from __future__ import annotations

from click.testing import CliRunner

import src.cli as cli_module


def test_build_derived_cli_passes_targets(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_build_derived(**kwargs):
        captured.update(kwargs)
        return [{"dataset": "cn_security_master", "status": "success", "rows": 1, "active": 1, "delisted": 0}]

    monkeypatch.setattr(cli_module, "run_build_derived", fake_build_derived)

    result = CliRunner().invoke(
        cli_module.cli,
        ["build-derived", "--target", "security_master", "--no-build-duckdb-views"],
    )

    assert result.exit_code == 0
    assert captured["targets"] == ("security_master",)
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

    monkeypatch.setattr(cli_module, "run_build_derived", fake_build_derived)

    result = CliRunner().invoke(cli_module.cli, ["build-derived", "--target", "all", "--no-build-duckdb-views"])

    assert result.exit_code == 0
    assert captured["targets"] == ("all",)
    assert "cn_stock_daily_bar status=success rows=2 partitions=1" in result.output


def test_build_security_master_cli_uses_derived_builder(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_build_derived(**kwargs):
        captured.update(kwargs)
        return [{"dataset": "cn_security_master", "status": "success", "rows": 1}]

    monkeypatch.setattr(cli_module, "run_build_derived", fake_build_derived)

    result = CliRunner().invoke(cli_module.cli, ["build-security-master", "--no-build-duckdb-views"])

    assert result.exit_code == 0
    assert captured["targets"] == ("security_master",)
    assert captured["build_views"] is False
