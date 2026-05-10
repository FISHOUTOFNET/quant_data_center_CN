from __future__ import annotations

from click.testing import CliRunner

import src.cli as cli_module


def test_update_daily_full_cli_passes_explicit_provider(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_update_daily(**kwargs):
        captured.update(kwargs)
        return [{"dataset": "baostock_cn_trading_calendar", "code": "*", "status": "success", "row_count": 1}]

    monkeypatch.setattr(cli_module, "run_update_daily", fake_update_daily)

    result = CliRunner().invoke(
        cli_module.cli,
        ["update-baostock-daily", "--mode", "full", "--dataset", "baostock_cn_trading_calendar", "--provider", "baostock", "--no-build-duckdb-views"],
    )

    assert result.exit_code == 0
    assert captured["provider"] == "baostock"
    assert captured["mode"] == "full"
    assert captured["dataset"] == "baostock_cn_trading_calendar"


def test_update_daily_cli_keeps_provider_optional(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_update_daily(**kwargs):
        captured.update(kwargs)
        return [{"dataset": "baostock_cn_stock_daily_bar_qfq", "code": "sh.600000", "status": "success", "row_count": 2}]

    monkeypatch.setattr(cli_module, "run_update_daily", fake_update_daily)

    result = CliRunner().invoke(
        cli_module.cli,
        ["update-baostock-daily", "--code", "sh.600000", "--no-build-duckdb-views"],
    )

    assert result.exit_code == 0
    assert captured["provider"] is None
    assert captured["mode"] == "partial"
    assert captured["dataset"] == "all"


def test_update_daily_cli_does_not_expose_universe_option() -> None:
    result = CliRunner().invoke(cli_module.cli, ["update-baostock-daily", "--help"])

    assert result.exit_code == 0
    assert "--universe" not in result.output


def test_update_akshare_cli_passes_arguments(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_update_akshare(**kwargs):
        captured.update(kwargs)
        return [{"dataset": "akshare_cn_stock_valuation_eastmoney", "code": "600000", "status": "success", "row_count": 2}]

    monkeypatch.setattr(cli_module, "run_update_akshare", fake_update_akshare)

    result = CliRunner().invoke(
        cli_module.cli,
        [
            "update-akshare-valuation",
            "--dataset",
            "akshare_cn_stock_valuation_eastmoney",
            "--mode",
            "full",
            "--code",
            "600000",
            "--code",
            "000001",
            "--include-inactive",
            "--max-tasks",
            "10",
            "--workers",
            "3",
            "--no-resume",
            "--force",
            "--no-build-duckdb-views",
        ],
    )

    assert result.exit_code == 0
    assert captured["dataset"] == "akshare_cn_stock_valuation_eastmoney"
    assert captured["mode"] == "full"
    assert captured["code"] == ("600000", "000001")
    assert captured["include_inactive"] is True
    assert captured["max_tasks"] == 10
    assert captured["workers"] == 3
    assert captured["resume"] is False
    assert captured["force"] is True
    assert captured["build_views"] is False


def test_update_akshare_a_stock_cli_commands_pass_arguments(monkeypatch) -> None:
    captured: dict[str, dict[str, object]] = {}

    def fake_delist(**kwargs):
        captured["delist"] = kwargs
        return [{"dataset": "akshare_cn_stock_delist_sh", "code": "全部", "status": "success", "row_count": 1}]

    def fake_spot(**kwargs):
        captured["spot"] = kwargs
        return [{"dataset": "akshare_cn_stock_spot_quote_eastmoney", "code": "*", "status": "success", "row_count": 1}]

    def fake_hist(**kwargs):
        captured["hist"] = kwargs
        return [{"dataset": "akshare_cn_stock_daily_bar_unadjusted", "code": "600000", "status": "success", "row_count": 1}]

    monkeypatch.setattr(cli_module, "run_update_akshare_delist", fake_delist)
    monkeypatch.setattr(cli_module, "run_update_akshare_spot", fake_spot)
    monkeypatch.setattr(cli_module, "run_update_akshare_daily_bar", fake_hist)

    runner = CliRunner()
    delist_result = runner.invoke(
        cli_module.cli,
        ["update-akshare-delist", "--market", "沪市", "--snapshot-date", "2024-01-03", "--no-resume", "--force", "--no-build-duckdb-views"],
    )
    spot_result = runner.invoke(
        cli_module.cli,
        ["update-akshare-spot-quote", "--end", "2024-01-03", "--no-resume", "--force", "--no-build-duckdb-views"],
    )
    hist_result = runner.invoke(
        cli_module.cli,
        [
            "update-akshare-daily-bar",
            "--mode",
            "incremental",
            "--adjustment",
            "unadjusted",
            "--code",
            "600000",
            "--start",
            "2024-01-03",
            "--end",
            "2024-01-03",
            "--max-tasks",
            "1",
            "--workers",
            "1",
            "--no-resume",
            "--force",
            "--no-build-duckdb-views",
        ],
    )

    assert delist_result.exit_code == 0
    assert spot_result.exit_code == 0
    assert hist_result.exit_code == 0
    assert captured["delist"] == {
        "market": "沪市",
        "snapshot_date": "2024-01-03",
        "exchanges": None,
        "resume": False,
        "force": True,
        "build_views": False,
    }
    assert captured["spot"] == {
        "end": "2024-01-03",
        "resume": False,
        "force": True,
        "build_views": False,
    }
    assert captured["hist"]["mode"] == "incremental"
    assert captured["hist"]["adjustment"] == "unadjusted"
    assert captured["hist"]["code"] == ("600000",)
    assert captured["hist"]["start"] == "2024-01-03"
    assert captured["hist"]["end"] == "2024-01-03"
    assert captured["hist"]["max_tasks"] == 1
    assert captured["hist"]["workers"] == 1


def test_update_baostock_valuation_percentile_cli_passes_arguments(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_update_baostock_valuation_percentile(**kwargs):
        captured.update(kwargs)
        return [{"dataset": "baostock_cn_stock_valuation_percentile", "code": "sh.600000", "status": "success", "row_count": 2}]

    monkeypatch.setattr(cli_module, "run_update_baostock_valuation_percentile", fake_update_baostock_valuation_percentile)

    result = CliRunner().invoke(
        cli_module.cli,
        [
            "update-baostock-valuation-percentile",
            "--mode",
            "full",
            "--code",
            "sh.600000",
            "--start",
            "2021-01-01",
            "--no-resume",
            "--force",
            "--no-build-duckdb-views",
        ],
    )

    assert result.exit_code == 0
    assert captured == {
        "mode": "full",
        "code": ("sh.600000",),
        "start": "2021-01-01",
        "resume": False,
        "force": True,
        "build_views": False,
    }


def test_akshare_cli_rejects_non_six_digit_code_shapes() -> None:
    runner = CliRunner()

    for command in ["update-akshare-valuation", "update-akshare-daily-bar"]:
        args = [command, "--code", "sh.600000"]
        if command == "update-akshare-daily-bar":
            args.extend(["--mode", "incremental", "--start", "2024-01-03"])
        result = runner.invoke(cli_module.cli, args)

        assert result.exit_code != 0
        assert "must be 6 digits" in result.output


def test_legacy_cli_commands_are_not_registered() -> None:
    runner = CliRunner()

    for command in [
        "update-daily",
        "update-akshare",
        "update-akshare-spot",
        "update-akshare-hist",
        "repair",
        "build-views",
    ]:
        result = runner.invoke(cli_module.cli, [command, "--help"])

        assert result.exit_code != 0
        assert "No such command" in result.output


