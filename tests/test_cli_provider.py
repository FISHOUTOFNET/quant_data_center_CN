from __future__ import annotations

from click.testing import CliRunner

import src.cli as cli_module


def test_update_daily_full_cli_passes_explicit_provider(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_update_daily(**kwargs):
        captured.update(kwargs)
        return [{"dataset": "calendar", "code": "*", "status": "success", "row_count": 1}]

    monkeypatch.setattr(cli_module, "run_update_daily", fake_update_daily)

    result = CliRunner().invoke(
        cli_module.cli,
        ["update-daily", "--mode", "full", "--dataset", "calendar", "--provider", "baostock", "--no-build-views"],
    )

    assert result.exit_code == 0
    assert captured["provider"] == "baostock"
    assert captured["mode"] == "full"
    assert captured["dataset"] == "calendar"


def test_update_daily_cli_keeps_provider_optional(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_update_daily(**kwargs):
        captured.update(kwargs)
        return [{"dataset": "daily_k_qfq", "code": "sh.600000", "status": "success", "row_count": 2}]

    monkeypatch.setattr(cli_module, "run_update_daily", fake_update_daily)

    result = CliRunner().invoke(
        cli_module.cli,
        ["update-daily", "--code", "sh.600000", "--no-build-views"],
    )

    assert result.exit_code == 0
    assert captured["provider"] is None
    assert captured["mode"] == "partial"
    assert captured["dataset"] == "all"


def test_update_akshare_cli_passes_arguments(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_update_akshare(**kwargs):
        captured.update(kwargs)
        return [{"dataset": "stock_value_em", "code": "600000", "status": "success", "row_count": 2}]

    monkeypatch.setattr(cli_module, "run_update_akshare", fake_update_akshare)

    result = CliRunner().invoke(
        cli_module.cli,
        [
            "update-akshare",
            "--dataset",
            "stock_value_em",
            "--mode",
            "full",
            "--code",
            "sh.600000",
            "--code",
            "sz.000001",
            "--include-inactive",
            "--max-tasks",
            "10",
            "--workers",
            "3",
            "--no-resume",
            "--force",
            "--no-build-views",
        ],
    )

    assert result.exit_code == 0
    assert captured["dataset"] == "stock_value_em"
    assert captured["mode"] == "full"
    assert captured["code"] == ("sh.600000", "sz.000001")
    assert captured["include_inactive"] is True
    assert captured["max_tasks"] == 10
    assert captured["workers"] == 3
    assert captured["resume"] is False
    assert captured["force"] is True
    assert captured["build_views"] is False


def test_update_akshare_a_stock_cli_commands_pass_arguments(monkeypatch) -> None:
    captured: dict[str, dict[str, object]] = {}

    def fake_universe(**kwargs):
        captured["universe"] = kwargs
        return [{"dataset": "stock_info_sh_delist", "code": "全部", "status": "success", "row_count": 1}]

    def fake_spot(**kwargs):
        captured["spot"] = kwargs
        return [{"dataset": "stock_zh_a_spot_em", "code": "*", "status": "success", "row_count": 1}]

    def fake_hist(**kwargs):
        captured["hist"] = kwargs
        return [{"dataset": "stock_zh_a_hist_none", "code": "600000", "status": "success", "row_count": 1}]

    monkeypatch.setattr(cli_module, "run_update_akshare_universe", fake_universe)
    monkeypatch.setattr(cli_module, "run_update_akshare_spot", fake_spot)
    monkeypatch.setattr(cli_module, "run_update_akshare_hist", fake_hist)

    runner = CliRunner()
    universe_result = runner.invoke(
        cli_module.cli,
        ["update-akshare-universe", "--market", "沪市", "--snapshot-date", "2024-01-03", "--no-resume", "--force", "--no-build-views"],
    )
    spot_result = runner.invoke(
        cli_module.cli,
        ["update-akshare-spot", "--end", "2024-01-03", "--no-resume", "--force", "--no-build-views"],
    )
    hist_result = runner.invoke(
        cli_module.cli,
        [
            "update-akshare-hist",
            "--mode",
            "incremental",
            "--adjust",
            "none",
            "--code",
            "sh.600000",
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
            "--no-build-views",
        ],
    )

    assert universe_result.exit_code == 0
    assert spot_result.exit_code == 0
    assert hist_result.exit_code == 0
    assert captured["universe"] == {
        "market": "沪市",
        "snapshot_date": "2024-01-03",
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
    assert captured["hist"]["adjust"] == "none"
    assert captured["hist"]["code"] == ("sh.600000",)
    assert captured["hist"]["start"] == "2024-01-03"
    assert captured["hist"]["end"] == "2024-01-03"
    assert captured["hist"]["max_tasks"] == 1
    assert captured["hist"]["workers"] == 1
