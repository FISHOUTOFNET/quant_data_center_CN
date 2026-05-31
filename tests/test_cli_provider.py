from __future__ import annotations

from click.testing import CliRunner

import src.cli as cli_module
from src.utils.logging import logger


def test_update_daily_full_cli_passes_explicit_provider(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_update_daily(**kwargs):
        captured.update(kwargs)
        return [{"dataset": "baostock_cn_trading_calendar", "code": "*", "status": "success", "row_count": 1}]

    monkeypatch.setattr(cli_module, "run_update_daily", fake_update_daily)

    result = CliRunner().invoke(
        cli_module.cli,
        [
            "update-baostock-daily",
            "--mode",
            "full",
            "--dataset",
            "baostock_cn_trading_calendar",
            "--provider",
            "baostock",
            "--no-build-duckdb-views",
        ],
    )

    assert result.exit_code == 0
    assert captured["provider"] == "baostock"
    assert captured["mode"] == "full"
    assert captured["dataset"] == "baostock_cn_trading_calendar"


def test_update_daily_cli_keeps_provider_optional(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_update_daily(**kwargs):
        captured.update(kwargs)
        return [
            {
                "dataset": "baostock_cn_stock_daily_bar_unadjusted",
                "code": "sh.600000",
                "status": "success",
                "row_count": 2,
            }
        ]

    monkeypatch.setattr(cli_module, "run_update_daily", fake_update_daily)

    result = CliRunner().invoke(
        cli_module.cli,
        ["update-baostock-daily", "--code", "sh.600000", "--no-build-duckdb-views"],
    )

    assert result.exit_code == 0
    assert captured["provider"] is None
    assert captured["mode"] == "partial"
    assert captured["dataset"] == "baostock_cn_stock_daily_bar_unadjusted"


def test_update_daily_cli_does_not_expose_universe_option() -> None:
    result = CliRunner().invoke(cli_module.cli, ["update-baostock-daily", "--help"])

    assert result.exit_code == 0
    assert "--universe" not in result.output


def test_update_akshare_cli_passes_arguments(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_update_akshare(request):
        captured.update(request.__dict__)
        return [
            {"dataset": "akshare_cn_stock_valuation_eastmoney", "code": "600000", "status": "success", "row_count": 2}
        ]

    monkeypatch.setattr(cli_module, "run_update_akshare", fake_update_akshare)

    result = CliRunner().invoke(
        cli_module.cli,
        [
            "akshare",
            "update",
            "--target",
            "valuation",
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
    assert captured["target"] == "valuation"
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

    def fake_update_akshare(request):
        captured[str(request.target)] = request.__dict__
        dataset = {
            "delist": "akshare_cn_stock_delist_sh",
            "spot_quote": "akshare_cn_stock_spot_quote_eastmoney",
            "daily_bar": "akshare_cn_stock_daily_bar_unadjusted",
        }[str(request.target)]
        return [{"dataset": dataset, "code": "600000", "status": "success", "row_count": 1}]

    monkeypatch.setattr(cli_module, "run_update_akshare", fake_update_akshare)

    runner = CliRunner()
    delist_result = runner.invoke(
        cli_module.cli,
        [
            "akshare",
            "update",
            "--target",
            "delist",
            "--market",
            "沪市",
            "--end",
            "2024-01-03",
            "--no-resume",
            "--force",
            "--no-build-duckdb-views",
        ],
    )
    spot_result = runner.invoke(
        cli_module.cli,
        [
            "akshare",
            "update",
            "--target",
            "spot_quote",
            "--end",
            "2024-01-03",
            "--no-resume",
            "--force",
            "--no-build-duckdb-views",
        ],
    )
    hist_result = runner.invoke(
        cli_module.cli,
        [
            "akshare",
            "update",
            "--target",
            "daily_bar",
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
    assert captured["delist"] | {"root": None, "client": None, "client_factory": None, "now": None} == {
        "target": "delist",
        "mode": "partial",
        "adjustment": None,
        "code": (),
        "include_inactive": False,
        "market": "沪市",
        "start": None,
        "end": "2024-01-03",
        "max_tasks": None,
        "workers": None,
        "root": None,
        "resume": False,
        "force": True,
        "build_views": False,
        "client": None,
        "client_factory": None,
        "now": None,
    }
    assert captured["spot_quote"] | {"root": None, "client": None, "client_factory": None, "now": None} == {
        "target": "spot_quote",
        "mode": "partial",
        "adjustment": None,
        "code": (),
        "include_inactive": False,
        "market": None,
        "start": None,
        "end": "2024-01-03",
        "max_tasks": None,
        "workers": None,
        "root": None,
        "resume": False,
        "force": True,
        "build_views": False,
        "client": None,
        "client_factory": None,
        "now": None,
    }
    assert captured["daily_bar"]["mode"] == "incremental"
    assert captured["daily_bar"]["adjustment"] == "unadjusted"
    assert captured["daily_bar"]["code"] == ("600000",)
    assert captured["daily_bar"]["start"] == "2024-01-03"
    assert captured["daily_bar"]["end"] == "2024-01-03"
    assert captured["daily_bar"]["max_tasks"] == 1
    assert captured["daily_bar"]["workers"] == 1


def test_update_baostock_valuation_percentile_cli_passes_arguments(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_update_baostock_valuation_percentile(**kwargs):
        captured.update(kwargs)
        return [
            {
                "dataset": "baostock_cn_stock_valuation_percentile",
                "code": "sh.600000",
                "status": "success",
                "row_count": 2,
            }
        ]

    monkeypatch.setattr(
        cli_module, "run_update_baostock_valuation_percentile", fake_update_baostock_valuation_percentile
    )

    result = CliRunner().invoke(
        cli_module.cli,
        [
            "update-baostock-valuation-percentile",
            "--mode",
            "partial",
            "--start",
            "2024-01-03",
            "--code",
            "sh.600000",
            "--no-resume",
            "--force",
            "--no-build-duckdb-views",
        ],
    )

    assert result.exit_code == 0
    assert captured["mode"] == "partial"
    assert captured["start"] == "2024-01-03"
    assert captured["code"] == ("sh.600000",)
    assert captured["resume"] is False
    assert captured["force"] is True
    assert captured["build_views"] is False


def test_akshare_cli_rejects_non_six_digit_code_shapes() -> None:
    runner = CliRunner()

    for target in ["valuation", "daily_bar"]:
        args = ["akshare", "update", "--target", target, "--code", "sh.600000"]
        if target == "daily_bar":
            args.extend(["--mode", "incremental", "--start", "2024-01-03"])
        result = runner.invoke(cli_module.cli, args)

        assert result.exit_code != 0
        assert "must be 6 digits" in result.output


def test_akshare_cli_rejects_adjustment_for_non_daily_bar_target() -> None:
    result = CliRunner().invoke(
        cli_module.cli,
        ["akshare", "update", "--target", "valuation", "--adjustment", "qfq"],
    )

    assert result.exit_code != 0
    assert "--adjustment is only valid for --target daily_bar" in result.output


def test_akshare_cli_accepts_capital_structure_target(monkeypatch) -> None:
    captured = {}

    def fake_update(request):
        captured["target"] = request.target
        captured["code"] = request.code
        return [
            {"dataset": "akshare_cn_stock_capital_structure_em", "code": "600000", "status": "success", "row_count": 1}
        ]

    monkeypatch.setattr("src.cli.run_update_akshare", fake_update)

    result = CliRunner().invoke(
        cli_module.cli,
        ["akshare", "update", "--target", "capital_structure", "--code", "600000", "--no-build-duckdb-views"],
    )

    assert result.exit_code == 0
    assert captured == {"target": "capital_structure", "code": ("600000",)}
    assert "akshare_cn_stock_capital_structure_em 600000 status=success rows=1" in result.output


def test_sync_qlib_cli_skips_outside_friday_sunday_window_by_default(monkeypatch) -> None:
    def fail_sync_qlib_data(**kwargs):
        raise AssertionError("sync_qlib_data should not run on weekdays by default")

    monkeypatch.setattr(cli_module.qlib_sync_module, "is_qlib_update_day", lambda: False, raising=False)
    monkeypatch.setattr(cli_module.qlib_sync_module, "sync_qlib_data", fail_sync_qlib_data)

    result = CliRunner().invoke(cli_module.cli, ["sync-qlib", "--no-build-duckdb-views"])

    assert result.exit_code == 0
    assert "qlib status=skipped_weekday" in result.output
    assert "outside_friday_sunday_window" in result.output


def test_sync_qlib_cli_allows_weekday_override_and_passes_runtime_limit_and_workers(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class Result:
        status = "checked_current"
        target_date = "2024-01-05"
        source_latest_date = "2024-01-05"
        project_latest_date = "2024-01-05"
        downloaded = False
        synced = False

    def fake_sync_qlib_data(**kwargs):
        captured.update(kwargs)
        return Result()

    monkeypatch.setattr(cli_module.qlib_sync_module, "is_qlib_update_day", lambda: False, raising=False)
    monkeypatch.setattr(cli_module.qlib_sync_module, "sync_qlib_data", fake_sync_qlib_data)

    result = CliRunner().invoke(
        cli_module.cli,
        [
            "sync-qlib",
            "--allow-weekday",
            "--max-runtime-seconds",
            "12",
            "--workers",
            "2",
            "--no-build-duckdb-views",
        ],
    )

    assert result.exit_code == 0
    assert captured["max_runtime_seconds"] == 12.0
    assert captured["workers"] == 2
    assert captured["build_views"] is False
    assert "qlib status=checked_current" in result.output


def test_configure_logging_uses_qdc_log_dir(tmp_path, monkeypatch) -> None:
    project_root = tmp_path / "project"
    env_log_dir = tmp_path / "env-logs"
    monkeypatch.setenv("QDC_LOG_DIR", str(env_log_dir))

    try:
        cli_module.configure_logging(project_root)
        logger.info("env log dir smoke")
    finally:
        logger.remove()

    assert (env_log_dir / "qdc.log").exists()
    assert not (project_root / "logs" / "qdc.log").exists()


def test_configure_logging_can_disable_file_log(tmp_path, monkeypatch) -> None:
    project_root = tmp_path / "project"
    monkeypatch.setenv("QDC_DISABLE_FILE_LOG", "1")

    try:
        cli_module.configure_logging(project_root)
        logger.info("disabled file log smoke")
    finally:
        logger.remove()

    assert not (project_root / "logs" / "qdc.log").exists()


def test_legacy_cli_commands_are_not_registered() -> None:
    runner = CliRunner()

    for command in [
        "update-daily",
        "update-akshare",
        "update-akshare-valuation",
        "update-akshare-delist",
        "update-akshare-spot-quote",
        "update-akshare-daily-bar",
        "update-akshare-spot",
        "update-akshare-hist",
        "repair",
        "build-views",
    ]:
        result = runner.invoke(cli_module.cli, [command, "--help"])

        assert result.exit_code != 0
        assert "No such command" in result.output
