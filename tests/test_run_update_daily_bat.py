from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "run_update_daily.bat"


def _script_text() -> str:
    return SCRIPT.read_text(encoding="utf-8")


def test_run_update_daily_bat_defines_weekend_window_for_friday_through_sunday() -> None:
    text = _script_text()

    assert (
        'for /f "usebackq delims=" %%D in (`powershell -NoProfile -Command "(Get-Date).DayOfWeek.value__"`) do set "QDC_WEEKDAY=%%D"'
        in text
    )
    assert 'set "QDC_WEEKEND_WINDOW=0"' in text
    assert 'if "%QDC_WEEKDAY%"=="0" set "QDC_WEEKEND_WINDOW=1"' in text
    assert 'if "%QDC_WEEKDAY%"=="5" set "QDC_WEEKEND_WINDOW=1"' in text
    assert 'if "%QDC_WEEKDAY%"=="6" set "QDC_WEEKEND_WINDOW=1"' in text


def test_run_update_daily_bat_runs_qlib_inside_shared_weekend_window() -> None:
    text = _script_text()

    assert "QDC_QLIB_WEEKEND_WINDOW" not in text

    qlib = 'call :run_step "sync-qlib" "python -m src.cli sync-qlib --no-build-duckdb-views --max-runtime-seconds 7200"'
    assert qlib in text
    assert text.index('if "%QDC_WEEKEND_WINDOW%"=="1" (') < text.index(qlib)


def test_run_update_daily_bat_orders_daily_and_weekend_updates() -> None:
    text = _script_text()

    calendar = "python -m src.cli update-baostock-daily --dataset baostock_cn_trading_calendar --no-build-duckdb-views"
    delist = "python -m src.cli akshare update --target delist --no-build-duckdb-views"
    spot = "python -m src.cli akshare update --target spot_quote --no-build-duckdb-views"
    baostock_unadjusted = "python -m src.cli update-baostock-daily --dataset baostock_cn_stock_daily_bar_unadjusted --no-build-duckdb-views"
    baostock_basic = "python -m src.cli update-baostock-daily --dataset baostock_cn_stock_basic --no-build-duckdb-views"
    baostock_valuation_percentile = "python -m src.cli update-baostock-valuation-percentile --no-build-duckdb-views"
    baostock_adjustment_factor = (
        "python -m src.cli update-baostock-daily --dataset baostock_cn_stock_adjustment_factor --no-build-duckdb-views"
    )
    baostock_qfq = (
        "python -m src.cli update-baostock-daily --dataset baostock_cn_stock_daily_bar_qfq --no-build-duckdb-views"
    )
    baostock_hfq = (
        "python -m src.cli update-baostock-daily --dataset baostock_cn_stock_daily_bar_hfq --no-build-duckdb-views"
    )
    valuation = "python -m src.cli akshare update --target valuation --mode full --no-build-duckdb-views"
    report_disclosure = "python -m src.cli akshare update --target report_disclosure --no-build-duckdb-views"
    hist = "python -m src.cli akshare update --target daily_bar --mode incremental --adjustment all --start %QDC_HIST_START% --no-build-duckdb-views"
    qlib = "python -m src.cli sync-qlib --no-build-duckdb-views --max-runtime-seconds 7200"
    build_views = "python -m src.cli build-duckdb-views"

    assert text.index(calendar) < text.index(spot)
    assert text.index(spot) < text.index(baostock_unadjusted)
    assert text.index(baostock_unadjusted) < text.index(baostock_basic)
    assert text.index(baostock_basic) < text.index(baostock_valuation_percentile)
    assert text.index(baostock_valuation_percentile) < text.index(build_views)
    assert text.index(baostock_unadjusted) < text.index(build_views)
    assert "python -m src.cli update-baostock-daily --dataset all --no-build-duckdb-views" not in text

    assert text.index(calendar) < text.index(delist)
    assert text.index(delist) < text.index(spot)
    assert text.index(spot) < text.index(baostock_unadjusted)
    assert text.index(baostock_unadjusted) < text.index(baostock_basic)
    assert text.index(baostock_basic) < text.index(baostock_valuation_percentile)
    assert text.index(baostock_valuation_percentile) < text.index(baostock_adjustment_factor)
    assert text.index(baostock_adjustment_factor) < text.index(baostock_qfq)
    assert text.index(baostock_qfq) < text.index(baostock_hfq)
    assert text.index(baostock_hfq) < text.index(valuation)
    assert text.index(baostock_basic) < text.index(valuation)
    assert text.index(valuation) < text.index(report_disclosure)
    assert text.index(report_disclosure) < text.index(hist)
    assert text.index(hist) < text.index(qlib)
    assert text.index(qlib) < text.index(build_views)
    assert text.index(hist) < text.index(build_views)

    assert text.count(hist) == 1
    assert text.count(report_disclosure) == 1
    assert text.count(qlib) == 1
    assert text.count(baostock_adjustment_factor) == 1
    assert "--target capital_structure" not in text
    assert text.count(baostock_qfq) == 1
    assert text.count(baostock_hfq) == 1
    assert 'if "%QDC_WEEKEND_WINDOW%"=="1" (' in text
    assert text.index('if "%QDC_WEEKEND_WINDOW%"=="1" (') < text.index(hist)
    assert text.index('if "%QDC_WEEKEND_WINDOW%"=="1" (') < text.index(report_disclosure)
    assert text.index('if "%QDC_WEEKEND_WINDOW%"=="1" (') < text.index(baostock_adjustment_factor)


def test_run_update_daily_bat_logs_each_run_and_preserves_exit_codes() -> None:
    text = _script_text()

    assert 'if not exist "logs" mkdir "logs"' in text
    assert "Get-Date -Format yyyyMMdd_HHmmss" in text
    assert 'set "QDC_RUN_LOG=logs\\run_update_daily_!QDC_RUN_STAMP!.log"' in text
    assert '>> "!QDC_RUN_LOG!" 2>&1' in text
    assert "exit /b !QDC_STEP_EXIT!" in text
    assert "endlocal & exit /b %QDC_EXIT_CODE%" in text
    assert 'call :run_step "build-duckdb-views" "python -m src.cli build-duckdb-views"' in text
    assert text.index('call :run_step "build-duckdb-views"') < text.index("All updates completed")
