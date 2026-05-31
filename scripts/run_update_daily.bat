@echo off
setlocal EnableExtensions EnableDelayedExpansion
cd /d "%~dp0.."

if not exist "logs" mkdir "logs"
for /f "usebackq delims=" %%L in (`powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"`) do set "QDC_RUN_STAMP=%%L"
set "QDC_RUN_LOG=logs\run_update_daily_!QDC_RUN_STAMP!.log"

echo [%date% %time%] Logging to !QDC_RUN_LOG!
echo [%date% %time%] Logging to !QDC_RUN_LOG! > "!QDC_RUN_LOG!"

if exist "venv\Scripts\activate.bat" (
    call "venv\Scripts\activate.bat"
)

for /f "usebackq delims=" %%D in (`powershell -NoProfile -Command "(Get-Date).DayOfWeek.value__"`) do set "QDC_WEEKDAY=%%D"
set "QDC_WEEKEND_WINDOW=0"
if "%QDC_WEEKDAY%"=="0" set "QDC_WEEKEND_WINDOW=1"
if "%QDC_WEEKDAY%"=="5" set "QDC_WEEKEND_WINDOW=1"
if "%QDC_WEEKDAY%"=="6" set "QDC_WEEKEND_WINDOW=1"
for /f "usebackq delims=" %%S in (`powershell -NoProfile -Command "(Get-Date).AddDays(-30).ToString('yyyy-MM-dd')"`) do set "QDC_HIST_START=%%S"

call :run_step "update-baostock-daily calendar" "python -m src.cli update-baostock-daily --dataset baostock_cn_trading_calendar --no-build-duckdb-views"
if errorlevel 1 goto :failed

if "%QDC_WEEKEND_WINDOW%"=="1" (
    call :run_step "akshare update delist" "python -m src.cli akshare update --target delist --no-build-duckdb-views"
    if errorlevel 1 goto :failed
)

call :run_step "akshare update spot_quote" "python -m src.cli akshare update --target spot_quote --no-build-duckdb-views"
if errorlevel 1 goto :failed

call :run_step "update-baostock-daily unadjusted" "python -m src.cli update-baostock-daily --dataset baostock_cn_stock_daily_bar_unadjusted --no-build-duckdb-views"
if errorlevel 1 goto :failed

call :run_step "update-baostock-daily stock basic" "python -m src.cli update-baostock-daily --dataset baostock_cn_stock_basic --no-build-duckdb-views"
if errorlevel 1 goto :failed

call :run_step "update-baostock-valuation-percentile" "python -m src.cli update-baostock-valuation-percentile --no-build-duckdb-views"
if errorlevel 1 goto :failed

if "%QDC_WEEKEND_WINDOW%"=="1" (
    call :run_step "update-baostock-daily adjustment factor" "python -m src.cli update-baostock-daily --dataset baostock_cn_stock_adjustment_factor --no-build-duckdb-views"
    if errorlevel 1 goto :failed

    call :run_step "update-baostock-daily qfq" "python -m src.cli update-baostock-daily --dataset baostock_cn_stock_daily_bar_qfq --no-build-duckdb-views"
    if errorlevel 1 goto :failed

    call :run_step "update-baostock-daily hfq" "python -m src.cli update-baostock-daily --dataset baostock_cn_stock_daily_bar_hfq --no-build-duckdb-views"
    if errorlevel 1 goto :failed

    call :run_step "akshare update valuation full" "python -m src.cli akshare update --target valuation --mode full --no-build-duckdb-views"
    if errorlevel 1 goto :failed

    call :run_step "akshare update daily_bar incremental all from !QDC_HIST_START!" "python -m src.cli akshare update --target daily_bar --mode incremental --adjustment all --start %QDC_HIST_START% --no-build-duckdb-views"
    if errorlevel 1 goto :failed

    call :run_step "sync-qlib" "python -m src.cli sync-qlib --no-build-duckdb-views --max-runtime-seconds 7200"
    if errorlevel 1 goto :failed
)

call :run_step "build-duckdb-views" "python -m src.cli build-duckdb-views"
if errorlevel 1 goto :failed

echo [%date% %time%] All updates completed successfully
echo [%date% %time%] All updates completed successfully >> "!QDC_RUN_LOG!"
endlocal
exit /b 0

:failed
set "QDC_EXIT_CODE=!errorlevel!"
endlocal & exit /b %QDC_EXIT_CODE%

:run_step
set "QDC_STEP_NAME=%~1"
set "QDC_STEP_COMMAND=%~2"
echo [%date% %time%] Starting !QDC_STEP_NAME!...
echo [%date% %time%] Starting !QDC_STEP_NAME!... >> "!QDC_RUN_LOG!"
echo [%date% %time%] Command: !QDC_STEP_COMMAND! >> "!QDC_RUN_LOG!"
!QDC_STEP_COMMAND! >> "!QDC_RUN_LOG!" 2>&1
set "QDC_STEP_EXIT=!errorlevel!"
if not "!QDC_STEP_EXIT!"=="0" (
    echo [%date% %time%] !QDC_STEP_NAME! failed with error code !QDC_STEP_EXIT!
    echo [%date% %time%] !QDC_STEP_NAME! failed with error code !QDC_STEP_EXIT! >> "!QDC_RUN_LOG!"
    exit /b !QDC_STEP_EXIT!
)
echo [%date% %time%] Completed !QDC_STEP_NAME!
echo [%date% %time%] Completed !QDC_STEP_NAME! >> "!QDC_RUN_LOG!"
exit /b 0
