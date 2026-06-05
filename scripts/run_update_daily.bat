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

python -m src.cli run-update-daily --run-log "!QDC_RUN_LOG!" %*
set "QDC_EXIT_CODE=!errorlevel!"

endlocal & exit /b %QDC_EXIT_CODE%
