@echo off
setlocal EnableExtensions EnableDelayedExpansion

cd /d "%~dp0.."

REM Per-run log lives outside the Git workspace by default so that git clean,
REM IDE cleanup, or project resets cannot lose it. The Python orchestrator is
REM the single owner: it creates the file via RunLogContext when --run-log is
REM forwarded. The BAT only computes a deterministic, user-predictable path.
if not defined LOCALAPPDATA (
    set "LOCALAPPDATA=%USERPROFILE%\AppData\Local"
)
set "QDC_LOG_ROOT=%LOCALAPPDATA%\QuantDataCenter\logs"
set "QDC_RUN_LOG_DIR=%QDC_LOG_ROOT%\runs"
if not exist "!QDC_RUN_LOG_DIR!" mkdir "!QDC_RUN_LOG_DIR!"
for /f "usebackq delims=" %%L in (`powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"`) do set "QDC_RUN_STAMP=%%L"
set "QDC_RUN_LOG=!QDC_RUN_LOG_DIR!\run_update_daily_!QDC_RUN_STAMP!.log"

echo [%date% %time%] Logging to !QDC_RUN_LOG!

if exist "venv\Scripts\activate.bat" (
    call "venv\Scripts\activate.bat"
)

python -m src.cli run-update-daily --run-log "!QDC_RUN_LOG!" %*
set "QDC_EXIT_CODE=!errorlevel!"

endlocal & exit /b %QDC_EXIT_CODE%
