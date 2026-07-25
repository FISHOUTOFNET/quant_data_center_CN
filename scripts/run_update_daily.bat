@echo off
setlocal EnableExtensions EnableDelayedExpansion

cd /d "%~dp0.."

REM The Python orchestrator is the single owner of the run log path. It
REM resolves the log directory via the unified priority chain
REM (CLI --run-log > QDC_LOG_DIR > settings.yaml > OS default) and creates
REM the RunLogContext. The BAT no longer computes a log path itself, so
REM QDC_LOG_DIR and settings.yaml are respected for both the application
REM log and the per-run log.
REM
REM A user may still pass --run-log explicitly; it is forwarded as-is.

if exist "venv\Scripts\activate.bat" (
    call "venv\Scripts\activate.bat"
)

python -m src.cli run-update-daily %*
set "QDC_EXIT_CODE=!errorlevel!"

endlocal & exit /b %QDC_EXIT_CODE%
