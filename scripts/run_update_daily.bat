@echo off
setlocal
cd /d "%~dp0.."

if exist "venv\Scripts\activate.bat" (
    call "venv\Scripts\activate.bat"
)

echo [%date% %time%] Starting update-baostock-daily...
python -m src.cli update-baostock-daily
if %errorlevel% neq 0 (
    echo [%date% %time%] update-baostock-daily failed with error code %errorlevel%
    endlocal
    exit /b %errorlevel%
)

echo [%date% %time%] Starting update-akshare-spot-quote...
python -m src.cli update-akshare-spot-quote
if %errorlevel% neq 0 (
    echo [%date% %time%] update-akshare-spot-quote failed with error code %errorlevel%
    endlocal
    exit /b %errorlevel%
)

echo [%date% %time%] All updates completed successfully
endlocal
