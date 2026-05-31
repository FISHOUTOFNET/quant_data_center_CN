@echo off
setlocal EnableExtensions EnableDelayedExpansion

for /f \ usebackq delims=\ %%D in (powershell -NoProfile -Command \ 1 \) do set \QDC_WEEKDAY=%%D\
set \QDC_WEEKEND_WINDOW=0\
if \%QDC_WEEKDAY%\==\0\ set \QDC_WEEKEND_WINDOW=1\
if \%QDC_WEEKDAY%\==\5\ set \QDC_WEEKEND_WINDOW=1\
if \%QDC_WEEKDAY%\==\6\ set \QDC_WEEKEND_WINDOW=1\

echo QDC_WEEKDAY=%QDC_WEEKDAY%
echo QDC_WEEKEND_WINDOW=%QDC_WEEKEND_WINDOW%

if \%QDC_WEEKEND_WINDOW%\==\1\ (
    echo Weekend window is active
) else (
    echo Weekend window is NOT active - should run weekday tasks
)
