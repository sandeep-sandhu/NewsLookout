@echo off

rem Run the scraper application
rem Pass the first argument

cd /d "%~dp0"

rem set rundate to today's date as YYYY-MM-DD
for /f "skip=1" %%x in ('wmic os get localdatetime') do if not defined MyDate set MyDate=%%x

set today=%MyDate:~0,4%-%MyDate:~4,2%-%MyDate:~6,2%

python scraper_app.py -c conf\newslookout_win.conf -d %today% %1

pause
