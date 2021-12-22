@echo off

rem Run the scraper application
rem Pass the first argument

cd /d "%~dp0"

if [%1]==[] goto makedate
rem Else, use date supplied as command-line argument
python newslookout\scraper_app.py -c conf\newslookout_win.conf -d %1

goto end

:makedate

rem set rundate to today's date as YYYY-MM-DD
for /f "skip=1" %%x in ('wmic os get localdatetime') do if not defined MyDate set MyDate=%%x

set today=%MyDate:~0,4%-%MyDate:~4,2%-%MyDate:~6,2%

python newslookout\scraper_app.py -c conf\newslookout_win.conf -d %today%

:end
echo Program completed successfully.
