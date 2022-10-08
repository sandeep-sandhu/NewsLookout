@echo off

rem Run the scraper application
rem Pass the first argument

set APP_ROOT_DIR=%USERPROFILE%\Documents\src\python_projs\NewsLookout
set PYTHON_SCRIPT=%APP_ROOT_DIR%\newslookout\scraper_app.py
set APP_CONF_FILE=%APP_ROOT_DIR%\conf\newslookout_win.conf

set NLTK_DATA=C:\shared\datasets\web_scraped_data\models\nltk

cd /d "%~dp0"

if [%1]==[] goto makedate
rem Else, use date supplied as command-line argument
python %PYTHON_SCRIPT% -c %APP_CONF_FILE% -d %1

goto end

:makedate

rem set rundate to today's date as YYYY-MM-DD
for /f "skip=1" %%x in ('wmic os get localdatetime') do if not defined MyDate set MyDate=%%x

set today=%MyDate:~0,4%-%MyDate:~4,2%-%MyDate:~6,2%

python %PYTHON_SCRIPT% -c %APP_CONF_FILE% -d %today%

:end
echo Program completed successfully.
