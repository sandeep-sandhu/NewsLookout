@echo off

rem Run the scraper application checker flake8 and python unit tests via pytest:

cd /d "%~dp0"
cd ..


flake8 bin\. --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics > temp\flake8_report_for_bin.txt

flake8 plugins\. --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics > temp\flake8_report_for_plugins.txt

pytest > temp\pytest_report.txt

