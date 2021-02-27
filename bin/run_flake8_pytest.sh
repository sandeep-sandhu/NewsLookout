#!/bin/sh

# Run the scraper application checker flake8 and python unit tests via pytest:

SCRIPT_PATH=`dirname $(realpath $0)`
cd $SCRIPT_PATH
cd ..

PYTHON_BIN=/usr/local/bin/python3


flake8 bin/. --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics > temp/flake8_report_for_bin.txt

flake8 plugins/. --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics > temp/flake8_report_for_plugins.txt

pytest > temp/pytest_report.txt
