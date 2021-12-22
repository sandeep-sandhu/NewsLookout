#!/bin/sh

SCRIPT_PATH=`dirname $(realpath $0)`
cd $SCRIPT_PATH


PYTHON_BIN=`which python3`

if [ -z "$1" ]
then
      RUN_DATE=`date +%Y-%m-%d`
else
      RUN_DATE=$1
fi


$PYTHON_BIN newslookout/scraper_app.py -c conf/newslookout_unix.conf -d $RUN_DATE $2

