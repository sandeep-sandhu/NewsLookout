#!/bin/bash

SCRIPT_PATH=`dirname $(realpath $0)`
cd $SCRIPT_PATH


PYTHON_BIN=`which python3`

if [ -z "$1" ]
then
  RUN_DATE=`date +%Y-%m-%d`
else
  RUN_DATE=$1
fi


$PYTHON_BIN /opt/newslookout/scraper_app.py -c /etc/newslookout/newslookout.conf -d $RUN_DATE $2


if [ "$NEWSLOOKOUT_PERSIST_AFTER_RUN" == "True" ]
then
  # wait for 1 million seconds
  echo Flag $NEWSLOOKOUT_PERSIST_AFTER_RUN="$NEWSLOOKOUT_PERSIST_AFTER_RUN", hence waiting for 1 million seconds...
  sleep 1000000
fi

# end of file #
