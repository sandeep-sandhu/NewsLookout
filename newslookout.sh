#!/bin/bash

SCRIPT_PATH=`dirname $(realpath $0)`
cd $SCRIPT_PATH

PYTHON_BIN=`which python3`

date_arr=()

# Check for environment variables: NEWSLOOKOUT_RUNDATE_FROM  and  NEWSLOOKOUT_RUNDATE_TO
if [[ -z "${NEWSLOOKOUT_RUNDATE_FROM}" ]];
then
	
	# processing for single date:
	if [ -z "$1" ]
	then
		RUN_DATE=`date +%Y-%m-%d`
		echo "Using todays date as Run date = $RUN_DATE"
	else
		echo "Using given Run date = $1"
		RUN_DATE=$1
	fi
	date_arr+=("$RUN_DATE")
	
else

	if [[ ! -z "${NEWSLOOKOUT_RUNDATE_TO}" ]];
	then
		echo "Start date = $NEWSLOOKOUT_RUNDATE_FROM"
		echo "End date = $NEWSLOOKOUT_RUNDATE_TO"
		
		start_date=$NEWSLOOKOUT_RUNDATE_FROM
		end_date=$NEWSLOOKOUT_RUNDATE_TO
		# loop through all dates in between:
		until [[ $start_date > $end_date ]]; do 
			#echo "Calculating and adding date: $start_date"
			date_arr+=($start_date)
			start_date=$(date -I -d "$start_date + 1 day")
		done
	else
		# processing for single date:
		if [ -z "$1" ]
		then
		  RUN_DATE=`date +%Y-%m-%d`
		else
		  RUN_DATE=$1
		fi
		date_arr+=("$RUN_DATE")
	fi
	
fi


# run the application for each of the given dates:
for dateval in "${date_arr[@]}" ; do
	$PYTHON_BIN /opt/newslookout/bin/scraper_app.py -c /etc/newslookout/newslookout.conf -d $dateval
done


if [ "$NEWSLOOKOUT_PERSIST_AFTER_RUN" == "True" ]
then
  # wait for 1 million seconds
  echo Flag $NEWSLOOKOUT_PERSIST_AFTER_RUN="$NEWSLOOKOUT_PERSIST_AFTER_RUN", hence waiting for 1 million seconds...
  sleep 1000000
fi

# end of file #