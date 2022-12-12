#!/bin/bash

basepath="/home/tbde/truedata-historical-data/JAN-22-Testing/"
FILELIST="`ls /home/tbde/truedata-historical-data/JAN-22-Testing/*.csv | cut -d"." -f1  | cut -d "/" -f6`"

for INPUT_FILE in $FILELIST
do

	entity_name=`echo "${INPUT_FILE%%[0-9]*}"`
	last_chars=${INPUT_FILE: (-2)}

	if [[ $last_chars == "UT" ]]
	then
                expiry_date=`echo "${INPUT_FILE//[^0-9]/}"`
                temp_month_string=`echo "${INPUT_FILE##*$expiry_date}"`
                month_string=`echo "${temp_month_string: 0:3}"`
                expiry_date=`echo "$expiry_date$month_string"`
	else
		temp_expiry_date=`echo "${INPUT_FILE//[^0-9]/}"`
		expiry_date=`echo "${temp_expiry_date: 0:6}"`
	fi
	full_path=`echo "$basepath$INPUT_FILE.csv"`
	echo $full_path
	psql -U tbde -h 127.0.0.1 -d tbde_test <<'OMG' | ...

	-- I have a schema "tmp" for testing purposes   
	-- drop TABLE IF EXISTS company_temp_5;

	--CREATE TABLE IF NOT EXISTS company_temp_5(symbol text, date text, time text, ltp text, ltq text, oi text, bid text, bid_qty text, ask text, ask_qty text, expiry_date text); 
	
	\COPY company_temp_5(date, time, ltp, ltq, oi, bid, bid_qty, ask, ask_qty) from '/$full_path' delimiter ',' ;

	UPDATE company_temp_5 SET symbol = '$entity_name'
	WHERE symbol IS NULL;
OMG
done
