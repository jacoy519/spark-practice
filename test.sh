#!/bin/bash
START_DATE=$1
END_DATE=$2
START_CAL_DATE=$START_DATE
while [[ $START_CAL_DATE < $END_DATE ]]; do
	END_CAL_DATE=`date -d "+20 day $START_CAL_DATE" +%Y-%m-%d`
	if [[  ${END_DATE} < ${END_CAL_DATE} ]]; then
		END_CAL_DATE=$END_DATE
	fi
	echo "start cal date $START_CAL_DATE"
	echo "end cal date $END_CAL_DATE"


	START_CAL_DATE=$END_CAL_DATE
done

