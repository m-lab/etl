#!/bin/bash
year=$1
for month in $(seq -w 01 12);
  do for day in $(seq -w 01 31); do ./convert-legacy-to-common.sh $year-$month-$day &  done;
done;