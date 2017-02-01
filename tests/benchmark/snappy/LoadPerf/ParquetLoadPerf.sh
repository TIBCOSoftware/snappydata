#!/usr/bin/env bash
source PerfRun.conf

export APP_PROPS="airline_file=$parquetDataLocation"

echo "******************start Creating Table******************"
. $SnappyData/bin/snappy-job.sh submit --lead $leads:8090 --app-name myapp --class io.snappydata.benchmark.LoadPerformance.ParquetLoad --app-jar $TPCHJar

