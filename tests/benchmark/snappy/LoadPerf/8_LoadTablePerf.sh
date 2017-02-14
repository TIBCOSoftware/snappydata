#!/usr/bin/env bash
source PerfRun.conf

export APP_PROPS="csvFile=$csvDataLocation,parquetFile=$parquetDataLocation,Buckets_Order_Lineitem=$buckets_Order_Lineitem,dataTypes=$dataTypes"

echo "******************start Creating Table******************"
. $SnappyData/bin/snappy-job.sh submit --lead $leads:8090 --app-name myapp --class io.snappydata.benchmark.LoadPerformance.BulkLoad_Snappy --app-jar $TPCHJar
