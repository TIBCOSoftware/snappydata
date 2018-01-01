#!/usr/bin/env bash
source PerfRun.conf

#rm -rf $SnappyData/work/$leads-lead-1/*.out
#> $SnappyData/work/$leads-lead-1/Average.out
#export APP_PROPS="queries=$queries,queryPlan=$queryPlan,sparkSqlProps=$sparkSqlProperties,useIndex=$UseIndex"

cp PerfRun.conf $leadDir

echo "******************start Executing Query******************"
. $SnappyData/bin/snappy-job.sh submit --lead $leads:8090 --app-name TPCDSQueryExecution --class io.snappydata.benchmark.snappy.tpch.QueryExecutionTPCDS --app-jar $appJar --conf sparkSqlProps=$sparkSqlProps --conf dataDir=$dataDir --conf queries=$queries --conf queryPath=$queryPath
