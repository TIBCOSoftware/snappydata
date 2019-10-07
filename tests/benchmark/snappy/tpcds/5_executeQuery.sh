#!/usr/bin/env bash
source PerfRun.conf

#rm -rf $SnappyData/work/$leads-lead-1/*.out
#> $SnappyData/work/$leads-lead-1/Average.out
#export APP_PROPS="queries=$queries,queryPlan=$queryPlan,sparkSqlProps=$sparkSqlProperties,useIndex=$UseIndex"

cp PerfRun.conf $leadDir

echo "******************start Executing Query******************"
. $SnappyData/bin/snappy-job.sh submit \
--lead $leads:8090 \
--app-name QueryExecution \
--class io.snappydata.benchmark.snappy.tpcds.QueryExecutionJob \
--app-jar $appJar \
--conf queries=$queries \
--conf sparkSqlProps=sparkSqlProps \
--conf resultCollection=$ResultCollection \
--conf warmUpIterations=$WarmupRuns \
--conf actualRuns=$AverageRuns \
--conf queryPath=$queryPath
