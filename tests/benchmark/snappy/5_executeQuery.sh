#!/usr/bin/env bash
source PerfRun.conf

#rm -rf $SnappyData/work/$leads-lead-1/*.out
#> $SnappyData/work/$leads-lead-1/Average.out
#export APP_PROPS="queries=$queries,queryPlan=$queryPlan,sparkSqlProps=$sparkSqlProperties,useIndex=$UseIndex"
export APP_PROPS="queries=$queries,sparkSqlProps=$sparkSqlProperties,useIndex=$UseIndex,resultCollection=$ResultCollection,warmUpIterations=$WarmupRuns,actualRuns=$AverageRuns"

echo "******************start Executing Query******************"
. $SnappyData/bin/snappy-job.sh submit --lead $leads:8090 --app-name QueryExecution --class io.snappydata.benchmark.snappy.TPCH_Snappy_Query --app-jar $TPCHJar

#. $SnappyData/bin/snappy-job.sh submit --lead localhost:8090 --app-name myapp --class io.snappydata.cluster.Cluster_TPCH_Snappy_Query --app-jar $TPCHJar

