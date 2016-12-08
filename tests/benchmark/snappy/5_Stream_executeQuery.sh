#!/usr/bin/env bash
source PerfRun.conf

#rm -rf $SnappyData/work/$leads-lead-1/*.out
#> $SnappyData/work/$leads-lead-1/Average.out
#export APP_PROPS="queries=$queries,queryPlan=$queryPlan,sparkSqlProps=$sparkSqlProperties,useIndex=$UseIndex"

echo "******************start Executing Query******************"
. $SnappyData/bin/snappy-job.sh submit --lead $leads:8090 --app-name QueryExecution --class io.snappydata.benchmark.snappy.TPCH_Snappy_Query_StreamExecution --app-jar $TPCHJar --conf queries=$queries --conf sparkSqlProps=$sparkSqlProperties --conf useIndex=$UseIndex --conf resultCollection=$ResultCollection --conf warmUpIterations=$WarmupRuns --conf actualRuns=$AverageRuns

#. $SnappyData/bin/snappy-job.sh submit --lead localhost:8090 --app-name myapp --class io.snappydata.cluster.Cluster_TPCH_Snappy_Query --app-jar $TPCHJar

