#!/usr/bin/env bash
source PerfRun.conf

rm -rf $SnappyData/build-artifacts/scala-2.10/snappy/work/$leads-lead-1/*.out
#> $SnappyData/build-artifacts/scala-2.10/snappy/work/$leads-lead-1/Average.out
export APP_PROPS="queries=$queries,queryPlan=$queryPlan,sparkSqlProps=$sparkSqlProperties,useIndex=$UseIndex"

echo "******************start Executing Query******************"
. $SnappyData/build-artifacts/scala-2.10/snappy/bin/snappy-job.sh submit --lead $leads:8090 --app-name myapp --class io.snappydata.benchmark.snappy.TPCH_Snappy_Query --app-jar $TPCHJar

#. $SnappyData/build-artifacts/scala-2.10/snappy/bin/snappy-job.sh submit --lead localhost:8090 --app-name myapp --class io.snappydata.cluster.Cluster_TPCH_Snappy_Query --app-jar $TPCHJar

