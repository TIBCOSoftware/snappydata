#!/usr/bin/env bash
source PerfRun.conf

echo "******************start Creating Table******************"
. $SnappyData/bin/snappy-job.sh submit --lead $leads:8090 --app-name TableCreation --class org.apache.spark.sql.execution.benchmark.TAQ.SnappyTaq --app-jar $TPCHJar --conf dataLocation=$dataDir --conf resultCollection=$ResultCollection --conf warmUpIterations=$WarmupRuns --conf actualRuns=$AverageRuns
#. $SnappyData/bin/snappy-job.sh submit --lead localhost:8090 --app-name myapp --class io.snappydata.cluster.Cluster_TPCH_Snappy_Tables --app-jar $TPCHJar
