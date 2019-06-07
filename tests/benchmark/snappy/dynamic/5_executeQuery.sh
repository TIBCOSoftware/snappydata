#!/usr/bin/env bash
source PerfRun.conf

cp PerfRun.conf $leadDir

echo "******************start Executing Query******************"
. $SnappyData/bin/snappy-job.sh submit --lead $leads:8090 \
--app-name QueryExecution --class io.snappydata.benchmark.snappy.tpch.QueryExecutionJob \
--app-jar $TPCHJar \
--conf queries=$queries \
--conf sparkSqlProps=$sparkSqlProperties \
--conf isDynamic=$IsDynamic \
--conf resultCollection=$ResultCollection \
--conf warmUpIterations=$WarmupRuns \
--conf actualRuns=$AverageRuns \
--conf threadNumber=1 \
--conf traceEvents=$traceEvents \
--conf randomSeed=$randomSeed
