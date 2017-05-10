#!/usr/bin/env bash
source PerfRun.conf

echo "******************start Creating Table******************"
. $SnappyData/bin/snappy-job.sh submit --lead $leads:8090 --app-name DataValidation --class io.snappydata.benchmark.snappy.tpch.DataValidationJob --app-jar $TPCHJar --conf ExpectedResultsAvailableAt=$ExpectedResultsAvailableAt --conf ActualResultsAvailableAt=$ActualResultsAvailableAt --conf isDynamic=$IsDynamic --conf queries=$queries
