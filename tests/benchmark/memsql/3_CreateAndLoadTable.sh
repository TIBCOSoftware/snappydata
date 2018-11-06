#!/usr/bin/env bash
source PerfRun.conf

#run table creating program
echo "========================Create Tables======================================"
scala -cp "$TPCHJar:$mysqlConnectorJar" \
io.snappydata.benchmark.memsql.TPCH_Memsql_Tables \
$aggregator \
$port \
$dataDir \
$numberOfDataLoadingStages

