#!/usr/bin/env bash
source PerfRun.conf

export JAVA_OPTS="-Xmx4g -Xms2g"
scala -cp "$TPCHJar:$mysqlConnectorJar" \
io.snappydata.benchmark.memsql.TPCH_Memsql_Query \
$aggregator \
$port \
$queries \
$ResultCollection \
$WarmupRuns \
$AverageRuns \
$isDynamic \
$randomSeed

