#!/usr/bin/env bash
source PerfRun.conf

scala -cp "$TPCHJar:$mysqlConnectorJar" \
io.snappydata.benchmark.memsql.TPCH_Memsql_Query_StreamExecution \
$aggregator \
$port \
$queries  \
$ResultCollection \
$WarmupRuns \
$AverageRuns \
$randomSeed
