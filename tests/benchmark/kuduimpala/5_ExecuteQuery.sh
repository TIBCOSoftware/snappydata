#!/usr/bin/env bash
source PerfRun.conf

scala -cp "$TPCHJar:$mysqlConnectorJar" io.snappydata.benchmark.kuduimpala.TPCH_Impala_Query $aggregator $port $queries  $ResultCollection $WarmupRuns $AverageRuns