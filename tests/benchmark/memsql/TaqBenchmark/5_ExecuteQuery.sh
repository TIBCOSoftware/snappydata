#!/usr/bin/env bash
source PerfRun.conf

scala -cp "$TPCHJar:$mysqlConnectorJar" org.apache.spark.sql.execution.benchmark.TAQ.MemsqlTaq $aggregator $port $ResultCollection $WarmupRuns $AverageRuns
