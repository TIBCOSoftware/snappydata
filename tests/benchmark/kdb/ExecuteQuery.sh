#!/usr/bin/env bash
source PerfRun.conf

scala -cp "$TPCHJar" org.apache.spark.sql.execution.benchmark.TAQ.KDBTaq $WarmupRuns $AverageRuns
