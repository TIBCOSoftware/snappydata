#!/usr/bin/env bash

source PerfRun.conf

#Execute Spark App
bash $SPARK_HOME/bin/spark-submit \
--master spark://$master:7077 \
--conf spark.snappydata.connection=$locator:1527 \
$sparkProperties --class io.snappydata.benchmark.snappy.tpch.QueryExecutionSmartConnector \
$TPCHJar $queries $sparkSqlProperties $IsDynamic $ResultCollection $WarmupRuns $AverageRuns $threadNumber
