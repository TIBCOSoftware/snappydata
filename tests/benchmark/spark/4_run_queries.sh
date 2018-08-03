#!/usr/bin/env bash

source PerfRun.conf

threadNumber=1
#Execute Spark App to create tables (Load data from Parquet/ csv files in Spark cache)
bash $sparkHome/bin/spark-submit --master spark://$master:7077 $sparkProperties --class io.snappydata.benchmark.snappy.tpch.QueryTPCHDataSparkApp $TPCHJar $queries $sparkSqlProperties $IsDynamic $ResultCollection $WarmupRuns $AverageRuns ${threadNumber}






