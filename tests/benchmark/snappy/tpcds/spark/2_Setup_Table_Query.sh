#!/usr/bin/env bash

source PerfRun.conf

# Create slaves configuration files
for element in "${slaves[@]}";
  do
	echo $element >> $SnappyData/conf/slaves
  done
echo "******************Created conf/slaves******************"

#Start master and slaves from master machines
ssh $master sh $SnappyData/sbin/start-all.sh

#Execute Spark App
bash $SnappyData/bin/spark-submit \
--master spark://$master:7077 \
--class io.snappydata.benchmark.snappy.tpcds.SparkApp \
$appJar $sparkSqlProperties $dataDir $queries $queryPath $buckets_ColumnTable $ResultCollection $WarmupRuns $AverageRuns



