#!/usr/bin/env bash

source PerfRun.conf

# Create slaves configuration files
for element in "${slaves[@]}";
  do
	echo $element >> $SnappyData/build-artifacts/scala-2.10/snappy/conf/slaves
  done
echo "******************Created conf/slaves******************"

#Start master and slaves from master machines
ssh $master sh $SnappyData/build-artifacts/scala-2.10/snappy/sbin/start-all.sh

#Execute Spark App
sh $SnappyData/build-artifacts/scala-2.10/snappy/bin/spark-submit --master spark://$master:7077 $sparkProperties --class io.snappydata.benchmark.snappy.TPCH_Spark $TPCHJar $dataDir $queries $queryPlan





