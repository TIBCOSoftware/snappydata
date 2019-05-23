#!/usr/bin/env bash

source PerfRun.conf

# Create slaves configuration files
for element in "${slaves[@]}";
  do
	echo $element >> $SPARK_HOME/conf/slaves
  done
echo "******************Created conf/slaves******************"

#Start master and slaves from master machines
ssh $master bash $SPARK_HOME/sbin/start-all.sh


