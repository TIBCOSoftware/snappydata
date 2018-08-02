#!/usr/bin/env bash
source PerfRun.conf

directory=$outputLocation/$(date "+%Y.%m.%d-%H.%M.%S")_$dataSize$queries
mkdir -p  $directory

mv *.csv *.out $directory/
cp PerfRun.conf $directory/
cp $sparkHome/conf/spark-env.sh $sparkHome/conf/spark-default.conf $directory/
mv *.csv *.out $directory/

latestProp=$directory/latestProp.props

echo spark = $(git rev-parse HEAD)_$(git log -1 --format=%cd) >> $latestProp

echo SPARK_PROPERTIES = $sparkProperties >> $latestProp
echo DATASIZE = $dataSize >> $latestProp
echo MASTER = $master >> $latestProp
echo SLAVES = $slaves >> $latestProp
echo CLIENT = $client >> $latestProp

echo "******************Performance Result Generated*****************"
