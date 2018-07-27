#!/usr/bin/env bash
source PerfRun.conf

directory=$outputLocation/$(date "+%Y.%m.%d-%H.%M.%S")_$dataSize$queries
mkdir $directory

mv *.out $directory/

latestProp=$directory/latestProp.props

cd $sparkHome/snappy-spark
echo spark = $(git rev-parse HEAD)_$(git log -1 --format=%cd) >> $latestProp

echo SPARK_PROPERTIES = $sparkProperties >> $latestProp
echo DATASIZE = $dataSize >> $latestProp
echo MASTER = $master >> $latestProp
echo SLAVES = $slaves >> $latestProp
echo CLIENT = $client >> $latestProp

for i in $directory/*.out
do
   cat $latestProp >> $i
done



echo "******************Performance Result Generated*****************"
