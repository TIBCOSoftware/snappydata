#!/usr/bin/env bash
source PerfRun.conf

directory=$outputLocation/$(date "+%Y.%m.%d-%H.%M.%S")_$dataSize$queries
mkdir $directory

mv *.out $directory/

latestProp=$directory/latestProp.props

cd $SnappyData/snappy-spark
echo snappy-spark = $(git rev-parse HEAD)_$(git log -1 --format=%cd) >> $latestProp
cd ../build-artifacts/scala-2.10/snappy/benchmark/snappy/

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
