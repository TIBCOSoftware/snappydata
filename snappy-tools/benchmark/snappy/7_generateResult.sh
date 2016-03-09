#!/usr/bin/env bash
source PerfRun.conf

directory=$outputLocation/$(date "+%Y.%m.%d-%H.%M.%S")_$dataSize$queries
mkdir $directory

cp $SnappyData/build-artifacts/scala-2.10/snappy/work/$leads-lead-1/*.out $directory/

latestProp=$SnappyData/build-artifacts/scala-2.10/snappy/benchmark/snappy/latestProp.out

cd $SnappyData
echo snappyData = $(git rev-parse HEAD)_$(git log -1 --format=%cd) > $latestProp
cd snappy-spark
echo snappy-spark = $(git rev-parse HEAD)_$(git log -1 --format=%cd) >> $latestProp
cd ../snappy-store
echo snappy-store = $(git rev-parse HEAD)_$(git log -1 --format=%cd) >> $latestProp
cd ../snappy-aqp
echo snappy-aqp = $(git rev-parse HEAD)_$(git log -1 --format=%cd) >> $latestProp
cd ../spark-jobserver
echo spark-jobserver = $(git rev-parse HEAD)_$(git log -1 --format=%cd) >> $latestProp
cd ../build-artifacts/scala-2.10/snappy/benchmark/snappy/

echo SPARK_PROPERTIES = $sparkProperties >> $latestProp
echo SPARK_SQL_PROPERTIES = $sparkSqlProperties >> $latestProp
echo DATASIZE = $dataSize >> $latestProp


for i in $directory/*.out
do 
   cat $latestProp >> $i
done

 

echo "******************Performance Result Generated*****************"
