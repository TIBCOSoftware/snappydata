#!/usr/bin/env bash
source PerfRun.conf

directory=$outputLocation/$(date "+%Y.%m.%d-%H.%M.%S")_$dataSize$queries
mkdir $directory

cp $SnappyData/build-artifacts/scala-2.10/snappy/work/$leads-lead-1/*.out $directory/

latestProp=$directory/latestProp.props

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
echo serverMemory = $serverMemory >> $latestProp
echo NoOfBuckets = $serverMemory >> $latestProp
echo DATASIZE = $dataSize >> $latestProp
echo LOCATOR = $locator >> $latestProp
echo LEAD = $leads >> $latestProp
echo SERVERS = $servers >> $latestProp



for i in $directory/*.out
do 
   cat $latestProp >> $i
done

 

echo "******************Performance Result Generated*****************"
