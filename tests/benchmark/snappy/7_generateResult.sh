#!/usr/bin/env bash
source PerfRun.conf

directory=$outputLocation/$(date "+%Y.%m.%d-%H.%M.%S")_$dataSize$queries$UseIndex
mkdir $directory

cp $SnappyData/build-artifacts/scala-2.10/snappy/work/$leads-lead-1/*.out $directory/

latestProp=$directory/latestProp.props

cd $SnappyData
echo snappyData = $(git rev-parse HEAD)_$(git log -1 --format=%cd) > $latestProp
cd spark
echo snappy-spark = $(git rev-parse HEAD)_$(git log -1 --format=%cd) >> $latestProp
cd ../store
echo snappy-store = $(git rev-parse HEAD)_$(git log -1 --format=%cd) >> $latestProp
#cd ../aqp
#echo snappy-aqp = $(git rev-parse HEAD)_$(git log -1 --format=%cd) >> $latestProp
cd ../spark-jobserver
echo spark-jobserver = $(git rev-parse HEAD)_$(git log -1 --format=%cd) >> $latestProp
cd $SnappyData/tests/benchmark/snappy/

echo SPARK_PROPERTIES = $sparkProperties >> $latestProp
echo SPARK_SQL_PROPERTIES = $sparkSqlProperties >> $latestProp
echo serverMemory = $serverMemory >> $latestProp
echo LineItem_Order_NoOfBuckets = $buckets_Order_Lineitem >> $latestProp
echo Cutomer_Part_PartSupp_NoOfBuckets = $buckets_Cust_Part_PartSupp >> $latestProp
echo UseIndex = $UseIndex >> $latestProp
echo DATASIZE = $dataSize >> $latestProp
echo LOCATOR = $locator >> $latestProp
echo LEAD = $leads >> $latestProp
for element in "${servers[@]}";
  do
       echo SERVERS = $element >> $latestProp 
  done


for i in $directory/*.out
do 
   cat $latestProp >> $i
done

 

echo "******************Performance Result Generated*****************"
