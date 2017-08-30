#!/usr/bin/env bash
source PerfRun.conf


directory=$outputLocation/$(date "+%Y.%m.%d-%H.%M.%S")_$dataSize$queries
mkdir $directory

mv *.out $directory/

latestProp=$directory/latestProp.props

echo aggregator = $aggregator >> $latestProp
echo leafs = $leafs >> $latestProp
echo SERVERS = $leads >> $latestProp
echo DATASIZE = $dataSize >> $latestProp


for i in $directory/*.out
do
   cat $latestProp >> $i
done

#echo snappyData = $(git -C $SnappyData rev-parse HEAD)_$(git -C $SnappyData log -1 --format=%cd) > latestProp.out
#echo snappy-spark = $(git -C $SnappyData/snappy-spark rev-parse HEAD)_$(git -C $SnappyData/snappy-spark log -1 --format=%cd) >> latestProp.out
#echo snappy-store = $(git -C $SnappyData/snappy-store rev-parse HEAD)_$(git -C $SnappyData/snappy-store log -1 --format=%cd) >> latestProp.out
#echo snappy-aqp = $(git -C $SnappyData/snappy-aqp rev-parse HEAD)_$(git -C $SnappyData/snappy-aqp log -1 --format=%cd) >> latestProp.out
#echo spark-jobserver = $(git -C $SnappyData/spark-jobserver rev-parse HEAD)_$(git -C $SnappyData/spark-jobserver log -1 --format=%cd) >> latestProp.out

#echo SPARK_PROPERTIES = $sparkProperties >> latestProp.out
#echo SPARK_SQL_PROPERTIES = $sparkSqlProperties >> latestProp.out
#echo DATASIZE = $dataSize >> latestProp.out


#for i in $directory/*.out
#do 
#   cat latestProp.out >> $i
#done

 

echo "******************Performance Result Generated*****************"
