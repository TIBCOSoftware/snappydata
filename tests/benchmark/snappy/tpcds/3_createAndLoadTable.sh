#!/usr/bin/env bash
source PerfRun.conf

#export APP_PROPS="dataLocation=$dataDir,Buckets_Order_Lineitem=$buckets_Order_Lineitem,Buckets_Cust_Part_PartSupp=$buckets_Cust_Part_PartSupp,useIndex=$UseIndex"

cp PerfRun.conf $leadDir

echo "******************start Creating Table******************"
. $SnappyData/bin/snappy-job.sh submit --lead $leads:8090 --app-name TableCreation --class io.snappydata.benchmark.snappy.tpcds.TableCreationJob --app-jar $appJar --conf sparkSqlProps=$sparkSqlProps --conf dataDir=$dataDir --conf Buckets_ColumnTable=$buckets_ColumnTable
