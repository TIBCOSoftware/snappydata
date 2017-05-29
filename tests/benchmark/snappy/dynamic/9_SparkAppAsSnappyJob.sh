#!/usr/bin/env bash
source PerfRun.conf

#export APP_PROPS="dataLocation=$dataDir,Buckets_Order_Lineitem=$buckets_Order_Lineitem,Buckets_Cust_Part_PartSupp=$buckets_Cust_Part_PartSupp,useIndex=$UseIndex"

echo "******************start Creating Table******************"
. $SnappyData/bin/snappy-job.sh submit --lead $leads:8090 --app-name SparkApp --class io.snappydata.benchmark.snappy.tpch.SparkAppUsingJob --app-jar $TPCHJar --conf dataLocation=$dataDir --conf NumberOfLoadStages=$NumberOfLoadStages --conf isParquet=$Parquet --conf queries=$queries --conf sparkSqlProps=$sparkSqlProperties --conf isDynamic=$IsDynamic --conf resultCollection=$ResultCollection --conf warmUpIterations=$WarmupRuns --conf actualRuns=$AverageRuns --conf threadNumber=1
