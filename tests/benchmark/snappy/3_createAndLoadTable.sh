#!/usr/bin/env bash
source PerfRun.conf

export APP_PROPS="dataLocation=$dataDir,Buckets_Order_Lineitem=$buckets_Order_Lineitem,Buckets_Cust_Part_PartSupp=$buckets_Cust_Part_PartSupp,useIndex=$UseIndex"

echo "******************start Creating Table******************"
. $SnappyData/build-artifacts/scala-2.10/snappy/bin/snappy-job.sh submit --lead $leads:8090 --app-name myapp --class io.snappydata.benchmark.snappy.TPCH_Snappy_Tables --app-jar $TPCHJar

#. $SnappyData/build-artifacts/scala-2.10/snappy/bin/snappy-job.sh submit --lead localhost:8090 --app-name myapp --class io.snappydata.cluster.Cluster_TPCH_Snappy_Tables --app-jar $TPCHJar
