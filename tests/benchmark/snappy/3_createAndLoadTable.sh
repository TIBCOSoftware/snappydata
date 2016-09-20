#!/usr/bin/env bash
source PerfRun.conf

#export APP_PROPS="dataLocation=$dataDir,Buckets_Order_Lineitem=$buckets_Order_Lineitem,Buckets_Cust_Part_PartSupp=$buckets_Cust_Part_PartSupp,useIndex=$UseIndex"
export APP_PROPS="dataLocation=$dataDir,Buckets_Order_Lineitem=$buckets_Order_Lineitem,Buckets_Cust_Part_PartSupp=$buckets_Cust_Part_PartSupp,useIndex=$UseIndex,Buckets_Nation_Region_Supp=$buckets_Nation_Region_Supp,Nation_Region_Supp_col=$Nation_Region_Supp_col"

echo "******************start Creating Table******************"
. $SnappyData/bin/snappy-job.sh submit --lead $leads:8090 --app-name TableCreation --class io.snappydata.benchmark.snappy.TPCH_Snappy_Tables --app-jar $TPCHJar

#. $SnappyData/bin/snappy-job.sh submit --lead localhost:8090 --app-name myapp --class io.snappydata.cluster.Cluster_TPCH_Snappy_Tables --app-jar $TPCHJar
