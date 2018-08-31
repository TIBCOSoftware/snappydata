#!/usr/bin/env bash
source PerfRun.conf

#export APP_PROPS="dataLocation=$dataDir,Buckets_Order_Lineitem=$buckets_Order_Lineitem,Buckets_Cust_Part_PartSupp=$buckets_Cust_Part_PartSupp,useIndex=$UseIndex"

cp PerfRun.conf $leadDir

echo "******************start Creating Table******************"
. $SnappyData/bin/snappy-job.sh submit --lead $leads:8090 \
--app-name TableCreation --class io.snappydata.benchmark.snappy.tpch.TableCreationJob \
--app-jar $TPCHJar \
--conf dataLocation=$dataDir \
--conf Buckets_Order_Lineitem=$buckets_Order_Lineitem \
--conf Buckets_Cust_Part_PartSupp=$buckets_Cust_Part_PartSupp \
--conf IsSupplierColumnTable=$IsSupplierColumnTable \
--conf Buckets_Supplier=$buckets_Supplier \
--conf Redundancy=$Redundancy \
--conf Persistence=$Persistence \
--conf Persistence_Type=$Persistence_Type \
--conf NumberOfLoadStages=$NumberOfLoadStages \
--conf isParquet=$Parquet \
--conf createParquet=$createParquet \
--conf traceEvents=$traceEvents
