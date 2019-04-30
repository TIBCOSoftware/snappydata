#!/usr/bin/env bash

source PerfRun.conf

echo "bash $SPARK_HOME/bin/spark-submit --master spark://$master:7077 \
--conf spark.snappydata.connection=$locator:1527 \
$sparkProperties --class io.snappydata.benchmark.snappy.tpch.TableCreationSmartConnector \
$TPCHJar $dataDir $NumberOfLoadStages $Parquet \
$buckets_Order_Lineitem $buckets_Cust_Part_PartSupp $IsSupplierColumnTable \
$buckets_Supplier $Redundancy $Persistence $Persistence_Type $threadNumber"

#Execute Spark App
bash $SPARK_HOME/bin/spark-submit --master spark://$master:7077 \
--conf spark.snappydata.connection=$locator:1527 \
$sparkProperties --class io.snappydata.benchmark.snappy.tpch.TableCreationSmartConnector \
$TPCHJar $dataDir $NumberOfLoadStages $Parquet \
$buckets_Order_Lineitem $buckets_Cust_Part_PartSupp $IsSupplierColumnTable \
$buckets_Supplier $Redundancy $Persistence $Persistence_Type $threadNumber 
