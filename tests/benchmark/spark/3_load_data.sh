#!/usr/bin/env bash

source PerfRun.conf

threadNumber=1
#Execute Spark App to create tables (Load data from Parquet/ csv files in Spark cache)
bash $sparkHome/bin/spark-submit --master spark://$master:7077 $sparkProperties --class io.snappydata.benchmark.snappy.tpch.LoadTPCHDataSparkApp $TPCHJar $dataDir $NumberOfLoadStages $Parquet  ${threadNumber} $rePartition $IsSupplierColumnTable $buckets_Supplier $buckets_Order_Lineitem $buckets_Cust_Part_PartSupp






