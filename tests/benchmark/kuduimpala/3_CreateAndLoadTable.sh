#!/usr/bin/env bash
source PerfRun.conf

#run table creating program
echo "========================Create Tables======================================"
scala -cp "$TPCHJar:$mysqlConnectorJar" io.snappydata.benchmark.kuduimpala.TPCH_Impala_Tables $aggregator $port