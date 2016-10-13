#!/usr/bin/env bash
source PerfRun.conf

#run table creating program
echo "========================Create Tables======================================"
scala -cp "$TPCHJar:$mysqlConnectorJar" io.snappydata.benchmark.memsql.TPCH_Memsql_Tables $aggregator $port

#change data size as per requirment. e.g. from 100GB to 10GB
echo "=============================Change data size in josn files=========================="
sed -i "s|\"file:.*|\"file:$dataDir\/customer.tbl\"|" $memsqlLoader/customer.json
sed -i "s|\"file:.*|\"file:$dataDir\/lineitem.tbl\"|" $memsqlLoader/lineitem.json
sed -i "s|\"file:.*|\"file:$dataDir\/nation.tbl\"|" $memsqlLoader/nation.json
sed -i "s|\"file:.*|\"file:$dataDir\/orders.tbl\"|" $memsqlLoader/orders.json
sed -i "s|\"file:.*|\"file:$dataDir\/part.tbl\"|" $memsqlLoader/part.json
sed -i "s|\"file:.*|\"file:$dataDir\/partsupp.tbl\"|" $memsqlLoader/partsupp.json
sed -i "s|\"file:.*|\"file:$dataDir\/region.tbl\"|" $memsqlLoader/region.json
sed -i "s|\"file:.*|\"file:$dataDir\/supplier.tbl\"|" $memsqlLoader/supplier.json

echo "=============================Change host name in josn files=========================="
sed -i "s|\"host\": .*|\"host\": \"$ip_aggregator\",|" $memsqlLoader/*.json
sed -i "s|\"port\": .*|\"port\": $port|" $memsqlLoader/*.json

echo "=============================load tables=========================="
#//load data in tables
$memsqlLoader/memsql-loader load --spec $memsqlLoader/customer.json
$memsqlLoader/memsql-loader load --spec $memsqlLoader/lineitem.json
$memsqlLoader/memsql-loader load --spec $memsqlLoader/nation.json
$memsqlLoader/memsql-loader load --spec $memsqlLoader/orders.json
$memsqlLoader/memsql-loader load --spec $memsqlLoader/part.json
$memsqlLoader/memsql-loader load --spec $memsqlLoader/partsupp.json
$memsqlLoader/memsql-loader load --spec $memsqlLoader/region.json
$memsqlLoader/memsql-loader load --spec $memsqlLoader/supplier.json

$memsqlLoader/memsql-loader ps

