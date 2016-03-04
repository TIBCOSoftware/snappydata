#!/usr/bin/env bash
source PerfRun.conf

#run table creating program
scala -cp "../TPCH.jar:mysql-connector-java-5.0.8-bin.jar" io.snappydata.benchmark.memsql.TPCH_Memsql_Tables $aggregator

#change data size as per requirment. e.g. from 100GB to 10GB
sed -i "s|\"file:.*|\"file:$dataDir\/customer.tbl\"|" memsqlloader/customer.json
sed -i "s|\"file:.*|\"file:$dataDir\/lineitem.tbl\"|" memsqlloader/lineitem.json
sed -i "s|\"file:.*|\"file:$dataDir\/nation.tbl\"|" memsqlloader/nation.json
sed -i "s|\"file:.*|\"file:$dataDir\/orders.tbl\"|" memsqlloader/orders.json
sed -i "s|\"file:.*|\"file:$dataDir\/part.tbl\"|" memsqlloader/part.json
sed -i "s|\"file:.*|\"file:$dataDir\/partsupp.tbl\"|" memsqlloader/partsupp.json
sed -i "s|\"file:.*|\"file:$dataDir\/region.tbl\"|" memsqlloader/region.json
sed -i "s|\"file:.*|\"file:$dataDir\/supplier.tbl\"|" memsqlloader/supplier.json


sed -i "s|\"host\": .*|\"host\": \"$aggregator\",|" memsqlloader/*.json

#//load data in tables
./memsqlloader/memsql-loader load --spec memsqlloader/customer.json 
./memsqlloader/memsql-loader load --spec memsqlloader/lineitem.json 
./memsqlloader/memsql-loader load --spec memsqlloader/nation.json 
./memsqlloader/memsql-loader load --spec memsqlloader/orders.json 
./memsqlloader/memsql-loader load --spec memsqlloader/part.json 
./memsqlloader/memsql-loader load --spec memsqlloader/partsupp.json 
./memsqlloader/memsql-loader load --spec memsqlloader/region.json 
./memsqlloader/memsql-loader load --spec memsqlloader/supplier.json 

./memsqlloader/memsql-loader ps

