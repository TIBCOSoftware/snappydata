#!/usr/bin/env bash
source PerfRun.conf.template

scala -cp "../TPCH.jar:mysql-connector-java-5.0.8-bin.jar" io.snappydata.benchmark.memsql.TPCH_Memsql_Query $aggregator $queries
