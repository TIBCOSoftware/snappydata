#!/usr/bin/env sh
#set -vx
source configFile.conf

if [ $# -ne 3 ]; then
    echo "ERROR: incorrect argument specified: " "$@"
    echo "Usage:./runStreamingApp <appName> <cdc_source_connection.properties> <source_destination.properties>"
    exit 1
fi

#sh $BuildPath/bin/spark-submit --class io.snappydata.app.JavaCdcStreamingApp --name $1 --master spark://dev11.telx.snappydata.io:7077 --total-executor-cores=15 --conf snappydata.connection=dev11:1527 --conf spark.sql.defaultSizeInBytes=1000 --conf spark.driver.memory=2g --conf spark.executor.memory=4g --conf snappydata.store.memory-size=6g --conf spark.locality.wait=30 --conf spark.local.dir=/nfs/users/spillai/tmp1 --jars /export/dev11a/users/spillai/snappydata/snappy-connectors/jdbc-stream-connector/build-artifacts/scala-2.11/libs/snappydata-jdbc-stream-connector_2.11-0.9.1.jar,/export/dev11a/users/spillai/mssql-jdbc/mssql-jdbc-6.1.0.jre8.jar /export/dev11a/users/spillai/snappydata/snappy-poc/cdc/target/original-cdc-test-0.0.1.jar $2 $3

sh $SnappyData/bin/spark-submit --class io.snappydata.app.JavaCdcStreamingApp --name $1 --master spark://dev11.telx.snappydata.io:7077 --total-executor-cores=6 --conf snappydata.connection=dev11:1527 --conf spark.sql.defaultSizeInBytes=1000 --conf spark.driver.memory=2g --conf spark.executor.memory=4g --conf snappydata.store.memory-size=5g --conf spark.locality.wait=30 --conf spark.local.dir=/nfs/users/spillai/tmp1 --jars $connectorJar,$driverJar $pocAppJar $2 $3
