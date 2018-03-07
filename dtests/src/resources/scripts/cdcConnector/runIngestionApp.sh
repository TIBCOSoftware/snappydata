#!/usr/bin/env sh
#set -vx
source configFile.conf

if [ $# -ne 2 ]; then
    echo "ERROR: incorrect argument specified: " "$@"
    echo "Usage:./runIngestionApp <starting Range> <ending range>"
    echo "eg: ./runIngestionApp 1 10 ,this will ingest 1 to 10 records."
    exit 1
fi

sh $SnappyData/bin/spark-submit --class io.snappydata.hydra.cdcConnector.cdcIngestionApp --jars $driverJar $ingestionAppJar $1 $2