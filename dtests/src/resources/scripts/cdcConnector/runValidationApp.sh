#!/usr/bin/env sh
#set -vx
source configFile.conf

if [ $# -ne 2 ]; then
    echo "ERROR: incorrect argument specified: " "$@"
    echo "Usage:./runValidationApp <filePath with list of ; seperated tablename> <order of execution>"
    exit 1
fi

sh $SnappyData/bin/spark-submit --class io.snappydata.hydra.cdcConnector.cdcValidationApp --jars $driverJar $validateAppJar $1 $2