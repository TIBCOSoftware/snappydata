#!/bin/bash
#
# compare the performance of some number of runs
#

trap 'exit 1' 2 #traps Ctrl-C (signal 2)

USAGE="Usage: $0 dir"

if [ "$#" == "0" ]; then
  echo "$USAGE"
  exit 1
fi

java -ea -cp $Snappydata/cluster/build-artifacts/scala-2.11/libs/snappydata-cluster_2.11-1.3.0-tests.jar io.snappydata.benchmark.snappy.TPCHPerfComparer $1