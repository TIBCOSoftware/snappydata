#!/usr/bin/env bash
source PerfRun.conf


directory=$outputLocation/$(date "+%Y.%m.%d-%H.%M.%S")
mkdir -p $directory

mv *.out *.csv $directory
cp PerfRun.conf $directory

latestProp=$directory/latestProp.props

echo aggregator = $aggregator >> $latestProp
echo leafs = $leafs >> $latestProp
echo SERVERS = $leads >> $latestProp
echo DATASIZE = $dataSize >> $latestProp

echo "******************Performance Result Generated*****************"
