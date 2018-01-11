#!/usr/bin/env bash
source PerfRun.conf

echo "*****************Stop locator, server, lead***********************"
sh $SnappyData/sbin/snappy-stop-all.sh
