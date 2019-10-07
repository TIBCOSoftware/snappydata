#!/usr/bin/env bash
source PerfRun.conf

echo "*****************Stop locator, server, lead***********************"
$SnappyData/sbin/snappy-stop-all.sh
