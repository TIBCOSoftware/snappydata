#!/usr/bin/env bash
source PerfRun.conf

bash $SnappyData/bin/snappy-job.sh --lead $leads:8090 status --job-id $1
