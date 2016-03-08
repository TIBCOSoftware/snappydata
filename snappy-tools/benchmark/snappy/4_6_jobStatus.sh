#!/usr/bin/env bash
source PerfRun.conf.template

. $SnappyData/build-artifacts/scala-2.10/snappy/bin/snappy-job.sh status --job-id $1
