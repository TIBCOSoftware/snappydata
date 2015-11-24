#!/usr/bin/env bash
#set -vx 

usage="Usage: snappy-job-submit.sh <hostname:port> <appName> <jobServerAppName> <jarPath-optional>"

if [ $# -lt 3 ] || [ $# -gt 4 ]
then
 echo "ERROR: incorrect argument specified"
 echo $usage
 exit 1
fi

 hostnamePort=$1
 appName=$2
 jobServerAppName=$3
 emptyString=""
 fullString="$hostnamePort/jobs?appName=${appName}&classPath=io.snappydata.examples.${jobServerAppName}"

if [ $# = 4 ]; then
 jarPath=$4
 curl --data-binary @$4 $1\/jars\/$3 $CURL_OPTS
fi
 curl -d "" ${fullString} $CURL_OPTS


