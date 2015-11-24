#!/usr/bin/env bash

usage="Usage: snappy-job-status.sh <hostname:port> <job-id>"

if [ $# != 2 ]
then
 echo "ERROR: incorrect argument specified"
 echo $usage
 exit 1
fi

 hostnamePort=$1
 jobID=$2
 curl ${hostnamePort}\/jobs\/${jobID}
