#!/usr/bin/env bash
#set -vx 

usage=$'Usage: snappy-job.sh submit --lead <hostname:port> --app-name <app-name> --class <job-class> [--app-jar jar-path]
       snappy-job.sh status --lead <hostname:port> --job-id <job-id>'

function showUsage {
  echo "ERROR: incorrect argument specified: " "$@"
  echo "$usage"
  exit 1
}

while (( "$#" )); do

param="$1"

case $param in
    submit)
    submit="true"
    ;;
    status)
    submit="false"
    ;;
    --lead)
    shift
    hostnamePort="$1"
    ;;
    --app-name)
    shift
    appName="$1"
    ;;
    --class)
    shift
    jobClass="$1"
    ;;
    --app-jar)
    shift
    appjar="$1"
    ;;
    --job-id)
    shift
    jobID="$1"
    ;;
    *)
    showUsage $1
    ;;
esac
shift
done


# verify parameters

if [[ $submit = "true" ]]; then
  if [[ $jobID != "" ]]; then
    showUsage "--job-id"
  fi
elif [[ $submit = "false" ]]; then
  if [[ $appName != "" ]]; then
    showUsage "--app-name"
  elif [[ $jobClass != "" ]]; then
    showUsage "--class"
  elif [[ $appjar != "" ]]; then
    showUsage "--app-jar"
  fi
else
  showUsage
fi

if [[ $submit = "true" ]]; then
  jobServerURL="$hostnamePort/jobs?appName=${appName}&classPath=${jobClass}"
  if [[ $appjar != "" ]]; then
    curl --data-binary @$appjar $hostnamePort\/jars\/$appName $CURL_OPTS
  fi
  curl -d "" ${jobServerURL} $CURL_OPTS
else
  curl ${hostnamePort}\/jobs\/${jobID} $CURL_OPTS
fi

