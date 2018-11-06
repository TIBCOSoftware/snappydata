#!/usr/bin/env bash

#
# Copyright (c) 2018 SnappyData, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License. You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License. See accompanying
# LICENSE file.
#

#set -vx 

function absPath() {
    perl -MCwd -le 'print Cwd::abs_path(shift)' "$1"
}

if [ -z "${SNAPPY_HOME}" ]; then
  export SNAPPY_HOME="$(absPath "$(dirname "$(absPath "$0")")/..")"
fi
if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$SNAPPY_HOME"
fi

usage=$'Usage: 
       # Create a new context using the provided context factory
       snappy-job.sh newcontext <context-name> --factory <factory class name> [--lead <hostname:port>]
         [--app-jar <jar-path> --app-name <app-name>] [--conf <property=value>]
         [--passfile <config-file-path-with-credentials>]
       # Submit a job, optionally with a provided context or create a streaming-context and use it with the job
       snappy-job.sh submit --app-name <app-name> --class <job-class> [--lead <hostname:port>]
         [--app-jar <jar-path>] [--context <context-name> | --stream] [--conf <property=value>]
         [--passfile <config-file-path-with-credentials>] [--batch-interval <Stream batch interval in millis>]
         [--packages <comma separated package-coordinates> ]
         [--repos <comma separated mvn repositories] [--jarcache <path where resolved jars will be kept]
       # Get status of the job with the given job-id
       snappy-job.sh status --job-id <job-id> [--lead <hostname:port>] [--passfile <config-file-path-with-credentials>]
       # Stop a job with the given job-id
       snappy-job.sh stop --job-id <job-id> [--lead <hostname:port>] [--passfile <config-file-path-with-credentials>]
       # List all the current contexts
       snappy-job.sh listcontexts [--lead <hostname:port>] [--passfile <config-file-path-with-credentials>]
       # Stop a context with the given name
       snappy-job.sh stopcontext <context-name> [--lead <hostname:port>] [--passfile <config-file-path-with-credentials>]'

function showUsage {
  echo "ERROR: incorrect argument specified: " "$@"
  echo "$usage"
  exit 1
}

hostnamePort=
appName=
jobClass=
appjar=
jobID=
contextName=
contextFactory=
newContext=
TOK_EMPTY="EMPTY"
APP_PROPS=$APP_PROPS
securePart=""
batchInterval=
packages=
repos=
jarcache=
alljars=

while (( "$#" )); do
  param="$1"
  case $param in
    submit)
      cmd="jobs"
    ;;
    status)
      cmd="status"
    ;;
    stop)
      cmd="stop"
    ;;
    newcontext)
      cmd="newcontext"
      shift
      contextName="${1:-$TOK_EMPTY}"
    ;;
    --lead)
      shift
      hostnamePort="${1:-$TOK_EMPTY}"
    ;;
    --app-name)
      shift
      appName="${1:-$TOK_EMPTY}"
    ;;
    --class)
      shift
      jobClass="${1:-$TOK_EMPTY}"
    ;;
    --app-jar)
      shift
      appjar="${1:-$TOK_EMPTY}"
    ;;
    --packages)
      shift
      packages="${1:-$TOK_EMPTY}"
    ;;
    --repos)
      shift
      repos="${1:-$TOK_EMPTY}"
    ;;
    --jarcache)
      shift
      jarcache="${1:-$TOK_EMPTY}"
    ;;
    --job-id)
      shift
      jobID="${1:-$TOK_EMPTY}"
    ;;
    --factory)
      shift
      contextFactory="${1:-$TOK_EMPTY}"
    ;;
    --context)
      shift
      contextName="${1:-$TOK_EMPTY}"
    ;;
    --conf)
      shift
      if [[ -z "$APP_PROPS" ]]; then
        APP_PROPS="${1:-$TOK_EMPTY}"
      else
        APP_PROPS=$APP_PROPS",""${1:-$TOK_EMPTY}"
      fi
    ;;
    --stream)
      if [[ $contextName != "" || $cmd != "jobs" ]]; then
        showUsage "--context ${contextName} AND --stream"
      fi
      newContext="yes"
      contextName="snappyStreamingContext"$(date +%s%N)
      contextFactory="org.apache.spark.sql.streaming.SnappyStreamingContextFactory"
    ;;
    listcontexts)
      cmd="listcontexts"
    ;;
    stopcontext)
      cmd="stopcontext"
      shift
      contextName="${1:-$TOK_EMPTY}"
    ;;
    --passfile)
      shift
      passwordfile="${1:-$TOK_EMPTY}"
      if [[ ! -e $passwordfile ]]; then
        echo "The config file $passwordfile not found."
        exit 1
      fi
      securePart=" --config ${passwordfile}"
    ;;
    --batch-interval)
      shift
      batchInterval="${1:-$TOK_EMPTY}"
      if [[ $contextFactory != "org.apache.spark.sql.streaming.SnappyStreamingContextFactory" ]]; then
        echo "Non Streaming job. Batch interval config parameter will not be used."
      fi
    ;;
    *)
      showUsage $1
    ;;
  esac
  shift
done


validateOptionalArg() {
 arg=$1
 if [[ -z $arg ]]; then
   return 1 # false
 fi

 validateArg $arg
 return $?
}

validateArg() {
 arg=$1
 if [[ $arg == "" || $arg == $TOK_EMPTY ||
       ${arg:0:2} == "--" ]]; then
   return 0 # true
 fi

 return 1
}

# command builder
cmdLine=

function buildCommand () {
case $cmd in
  status)
     if validateArg $jobID ; then
       showUsage "--job-id"
     fi
     cmdLine="jobs/${jobID}"
  ;;

  jobs)
    if validateArg $appName ; then
      showUsage "--app-name"
    elif validateArg $jobClass ; then
      showUsage "--class"
    elif validateOptionalArg $appjar ; then
        showUsage "--app-jar"
    elif validateOptionalArg $packages ; then
        showUsage "--packages"
    elif validateOptionalArg $repos ; then
        showUsage "--repos"
    elif validateOptionalArg $jarcache ; then
        showUsage "--jarcache"
    elif validateOptionalArg $contextName ; then
      showUsage "--context"
    fi
    cmdLine="jobs?appName=${appName}&classPath=${jobClass}"

    if [[ -n $contextName ]]; then
      cmdLine="${cmdLine}&context=${contextName}"
    fi
  ;;

  stop)
     if validateArg $jobID ; then
       showUsage "--job-id"
     fi
     cmdLine="jobs/${jobID}"
  ;;

  newcontext)
    if validateArg $contextName ; then
      showUsage "newcontext <context-name>"
    elif validateArg $contextFactory ; then
      showUsage "--factory"
    elif validateOptionalArg $appjar ; then
      showUsage "--app-jar"
    elif [[ $appjar != "" ]] && validateArg $appName ; then
      showUsage "--app-name"
    fi
    cmdLine="contexts/${contextName}?context-factory=${contextFactory}"

    if [[ -n $batchInterval ]]; then
      cmdLine="${cmdLine}&streaming.batch_interval=${batchInterval}"
    fi
  ;;

  listcontexts)
    cmdLine="contexts"
  ;;

  stopcontext)
    if validateArg $contextName ; then
      showUsage "stopcontext <context-name>"
    fi
    cmdLine="contexts/${contextName}"
  ;;

  *)
    showUsage
esac
}

if [[ $cmd == "jobs" && -z $newContext && -z $contextName ]]; then
  contextName="snappyContext"$(date +%s%N)
  contextFactory="org.apache.spark.sql.SnappySessionFactory"
  newContext="yes"
fi

function addDependentJarsToProp () {
  if [[ $packages != "" ]]; then
    JAR_FOLDER=$SNAPPY_HOME/jars
    jarclasspath=`echo $JAR_FOLDER/*.jar | tr -s ' ' ':'`
    depargs=
    if [ ! -z $repos ]; then
      depargs="-- repos $repos"
    fi
    if [ ! -z $jarcache ]; then
      depargs="$depargs --jarcache $jarcache"
    fi
    depargs="$depargs $packages"
    depjars=`${SPARK_HOME}/bin/spark-class -cp $jarclasspath org.apache.spark.deploy.GetJarsAndDependencies $depargs`
    depjars=`echo $depjars | sed -e "s/,/|/g"`
    if [[ -z "$APP_PROPS" ]]; then
      APP_PROPS="dependent-jar-uris=$depjars"
    else
      APP_PROPS=$APP_PROPS",dependent-jar-uris=$depjars"
    fi
  fi
}

buildCommand

# build command for new context, if needed.
if [[ -n $newContext ]]; then
  cmd="newcontext"
  jobsCommand=$cmdLine
  buildCommand
  newContext=$cmdLine
  cmdLine=$jobsCommand
fi


if [[ -z $hostnamePort ]]; then
  hostnamePort=localhost:8090
fi


# invoke command

jobServerURL="$hostnamePort/${cmdLine}"

addDependentJarsToProp

case $cmd in
  jobs | newcontext)
    if [[ $appjar != "" ]]; then
      curl --data-binary @$appjar $hostnamePort\/jars\/$appName $CURL_OPTS ${securePart}
    fi

    if [[ $newContext != "" ]]; then
      curl -d "${APP_PROPS}" ${hostnamePort}/${newContext} $CURL_OPTS ${securePart}
    fi

    curl -d "${APP_PROPS}" ${jobServerURL} $CURL_OPTS ${securePart}
  ;;

  status)
    curl ${jobServerURL} $CURL_OPTS  ${securePart}
  ;;

  listcontexts)
    curl -X GET ${jobServerURL} $CURL_OPTS  ${securePart}
  ;;

  stop | stopcontext)
    curl -X DELETE ${jobServerURL} $CURL_OPTS  ${securePart}
  ;;
esac

echo
