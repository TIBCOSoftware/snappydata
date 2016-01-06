#!/usr/bin/env bash
#set -vx 

usage=$'Usage: 
       snappy-job.sh newcontext <context-name> --factory <factory class name> [--app-jar <jar-path> --app-name <app-name>]
       snappy-job.sh submit --lead <hostname:port> --app-name <app-name> --class <job-class> [--app-jar <jar-path>] [--context <context-name> | --stream]
       snappy-job.sh status --lead <hostname:port> --job-id <job-id>'

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

while (( "$#" )); do
  param="$1"
  case $param in
    submit)
      cmd="jobs"
    ;;
    status)
      cmd="status"
    ;;
    newcontext)
      cmd="context"
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
    --stream)
      if [[ $contextName != "" || $cmd != "jobs" ]]; then
        showUsage "--context ${contextName} AND --stream"
      fi
      newContext="yes"
      contextName="snappyStreamingContext"$(date +%s%N)
      contextFactory="org.apache.spark.sql.streaming.SnappyStreamingContextFactory"
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
    elif validateOptionalArg $contextName ; then
      showUsage "--context"
    fi
    cmdLine="jobs?appName=${appName}&classPath=${jobClass}"

    if [[ -n $contextName ]]; then
      cmdLine="${cmdLine}&context=${contextName}"
    fi
  ;;

  context)
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
  ;;

  *)
    showUsage
esac
}

if [[ $cmd == "jobs" && -z $newContext && -z $contextName ]]; then
  contextName="snappyContext"$(date +%s%N)
  contextFactory="org.apache.spark.sql.SnappyContextFactory"
  newContext="yes"
fi

buildCommand

# build command for new context, if needed.
if [[ -n $newContext ]]; then
  cmd="context"
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
case $cmd in 
  jobs | context)
    if [[ $appjar != "" ]]; then
      curl --data-binary @$appjar $hostnamePort\/jars\/$appName $CURL_OPTS
    fi

    if [[ $newContext != "" ]]; then
      curl -d "${APP_PROPS}" ${hostnamePort}/${newContext} $CURL_OPTS
    fi

    curl -d "${APP_PROPS}" ${jobServerURL} $CURL_OPTS
  ;;

  status)
    curl ${jobServerURL} $CURL_OPTS
  ;;
esac

