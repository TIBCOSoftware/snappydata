#!/usr/bin/env bash
#set -vx 

usage=$'Usage: 
       snappy-job.sh submit --lead <hostname:port> --app-name <app-name> --class <job-class> [--app-jar <jar-path>] [--context <context-name>]
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
    --context)
      shift
      contextName="${1:-$TOK_EMPTY}"
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

  *)
    showUsage
esac

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

    curl -d "${APP_PROPS}" ${jobServerURL} $CURL_OPTS
  ;;

  status)
    curl ${jobServerURL} $CURL_OPTS
  ;;
esac

