#!/usr/bin/env bash

#
# Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

# Run a shell command on all nodes.
#
# Environment Variables
#
#   SPARK_CONF_DIR  Alternate conf dir. Default is ${SNAPPY_HOME}/conf.
#   SPARK_SSH_OPTS Options passed to ssh when running remote commands.
##

usage="Usage: snappy-nodes.sh locator/server/lead [-bg|--background] [--config <conf-dir>] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

. "$sbin/spark-config.sh"
. "$sbin/snappy-config.sh"


componentType=$1
shift

# Whether to apply the operation in background
RUN_IN_BACKGROUND=0
if [ "$1" = "-bg" -o "$1" = "--background" ]; then
  RUN_IN_BACKGROUND=1
  shift
fi
export RUN_IN_BACKGROUND
  
# Check if --config is passed as an argument. It is an optional parameter.
# Exit if the argument is not a directory.
if [ "$1" == "--config" ]
then
  shift
  conf_dir="$1"
  if [ ! -d "$conf_dir" ]
  then
    echo "ERROR : $conf_dir is not a directory"
    echo $usage
    exit 1
  else
    export SPARK_CONF_DIR="$conf_dir"
  fi
  shift
fi

. "$SNAPPY_HOME/bin/load-spark-env.sh"
. "$SNAPPY_HOME/bin/load-snappy-env.sh"


case $componentType in

  (locator)
    if [ -f "${SPARK_CONF_DIR}/locators" ]; then
      HOSTLIST="${SPARK_CONF_DIR}/locators"
    fi
    ;;

  (server)
    if [ -f "${SPARK_CONF_DIR}/servers" ]; then
      HOSTLIST="${SPARK_CONF_DIR}/servers"
    fi
    ;;
  (lead)
    if [ -f "${SPARK_CONF_DIR}/leads" ]; then
      HOSTLIST="${SPARK_CONF_DIR}/leads"
    fi
    ;;
  (*)
      echo $usage
      exit 1
      ;;
esac
# By default disable strict host key checking
if [ "$SPARK_SSH_OPTS" = "" ]; then
  SPARK_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

default_loc_port=10334

function readalllocators { 
  retVal=
  while read loc || [[ -n "${loc}" ]]; do
    [[ -z "$(echo $loc | grep ^[^#] | grep -v ^$ )"  ]] && continue
    if [ -n "$(echo $loc | grep peer-discovery-port)" ]; then
      retVal="$retVal,$(echo $loc | sed "s#\([^ ]*\).*peer-discovery-port\s*=\s*\([^ ]*\).*#\1:\2#g")"
    else
      retVal="$retVal,$(echo $loc | sed "s#\([^ ]*\).*#\1:$default_loc_port#g")"
    fi
  done < "${SPARK_CONF_DIR}/locators" 
  echo ${retVal#","}
}

if [ -f "${SPARK_CONF_DIR}/locators" ]; then
  LOCATOR_ARGS="-locators="$(readalllocators)
else
  LOCATOR_ARGS="-locators=localhost[$default_loc_port]"
fi

MEMBERS_FILE="$SNAPPY_HOME/work/members.txt"

FIRST_NODE=1
export FIRST_NODE
function execute() {
  dirparam="$(echo $args | sed -n 's/^.*\(-dir=[^ ]*\).*$/\1/p')"

  # Set directory folder if not already set.
  if [ -z "${dirparam}" ]; then
    dirfolder="$SNAPPY_HOME"/work/"$host"-$componentType-$index
    dirparam="-dir=${dirfolder}"
    args="${args} ${dirparam}"
  fi

  # For stop and status mode, don't pass any parameters other than directory
  if echo $"${@// /\\ }" | grep -wq "start"; then
    # Set a default locator if not already set.
    if [ -z "$(echo  $args $"${@// /\\ }" | grep '[-]locators=')" ]; then
      args="${args} $LOCATOR_ARGS"
      # inject start-locators argument if not present
      if [[ "${componentType}" == "locator" && -z "$(echo  $args $"${@// /\\ }" | grep 'start-locator=')" ]]; then
        port=$(echo $args | grep -wo "peer-discovery-port=[^ ]*" | sed 's#peer-discovery-port=##g')
        if [ -z "$port" ]; then
          port=$default_loc_port
        fi
        args="${args} -start-locator=$host:$port"
      fi
      # Set low discovery and join timeouts for quick startup when locator is local.
      if [ -z "$(echo  $args $"${@// /\\ }" | grep 'Dp2p.discoveryTimeout=')" ]; then
        args="${args} -J-Dp2p.discoveryTimeout=1000"
      fi
      if [ -z "$(echo  $args $"${@// /\\ }" | grep 'Dp2p.joinTimeout=')" ]; then
        args="${args} -J-Dp2p.joinTimeout=2000"
      fi
    fi
    if [ -z "$(echo  $args $"${@// /\\ }" | grep 'client-bind-address=')" -a "${componentType}" != "lead"  ]; then
      args="${args} -client-bind-address=${host}"
    fi
    if [ -z "$(echo  $args $"${@// /\\ }" | grep 'peer-discovery-address=')" -a "${componentType}" == "locator"  ]; then
      args="${args} -peer-discovery-address=${host}"
    fi
  else
    args="${dirparam}"
  fi

  if [ ! -d "${SNAPPY_HOME}/work" ]; then
    mkdir -p "${SNAPPY_HOME}/work"
    ret=$?
    if [ "$ret" != "0" ]; then
      echo "Could not create work directory ${SNAPPY_HOME}/work"
      exit 1
    fi
  fi

  if [ "$host" != "localhost" ]; then
    if [ "$dirfolder" != "" ]; then
      # Create the directory for the snappy component if the folder is a default folder
      (ssh $SPARK_SSH_OPTS "$host" \
        "{ if [ ! -d \"$dirfolder\" ]; then  mkdir -p \"$dirfolder\"; fi; } && " $"${@// /\\ } ${args};" < /dev/null \
        2>&1 | sed "s/^/$host: /") &
      LAST_PID="$!"
    else
      # ssh reads from standard input and eats all the remaining lines.Connect its standard input to nowhere:
      (ssh $SPARK_SSH_OPTS "$host" $"${@// /\\ } ${args}" < /dev/null \
        2>&1 | sed "s/^/$host: /") &
      LAST_PID="$!"
    fi
    if [ "${RUN_IN_BACKGROUND}" = "0" -o "${FIRST_NODE}" = "1" ]; then
      if wait $LAST_PID; then
        FIRST_NODE=0
      fi
    else
      sleep 3
    fi
  else
    if [ "$dirfolder" != "" ]; then
      # Create the directory for the snappy component if the folder is a default folder
      if [ ! -d "$dirfolder" ]; then
         mkdir -p "$dirfolder"
      fi
    fi
    launchcommand="${@// /\\ } ${args} < /dev/null 2>&1"
    eval $launchcommand
  fi

  df=${dirfolder}
  if [ -z "${df}" ]; then
    df=$(echo ${dirparam} | cut -d'=' -f2)
  fi

  if [ -z "${df}" ]; then
    echo "No run directory identified for ${host}"
    exit 1
  fi

  echo "${host} ${df}" >> $MEMBERS_FILE
}

index=1
declare -a arr
if [ -n "${HOSTLIST}" ]; then
  while read slave || [[ -n "${slave}" ]]; do
    [[ -z "$(echo $slave | grep ^[^#])" ]] && continue
    arr+=("${slave}");
    host="$(echo "$slave "| tr -s ' ' | cut -d ' ' -f1)"
    args="$(echo "$slave "| tr -s ' ' | cut -d ' ' -f2-)"
    if echo $"${@// /\\ }" | grep -wq "start\|status"; then
      execute "$@"
    fi
    ((index++))
  done < $HOSTLIST
  if echo $"${@// /\\ }" | grep -wq "stop"; then
    line=${#arr[@]}
    if [ $((index-1)) -eq $line ]; then
      for (( i=${#arr[@]}-1 ; i>=0 ; i-- )) ; do
        ((index--))
        CONF_ARG=${arr[$i]}
        host="$(echo "$CONF_ARG "| tr -s ' ' | cut -d ' ' -f1)"
        args="$(echo "$CONF_ARG "| tr -s ' ' | cut -d ' ' -f2-)"
        execute "$@"
      done
    fi
  fi
else
  host="localhost"
  args=""
  execute "$@"
fi
wait
