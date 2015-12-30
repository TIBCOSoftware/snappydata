#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Run a shell command on all nodes.
#
# Environment Variables
#
#   SPARK_CONF_DIR  Alternate conf dir. Default is ${SPARK_HOME}/conf.
#   SPARK_SSH_OPTS Options passed to ssh when running remote commands.
##

usage="Usage: snappy-nodes.sh locator/server/lead [--config <conf-dir>] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

. "$sbin/snappy-config.sh"
. "$sbin/spark-config.sh"

componentType=$1
shift

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

. "$SPARK_PREFIX/bin/load-snappy-env.sh"
. "$SPARK_PREFIX/bin/load-spark-env.sh"

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

function startComponent() {
  dirparam="$(echo $args | sed -n 's/^.*\(-dir=[^ ]*\).*$/\1/p')"

  # Set directory folder if not already set.
  if [ -z "${dirparam}" ]; then
    dirfolder="$SPARK_HOME"/work/"$host"-$componentType-$index
    dirparam="-dir=${dirfolder}"
    args="${args} ${dirparam}"
  fi

  # For stop and status mode, don't pass any parameters other than directory
  if echo $"${@// /\\ }" | grep -wq "start"; then
    # Set a default locator if not already set.
    if [ -z "$(echo  $args $"${@// /\\ }" | grep '[-]locators=')" -a "${componentType}" != "locator"  ]; then
      args="${args} -locators="$(hostname)"[10334]"
    fi
    # Set MaxPermSize if not already set.
    if [ -z "$(echo  $args $"${@// /\\ }" | grep 'XX:MaxPermSize=')" -a "${componentType}" != "locator"  ]; then
      args="${args} -J-XX:MaxPermSize=350m"
    fi
  else
    args="${dirparam}"
  fi

  if [ "$dirfolder" != "" ]; then
    # Create the directory for the snappy component if the folder is a default folder
    ssh $SPARK_SSH_OPTS "$host" \
      "if [ ! -d \"$dirfolder\" ]; then  mkdir -p \"$dirfolder\"; fi;" $"${@// /\\ } ${args};" < /dev/null \
      2>&1 | sed "s/^/$host: /"
  else
    # ssh reads from standard input and eats all the remaining lines.Connect its standard input to nowhere:
    ssh $SPARK_SSH_OPTS "$host" $"${@// /\\ } ${args}" < /dev/null \
      2>&1 | sed "s/^/$host: /"
  fi
}

index=1

if [ -n "${HOSTLIST}" ]; then
  while read -r slave; do
    [[ -z "$(echo $slave | grep ^[^#])" ]] && continue
    host="$(echo "$slave "| tr -s ' ' | cut -d ' ' -f1)"
    args="$(echo "$slave "| tr -s ' ' | cut -d ' ' -f2-)"
    startComponent $@
    index=$[index +1]
  done < $HOSTLIST
else
    host="localhost"
    args=""
    startComponent $@
fi
wait

