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
#   SNAPPY_LOCATORS/SNAPPY_LEADS/SNAPPY_SERVERS File naming remote hosts.
#     Default is ${SPARK_CONF_DIR}/nodes.
#   SPARK_CONF_DIR  Alternate conf dir. Default is ${SPARK_HOME}/conf.
#   SPARK_SLAVE_SLEEP Seconds to sleep between spawning remote commands.
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

# If the slaves file is specified in the command line,
# then it takes precedence over the definition in
# snappy-env.sh. Save it here.
componentType=$1
case $1 in
  (locator)
    if [ -f "$SNAPPY_LOCATORS" ]; then
      HOSTLIST=`cat "$SNAPPY_LOCATORS"`
    fi
    nodeType=1
    ;;

  (server)
    if [ -f "$SNAPPY_SERVERS" ]; then
      HOSTLIST=`cat "$SNAPPY_SERVERS"`
    fi
    nodeType=2
    ;;
  (lead)
    if [ -f "$SNAPPY_LEADS" ]; then
      HOSTLIST=`cat "$SNAPPY_LEADS"`
    fi
    nodeType=3
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac
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

if [ "$HOSTLIST" = "" ]; then
  case $nodeType in

    (1)
    if [ "$SNAPPY_LOCATORS" = "" ]; then
      if [ -f "${SPARK_CONF_DIR}/locators" ]; then
        HOSTLIST=`cat "${SPARK_CONF_DIR}/locators"`
      else
        HOSTLIST=localhost
      fi
    else
      HOSTLIST=`cat "${SNAPPY_LOCATORS}"`
    fi
    ;;

    (2)
    if [ "$SNAPPY_SERVERS" = "" ]; then
      if [ -f "${SPARK_CONF_DIR}/servers" ]; then
        HOSTLIST=`cat "${SPARK_CONF_DIR}/servers"`
      else
        # Start two servers by default.
        HOSTLIST=$'localhost\nlocalhost'
      fi
    else
      HOSTLIST=`cat "${SNAPPY_SERVERS}"`
    fi
    ;;
    (3)
    if [ "$SNAPPY_LEADS" = "" ]; then
      if [ -f "${SPARK_CONF_DIR}/leads" ]; then
        HOSTLIST=`cat "${SPARK_CONF_DIR}/leads"`
      else
        HOSTLIST=localhost
      fi
    else
      HOSTLIST=`cat "${SNAPPY_LEADS}"`
    fi
    ;;

    (*)
      echo $usage
      exit 1
      ;;

  esac
fi


# By default disable strict host key checking
if [ "$SPARK_SSH_OPTS" = "" ]; then
  SPARK_SSH_OPTS="-o StrictHostKeyChecking=no"
fi
IFS=$'\n'
index=1

for slave in `echo "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do

  host="$(echo "$slave "| tr -s ' ' | cut -d ' ' -f1)"
  args="$(echo "$slave "| tr -s ' ' | cut -d ' ' -f2-)"
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

  index=$[index +1]
  if [ -n "${SPARK_SSH_FOREGROUND}" ]; then
    if [ "$dirfolder" != "" ]; then
      # Create the directory for the snappy component if the folder is a default folder
      ssh $SPARK_SSH_OPTS "$host" \
        "if [ ! -d \"$dirfolder\" ]; then  mkdir -p \"$dirfolder\"; fi;" $"${@// /\\ } ${args};" \
        2>&1 | sed "s/^/$host: /"
    else
      ssh $SPARK_SSH_OPTS "$host" $"${@// /\\ } ${args}" \
        2>&1 | sed "s/^/$host: /"
    fi
  else
    if [ "$dirfolder" != "" ]; then
      # Create the directory for the snappy component if the folder is a default folder
      ssh $SPARK_SSH_OPTS "$host" \
        "if [ ! -d \"$dirfolder\" ]; then  mkdir -p \"$dirfolder\"; fi;" $"${@// /\\ } ${args};" \
        2>&1 | sed "s/^/$host: /"
    else
      ssh $SPARK_SSH_OPTS "$host" $"${@// /\\ } ${args}" \
        2>&1 | sed "s/^/$host: /" &
    fi
  fi

  if [ "$SPARK_SLAVE_SLEEP" != "" ]; then
    sleep $SPARK_SLAVE_SLEEP
  fi
done

wait
