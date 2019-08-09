#!/bin/bash
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


function absPath() {
  perl -MCwd -le 'print Cwd::abs_path(shift)' "$1"
}
sbin="$(dirname "$(absPath "$0")")"

# Load the Spark configuration
. "$sbin/snappy-config.sh"
. "$sbin/spark-config.sh"

SNAPPY_DIR="$SNAPPY_HOME"
echo "SNAPPY_HOME= $SNAPPY_HOME"
usage() {
  # echo "Usage: cluster-util.sh [--snappydir <path-of-snappy-product>] [--run <cmd-to-run-on-all-servers>]"
  echo "Usage: cluster-util.sh [locator|server|lead|all] [--run <cmd-to-run-on-all-servers>]"
  exit 1
}

componentType=$1
shift

echo "Input component type: $componentType"

if [[ "$#" < "2" ]]; then
  usage
fi

while :
do
  case $1 in
    --run)
      RUN="true"
      shift
      ;;
    -*)
      echo "ERROR: Unknown option: $1" >&2
      usage
      ;;
    *) # End of options
      break
      ;;
  esac
done

if [[ ! -d ${SNAPPY_DIR} ]]; then
  echo "${SNAPPY_DIR} does not exist. Exiting ..."
  exit 1
fi

if [[ ! -e $SNAPPY_DIR/conf/servers ]]; then
  echo "${SNAPPY_DIR}/conf/servers does not exist. Exiting ..."
  exit 2
fi

if [[ ! -e $SNAPPY_DIR/conf/leads ]]; then
  echo "${SNAPPY_DIR}/conf/leads does not exist. Exiting ..."
  exit 3
fi

if [[ ! -e $SNAPPY_DIR/conf/locators ]]; then
  echo "${SNAPPY_DIR}/conf/locators does not exist. Exiting ..."
  exit 2
fi

SERVER_LIST=`awk '!/^ *#/ && !/^[[:space:]]*$/ { print$1; }' $SNAPPY_DIR/conf/servers`

LEAD_LIST=`awk '!/^ *#/ && !/^[[:space:]]*$/ { print$1; }' $SNAPPY_DIR/conf/leads`

LOCATOR_LIST=`awk '!/^ *#/ && !/^[[:space:]]*$/ { print$1; }' $SNAPPY_DIR/conf/locators`

MEMBER_LIST=
MEMBER_TYPE=
case $componentType in

  (locator)
    MEMBER_LIST=$LOCATOR_LIST
    MEMBER_TYPE="locator"
    ;;

  (server)
    MEMBER_LIST=$SERVER_LIST
    MEMBER_TYPE="server"
    ;;
  (lead)
    MEMBER_LIST=$LEAD_LIST
    MEMBER_TYPE="lead"
    ;;
  (all)
    MEMBER_LIST="all"
    ;;
esac

function executeCommand() {
  echo "################### Executing $@ on $node ###################"
  read -p "Are you sure to run $@ on $MEMBER_TYPE $node (y/n)?" userinput
  if [[ $userinput == "y" || $userinput == "yes" || $userinput == "Y" || $userinput == "YES" ]]; then
    ssh $node $@
  fi
}

if [[ $RUN = "true" ]]; then
  if [[ $MEMBER_LIST != "all" ]]; then
    for node in $MEMBER_LIST; do
      executeCommand "$@"
    done
  else
    for node in $SERVER_LIST; do
      MEMBER_TYPE="server"
      executeCommand "$@"
    done
    for node in $LEAD_LIST; do
      MEMBER_TYPE="lead"
      executeCommand "$@"
    done
    for node in $LOCATOR_LIST; do
      MEMBER_TYPE="locator"
      executeCommand "$@"
    done
  fi
fi

