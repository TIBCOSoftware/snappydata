#!/usr/bin/env bash

#
# Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
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

# Start all snappy daemons - locator, lead and server on the nodes specified in the
# conf/locators, conf/leads and conf/servers files respectively

function absPath() {
  perl -MCwd -le 'print Cwd::abs_path(shift)' "$1"
}
sbin="$(dirname "$(absPath "$0")")"

# Load the Spark configuration
. "$sbin/snappy-config.sh"
. "$sbin/spark-config.sh"

MEMBERS_FILE="$SNAPPY_HOME/work/members.txt"
if [ -f "${MEMBERS_FILE}" ]; then
  rm $MEMBERS_FILE
fi

SERVERS_STATUS_FILE="$SNAPPY_HOME/work/members-status.txt"
if [ -f "${SERVERS_STATUS_FILE}" ]; then
  rm $SERVERS_STATUS_FILE
fi

CAN_START_LEAD=0
function checkIfOkayToStartLeads() {
  while  read -r line || [[ -n "$line" ]]; do
    read exitstatus member <<< $line
    if [ "x$exitstatus" = "x0" -o "x$exitstatus" = "x10" ]; then
      CAN_START_LEAD=1
    fi
  done < $SERVERS_STATUS_FILE
}

BACKGROUND=-bg
clustermode=
recover=
CONF_DIR_ARG=

while (( "$#" )); do
  param="$1"
  case $param in
    # Check for background/foreground start
    -bg | --background)
      BACKGROUND=-bg
    ;;
    -fg | --foreground)
      BACKGROUND=-fg
    ;;
    -r  | --recover)
      recover="-recover"
    ;;
    -conf | --config)
      conf_dir="$2"
      if [ ! -d $conf_dir ] ; then
        echo "Conf directory $conf_dir does not exist"
        exit 1
      fi
      CONF_DIR_ARG="--config $conf_dir"
      shift ;;
    rowstore)
      clustermode="rowstore"
    ;;
    *)
    echo "Invalid argument: $1"
    echo "Usage: ./snappy-start-all.sh [-bg | --background | -fg | --foreground] [-conf conf_dir | --config conf_dir] [rowstore]"
    exit
    ;;
  esac
  shift
done

if [ ! -z "$clustermode" -a ! -z "$recover" ] ; then
  echo "recovery is not supported for rowstore mode"
  exit 1
fi


# TODO: Why is "$@" there. The args are parsed and shifted above making $@ empty. Isn't it?
# Start Locators
"$sbin"/snappy-locators.sh $CONF_DIR_ARG start $clustermode $recover "$@"

# Start Servers
"$sbin"/snappy-servers.sh $BACKGROUND $CONF_DIR_ARG start $clustermode $recover "$@"

# Start Leads
if [ "$clustermode" != "rowstore" ]; then
  checkIfOkayToStartLeads
  if [ "$CAN_START_LEAD" = "1" ]; then
    "$sbin"/snappy-leads.sh $CONF_DIR_ARG start $recover
  else
    echo "Cannot start lead components. At least one server should be"
    echo "in running state."
    echo "Check work/members-status.txt for server status code and try"
    echo "running snappy-status-all script ..."
    echo "Try running lead with snappy-start-all once again"
  fi
fi
