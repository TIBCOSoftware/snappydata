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

BACKGROUND=-bg
clustermode=
recover=
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
    rowstore)
      clustermode="rowstore"
    ;;
    *)
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
"$sbin"/snappy-locators.sh start $clustermode $recover "$@"

# Start Servers
"$sbin"/snappy-servers.sh $BACKGROUND start $clustermode $recover "$@"

# Start Leads
if [ "$clustermode" != "rowstore" ]; then
  "$sbin"/snappy-leads.sh start $recover
fi
