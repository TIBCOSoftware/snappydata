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

# Stops all snappy daemons - locator, lead and server on the nodes specified in the
# conf/locators, conf/leads and conf/servers files respectively

function absPath() {
  perl -MCwd -le 'print Cwd::abs_path(shift)' "$1"
}
sbin="$(dirname "$(absPath "$0")")"

# Load the Spark configuration

. "$sbin/snappy-config.sh"
. "$sbin/spark-config.sh"

BACKGROUND=-fg
clustermode=

while (( "$#" )); do
  param="$1"
  case $param in
    # Check for background/foreground stop
    -bg | --background)
      BACKGROUND="$param"
    ;;
    -fg | --foreground)
      BACKGROUND=-fg
    ;;
    rowstore)
      clustermode="rowstore"
    ;;
    *)
    ;;
  esac
  shift
done

# Stop Leads
if [ "$clustermode" != "rowstore" ]; then
  "$sbin"/snappy-leads.sh stop
fi

# Stop Servers
"$sbin"/snappy-servers.sh $BACKGROUND stop

# Stop locators
"$sbin"/snappy-locators.sh stop
