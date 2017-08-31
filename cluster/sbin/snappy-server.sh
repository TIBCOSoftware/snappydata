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

# Starts a server on the machine this script is executed on.
#

usage="Usage: snappy-server.sh (start|stop|status) -locators=locatorhost:port[,locatorhostN:portN] -dir=directory"

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

mode=$1
shift

. "$sbin/spark-config.sh"
. "$sbin/snappy-config.sh"

. "$SPARK_HOME/bin/load-spark-env.sh"
. "$SPARK_HOME/bin/load-snappy-env.sh"



# Start up  the server instance
function start_instance {
  "$SPARK_HOME"/bin/snappy server "$mode" "$@"
}

start_instance "$@"
