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

# Starts a locator on the machine this script is executed on.
#

usage="Usage: snappy-locator.sh (start|stop|status) -dir=directory"

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

mode=$1
shift

. "$sbin/snappy-config.sh"
. "$sbin/spark-config.sh"

. "$SPARK_PREFIX/bin/load-snappy-env.sh"
. "$SPARK_PREFIX/bin/load-spark-env.sh"


# Start up  the locator instance
function start_instance {
  "$SPARK_PREFIX"/bin/snappy-shell locator "$mode" "$@"
}

start_instance "$@"

