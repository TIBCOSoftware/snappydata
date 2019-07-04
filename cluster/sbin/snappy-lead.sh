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

# Starts a lead on the machine this script is executed on.
#

usage="Usage: snappy-lead.sh (start|stop|status) -locators=locatorhost:port[,locatorhostN:portN] -dir=directory"

function absPath() {
  perl -MCwd -le 'print Cwd::abs_path(shift)' "$1"
}
sbin="$(dirname "$(absPath "$0")")"

mode=$1

shift

. "$sbin/snappy-config.sh" lead
. "$sbin/spark-config.sh"

. "$SNAPPY_HOME/bin/load-spark-env.sh"
. "$SNAPPY_HOME/bin/load-snappy-env.sh"

# Start up  the lead instance
function start_instance {
  "$SNAPPY_HOME"/bin/snappy leader "$mode" "$@"
}

#Since want to test whether the result is zero, don't need to treat it as an return value using $? . Just treat the command itself as a conditional.
if "$sbin/check-dir-option.sh" "$@" ; then	
   start_instance "$@"
else
  echo $usage
fi

