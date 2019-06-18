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

# Starts a locator on the machine this script is executed on.
#

usage="Usage: snappy-locator.sh (start|stop|status) -dir=directory"

function absPath() {
  perl -MCwd -le 'print Cwd::abs_path(shift)' "$1"
}
sbin="$(dirname "$(absPath "$0")")"

mode=$1
dir=
shift

. "$sbin/snappy-config.sh"
. "$sbin/spark-config.sh"


. "$SNAPPY_HOME/bin/load-spark-env.sh"
. "$SNAPPY_HOME/bin/load-snappy-env.sh"

noOfInputsArgs=$#

# Start up  the locator instance
function start_instance {
  "$SNAPPY_HOME"/bin/snappy locator "$mode" "$@"
}

if [ $noOfInputsArgs -le 1 ]
then
  if [ $noOfInputsArgs -eq 0 ]
  then  #if no arguments passed
   echo "Please provide -dir argument"
  elif [[ "$1" = -dir=* && -n $(echo $1 | cut -d'=' -f 2) ]] #check -dir is not empty  
  then  
   start_instance "$1"
  else #agrument is given,but not -dir.
   echo "Invalid argument"
  fi
else # when start by snappy-start-all.sh
start_instance "$@"
fi

