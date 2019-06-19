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

# Starts a server on the machine this script is executed on.
#

usage="Usage: snappy-server.sh (start|stop|status) -locators=locatorhost:port[,locatorhostN:portN] -dir=directory"

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

# Start up  the server instance
function start_instance {
  "$SNAPPY_HOME"/bin/snappy server "$mode" "$@"
}

if [ $noOfInputsArgs -le 1 ]
then
  if [ $noOfInputsArgs -eq 0 ] #if no arguments passed
  then 
   hostIp=$(ip addr | grep 'state UP' -A2 | tail -n1 | awk '{print $2}' | cut -f1  -d'/')
   dirfolder="$SNAPPY_HOME"/work/"$hostIp"-server-1
   if [ ! -d "$dirfolder" ]
   then  
	if [ ! -d "$SNAPPY_HOME/work" ]; then 
	 mkdir work 
	fi
	mkdir $dirfolder
   fi
   dir="-dir=${dirfolder}"
   start_instance "$dir"
  elif [[ "$1" = -dir=* && -n $(echo $1 | cut -d'=' -f 2) ]] #check -dir is not empty or valid 
  then  
     if [ ! -d $(echo $1 | cut -d'=' -f 2) ]
     then
     	echo "ERROR : $1 is not a directory"
	echo $usage
    	exit 1
     fi
   start_instance "$1"
  else #agrument is given,but not -dir.
   echo "Invalid argument"
   echo $usage
   exit 1
  fi
else # when start by snappy-start-all.sh
start_instance "$@"
fi

