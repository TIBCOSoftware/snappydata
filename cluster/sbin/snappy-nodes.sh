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

# Run a shell command on all nodes.
#
# Environment Variables
#
#   SPARK_CONF_DIR  Alternate conf dir. Default is ${SNAPPY_HOME}/conf.
#   SPARK_SSH_OPTS Options passed to ssh when running remote commands.
##

usage="Usage: snappy-nodes.sh locator/server/lead [-bg|--background] [--config <conf-dir>] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

function absPath() {
  perl -MCwd -le 'print Cwd::abs_path(shift)' "$1"
}
sbin="$(dirname "$(absPath "$0")")"

. "$sbin/common.funcs"
. "$sbin/snappy-config.sh"
. "$sbin/spark-config.sh"


componentType=$1
shift

# Whether to apply the operation in background
RUN_IN_BACKGROUND=
if [ "$1" = "-bg" -o "$1" = "--background" ]; then
  RUN_IN_BACKGROUND=1
  shift
fi
export RUN_IN_BACKGROUND

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

. "$SNAPPY_HOME/bin/load-spark-env.sh"
. "$SNAPPY_HOME/bin/load-snappy-env.sh"


FIRST_LOCATOR=
case $componentType in

  (locator)
    if [ -f "${SPARK_CONF_DIR}/locators" ]; then
      HOSTLIST="${SPARK_CONF_DIR}/locators"
    fi
    FIRST_LOCATOR=1
    ;;

  (server)
    if [ -f "${SPARK_CONF_DIR}/servers" ]; then
      HOSTLIST="${SPARK_CONF_DIR}/servers"
    fi
    if [ -f "${SPARK_CONF_DIR}/leads" ]; then
      LEADHOSTLIST="${SPARK_CONF_DIR}/leads"
    fi
    ;;
  (lead)
    if [ -f "${SPARK_CONF_DIR}/leads" ]; then
      HOSTLIST="${SPARK_CONF_DIR}/leads"
    fi
    ;;
  (*)
      echo $usage
      exit 1
      ;;
esac
export FIRST_LOCATOR

# By default disable strict host key checking
if [ "$SPARK_SSH_OPTS" = "" ]; then
  SPARK_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

default_loc_port=10334

function readAllLocators() {
  retVal=
  while read loc || [[ -n "${loc}" ]]; do
    [[ -z "$(echo $loc | grep ^[^#] | grep -v ^$ )" ]] && continue
    if [ -n "$(echo $loc | grep peer-discovery-port)" ]; then
      retVal="$retVal,$(echo $loc | sed "s#\([^ ]*\).*peer-discovery-port\s*=\s*\([^ ]*\).*#\1:\2#g")"
    else
      retVal="$retVal,$(echo $loc | sed "s#\([^ ]*\).*#\1:$default_loc_port#g")"
    fi
  done < "${SPARK_CONF_DIR}/locators"
  echo ${retVal#","}
}

LOCATOR_IS_LOCAL=
if [ -f "${SPARK_CONF_DIR}/locators" ]; then
  allLocators="$(readAllLocators)"
  LOCATOR_ARGS="-locators=$allLocators"
  if echo $allLocators | egrep -wq '(localhost|127\.0\.0\.1|::1)'; then
    LOCATOR_IS_LOCAL=1
  fi
else
  LOCATOR_ARGS="-locators=localhost[$default_loc_port]"
  LOCATOR_IS_LOCAL=1
fi

MEMBERS_FILE="$SNAPPY_HOME/work/members.txt"
isStart=

function execute() {
  dirparam="$(echo $args | sed -n 's/^.*\(-dir=[^ ]*\).*$/\1/p')"

  # Set directory folder if not already set.
  if [ -z "${dirparam}" ]; then
    dirfolder="$SNAPPY_HOME"/work/"$host"-$componentType-$index
    dirparam="-dir=${dirfolder}"
    args="${args} ${dirparam}"
  fi

  # For stop and status mode, don't pass any parameters other than directory
  if echo $"${@// /\\ }" | grep -wq "start"; then
    # Set a default locator if not already set.
    if ! echo $args $"${@// /\\ }" | egrep -q '[-](locators=|peer-discovery-address=)'; then
      args="${args} $LOCATOR_ARGS"
      # inject start-locators argument if not present
      if [ "${componentType}" = "locator" -a -z "$(echo  $args $"${@// /\\ }" | grep 'start-locator=')" ]; then
        port=$(echo $args | grep -wo "peer-discovery-port=[^ ]*" | sed 's#peer-discovery-port=##g')
        if [ -z "$port" ]; then
          port=$default_loc_port
        fi
        args="${args} -start-locator=$host:$port"
      fi
    fi
    # Reduce discovery and join timeouts, retries for first locator to reduce self-wait
    if [ -n "$FIRST_LOCATOR" ]; then
      FIRST_LOCATOR=
      if ! echo $args $"${@// /\\ }" | grep -q 'Dp2p.discoveryTimeout='; then
        args="${args} -J-Dp2p.discoveryTimeout=1000"
      fi
      if ! echo $args $"${@// /\\ }" | grep -q 'Dp2p.joinTimeout='; then
        args="${args} -J-Dp2p.joinTimeout=2000"
      fi
      if ! echo $args $"${@// /\\ }" | grep -q 'Dp2p.minJoinTries='; then
        args="${args} -J-Dp2p.minJoinTries=1"
      fi
    fi

    bindAddress=
    clientBindAddress=
    clientHostName=
    clientPort=
    dumpServerInfo=
    for arg in $args $"${@// /\\ }"; do
      case "$arg" in
        -bind-address=*) bindAddress="$(echo $arg | sed 's/-bind-address=//')" ;;
        -client-bind-address=*) clientBindAddress="$(echo $arg | sed 's/-client-bind-address=//')" ;;
        -hostname-for-clients=*) clientHostName="$(echo $arg | sed 's/-hostname-for-clients=//')" ;;
        -client-port=*) clientPort="$(echo $arg | sed 's/-client-port=//')" ;;
        -dump-server-info) dumpServerInfo=1 ;;
      esac
    done
    # set the default bind-address and SPARK_LOCAL_IP
    if [ -z "${bindAddress}" ]; then
      args="${args} -bind-address=$host"
      bindAddress="${host}"
    fi
    preCommand="${preCommand}export SPARK_LOCAL_IP=$bindAddress; "

    # set the default client-bind-address and locator's peer-discovery-address
    if [ -z "${clientBindAddress}" -a "${componentType}" != "lead" ]; then
      args="${args} -client-bind-address=${host}"
      clientBindAddress="${host}"
    fi
    if [ -z "$(echo $args $"${@// /\\ }" | grep 'peer-discovery-address=')" -a "${componentType}" = "locator" ]; then
      args="${args} -peer-discovery-address=${host}"
    fi
    # set the public hostname for Spark Web UI to hostname-for-clients if configured
    if [ -n "${clientHostName}" ]; then
      preCommand="${preCommand}export SPARK_PUBLIC_DNS=${clientHostName}; "
    fi
    # set host-data=false explicitly for leads
    if [ "${componentType}" = "lead" ]; then
      args="${args} -host-data=false"
    fi

    if [ -n "${dumpServerInfo}" ]; then
      if [ -n "${clientHostName}" ]; then
        echo "${clientHostName}:${clientPort}"
      else
        echo "${clientBindAddress}:${clientPort}"
      fi
      exit 0
    fi
  else
    args="${dirparam}"
  fi

  if [ ! -d "${SNAPPY_HOME}/work" ]; then
    mkdir -p "${SNAPPY_HOME}/work"
    ret=$?
    if [ "$ret" != "0" ]; then
      echo "Could not create work directory ${SNAPPY_HOME}/work"
      exit 1
    fi
  fi

  postArgs=
  for arg in "${@// /\\ }"; do
    case "$arg" in
      -*) postArgs="$postArgs $arg"
    esac
  done
  #copy the conf files into other node before starting the launch processs
  if [[ -n "$isStart" && $SKIP_CONF_COPY -eq 0 ]]; then
    copyConf "$@"    
  fi
  if [ "$host" != "localhost" ]; then
    if [ "$dirfolder" != "" ]; then
      # Create the directory for the snappy component if the folder is a default folder
      (ssh $SPARK_SSH_OPTS "$host" \
        "{ if [ ! -d \"$dirfolder\" ]; then  mkdir -p \"$dirfolder\"; fi; } && " $"${preCommand}${@// /\\ } ${args} ${postArgs};" \
        < /dev/null 2>&1 | sed "s/^/$host: /") &
      LAST_PID="$!"
    else
      # ssh reads from standard input and eats all the remaining lines.Connect its standard input to nowhere:
      (ssh $SPARK_SSH_OPTS "$host" $"${preCommand}${@// /\\ } ${args} ${postArgs}" < /dev/null \
        2>&1 | sed "s/^/$host: /") &
      LAST_PID="$!"
    fi
  else
    if [ "$dirfolder" != "" ]; then
      # Create the directory for the snappy component if the folder is a default folder
      if [ ! -d "$dirfolder" ]; then
         mkdir -p "$dirfolder"
      fi
    fi
    launchcommand="${@// /\\ } ${args} ${postArgs} < /dev/null 2>&1"
    eval $launchcommand &
    LAST_PID="$!"
  fi
  if [ -z "$RUN_IN_BACKGROUND" ]; then
    wait $LAST_PID
  else
    sleep 1
    if [ -e "/proc/$LAST_PID/status" ]; then
      sleep 1
    fi
  fi

  childPids+=($LAST_PID)
  df=${dirfolder}
  if [ -z "${df}" ]; then
    df=$(echo ${dirparam} | cut -d'=' -f2)
  fi

  if [ -z "${df}" ]; then
    echo "No run directory identified for ${host}"
    exit 1
  fi

  echo "${host} ${df}" >> $MEMBERS_FILE
}

index=1
isServerStart=
declare -a leadHosts
declare -a leadCounts
declare -a childPids

if [ "$componentType" = "server" -a -n "$(echo $"${@// /\\ }" | grep -w start)" ]; then
  isServerStart=1
fi
# check leads on the same nodes as servers
# (and if none then memory-size can be increased)
if [ -n "$LEADHOSTLIST" -a -n "$isServerStart" ]; then
  while read slave || [[ -n "$slave" ]]; do
    [[ -z "$(echo $slave | grep ^[^#])" ]] && continue
    host="$(echo "$slave "| tr -s ' ' | cut -d ' ' -f1)"
    args="$(echo "$slave "| tr -s ' ' | cut -d ' ' -f2-)"
    leadIndex=$(keyIndex "$host" "${leadHosts[@]}")
    leadPutIndex="$leadIndex"
    if [ -z "$leadPutIndex" ]; then
      leadPutIndex=${#leadCounts[@]}
    fi
    leadHosts[$leadPutIndex]="$host"
    # marker for the case when lead heap/memory has been configured explicitly
    # in which case server side auto-configuration will also be skipped
    if echo $args $"${@// /\\ }" | grep -q "heap-size=\|memory-size="; then
      leadCounts[$leadPutIndex]=-1
    elif [ -z "$leadIndex" ]; then
      leadCounts[$leadPutIndex]=1
    else
      ((leadCounts[$leadPutIndex]++))
    fi
  done < "$LEADHOSTLIST"
fi

function getNumLeadsOnHost() {
  host="$1"
  numLeadsOnHost=
  if [ ${#leadCounts[@]} -gt 0 ]; then
    leadIndex=$(keyIndex "$host" "${leadHosts[@]}")
    if [ -n "$leadIndex" ]; then
      numLeadsOnHost="${leadCounts[$leadIndex]}"
    fi
  elif [ "$host" = "localhost" ]; then
    numLeadsOnHost=1
  fi
  if [ -z "$numLeadsOnHost" ]; then
    numLeadsOnHost=0
  fi
  echo $numLeadsOnHost
}
# function for copying all the configuration files into the other nodes/members of the cluster
function copyConf() {
  currentNodeIpAddr=$(ip addr | grep 'state UP' -A2 | head -n3 | tail -n1 | awk '{print $2}' | cut -f1  -d'/')
  currentNodeHostName=$(uname -n)

  if [[ "$host" != "$currentNodeIpAddr" && "$host" != "localhost" && "$host" != $currentNodeHostName ]] ; then
  #loop to get the all the files avaliable in Conf directory
    for entry in "${SPARK_CONF_DIR}"/*; do
      if [ -f "$entry" ];then
        #${file%.*} to get the filename without the extension and ${file##*.} to get the extension alone
	fileName=$(basename $entry)
 	template=".template"
	#check the extension, interested in files those doesn't have template extension
	if [[ ! "$fileName" = @(*.template) ]]; then	       	
	  if ! ssh $host "test -e $entry"; then #"File does not exist."	  
            scp ${SPARK_CONF_DIR}/$fileName  $host:${SPARK_CONF_DIR}
	  else
	      backupDir="backup"
	      if [[ ! -z $(ssh $host "cat $entry" | diff - "$entry") ]] ; then
		backupFileName=${fileName}_${START_ALL_TIMESTAMP}
		echo "INFO: Copied $filename from this host to $host. Moved the original $filename on $host to $backupFileName."
                (ssh "$host" "{ if [ ! -d \"${SPARK_CONF_DIR}/$backupDir\" ]; then  mkdir \"${SPARK_CONF_DIR}/$backupDir\"; fi; } ")
		ssh $host "mv ${SPARK_CONF_DIR}/$fileName ${SPARK_CONF_DIR}/$backupDir/$backupFileName"		    
		scp ${SPARK_CONF_DIR}/$fileName  $host:${SPARK_CONF_DIR} 
	      fi  
            #fi												
	  fi        				
	fi # end of if, check extension
      fi # end of if to get each file
    done  #end of for loop
  fi # end of if 	
}

if [ -n "${HOSTLIST}" ]; then
  declare -a arr
  declare -a hosts
  declare -a counts
  isStartOrStatus=
  while read slave || [[ -n "${slave}" ]]; do
    [[ -z "$(echo $slave | grep ^[^#])" ]] && continue
    arr[${#arr[@]}]="$slave"
    if [ -n "$isServerStart" ]; then
      host="$(echo "$slave "| tr -s ' ' | cut -d ' ' -f1)"
      hostIndex=$(keyIndex "$host" "${hosts[@]}")
      if [ -z "$hostIndex" ]; then
        hostIndex=${#hosts[@]}
        counts[$hostIndex]=1
      else
        ((counts[$hostIndex]++))
      fi
      hosts[$hostIndex]="$host"
    fi
  done < "$HOSTLIST"

  numSlaves=${#arr[@]}
  if [ $numSlaves -eq 0 ]; then
    arr[0]=localhost
    hosts[0]=localhost
    counts[0]=1
    numSlaves=1
  fi

  if echo $"${@// /\\ }" | grep -wq "start\|status"; then
    isStartOrStatus=1
  fi
  if echo $"${@// /\\ }" | grep -wq "start"; then
    isStart=1
  fi
  for slave in "${arr[@]}"; do
    if [ -n "$isStartOrStatus" ]; then
      host="$(echo "$slave "| tr -s ' ' | cut -d ' ' -f1)"
      args="$(echo "$slave "| tr -s ' ' | cut -d ' ' -f2-)"
      # disable implicit off-heap for nodes having multiple servers configured
      if [ -n "$isServerStart" ]; then
        hostIndex=$(keyIndex "$host" "${hosts[@]}")
        if [ -n "$hostIndex" -a ${counts[$hostIndex]} -gt 1 -a -z "$(echo $args $"${@// /\\ }" | grep 'memory-size=')" ]; then
          args="$args -memory-size=0"
        fi
        # check number of leads on the same node
        args="$args -J-Dsnappydata.numLeadsOnHost=$(getNumLeadsOnHost "$host")"
      fi
      execute "$@"
    fi
    ((index++))
  done

  # stop nodes in reverse order
  if echo $"${@// /\\ }" | grep -wq "stop"; then
    line=$numSlaves
    if [ $((index-1)) -eq $line ]; then
      for (( i=$numSlaves-1 ; i>=0 ; i-- )) ; do
        ((index--))
        CONF_ARG=${arr[$i]}
        host="$(echo "$CONF_ARG "| tr -s ' ' | cut -d ' ' -f1)"
        args="$(echo "$CONF_ARG "| tr -s ' ' | cut -d ' ' -f2-)"
        execute "$@"
      done
    fi
  fi
else
  host="localhost"
  args=""
  if [ -n "$isServerStart" ]; then
    args="$args -J-Dsnappydata.numLeadsOnHost=$(getNumLeadsOnHost "$host")"
  fi
  execute "$@"
fi

if [ "$isServerStart" ]; then
  # server status file
  SERVERS_STATUS_FILE="$SNAPPY_HOME/work/serversstatus"
  if [ -f $SERVERS_STATUS_FILE ]; then
    rm $SERVERS_STATUS_FILE
  fi
  touch $SERVERS_STATUS_FILE
  for pid in "${childPids[@]}"; do
    wait $pid
    echo $? >> $SERVERS_STATUS_FILE
  done
else
  wait
fi
