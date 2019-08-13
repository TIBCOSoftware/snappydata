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
bold=$(tput bold)
normal=$(tput sgr0)
space=
usage() {
  echo "${bold}Usage: ${normal}cluster-util.sh -on-locators|-on-servers|-on-leads|-on-all [-y] --run  -copyconf | <cmd-to-run-on-selected-nodes>"
  echo
  echo "${bold}Discription${normal}"
  echo
  echo -e ' \t '"This is an experimental utility to execute a given command on selected members of the cluster."
  echo -e ' \t '"The script relies on the entries you specify in locators, servers and leads files in conf directory to identify the members of the cluster."
  echo 
  echo -e ' \t '"-on-locators|-on-servers|-on-leads|-on-all"   
  echo -e ' \t ''\t'"Indicates which members of the cluster the given command would be executed on."
  echo
  echo -e ' \t '"-y"
  echo -e ' \t ''\t'"If specified, the script doesn't ask for confirmation for execution of the command on each member node."
  echo
  echo -e ' \t '"-copyconf"
  echo -e ' \t ''\t'"This is a shortcut command with --run option which when specified copies log4j.properties, snappy-env.sh and "
  echo -e ' \t ''\t'"spark-env.sh configuration files from local machine to all the members."
  echo -e ' \t ''\t'"These files are copied only if a) these are absent in the destination member or b) their content is different. In "
  echo -e ' \t ''\t'"latter case, a backup of the file is taken in conf/backup directory on destination member, before copy."
  echo -e ' \t '"<cmd-to-run-on-selected-nodes>"
  echo -e ' \t ''\t'"Command"
  echo
  exit 1
}

COMPONENT_TYPE=$1
shift

# Whether to apply the operation forcefully.
ISFORCE=0
if [ "$1" = "-y" ]; then
  ISFORCE=1
  shift
fi

if [[ "$#" < "2" ]]; then
  usage
fi

while :
do
  case $1 in
    --run)
      RUN="true"
      shift
      break
      ;;
    -*)
      echo "ERROR: Unknown option: $1" >&2
      echo
      usage
      ;;
    *) # End of options
      break
      ;;
  esac
done

#whether user wants to perform copy configuration files operation only
COPY_CONF=0
SPARK_CONF_DIR=
if [ "$1" = "-copyconf" ]; then
  COPY_CONF=1
  SPARK_CONF_DIR=$SNAPPY_HOME/conf/
  shift
fi

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

SERVER_LIST=$(sed ':loop /^[^#].*[^\\]\\$/N; s/\\\n//; t loop' $SNAPPY_DIR/conf/servers | awk '!/^ *#/ && !/^[[:space:]]*$/ { print$1; }')

LEAD_LIST=$(sed ':loop /^[^#].*[^\\]\\$/N; s/\\\n//; t loop' $SNAPPY_DIR/conf/leads | awk '!/^ *#/ && !/^[[:space:]]*$/ { print$1; }')

LOCATOR_LIST=$(sed ':loop /^[^#].*[^\\]\\$/N; s/\\\n//; t loop' $SNAPPY_DIR/conf/locators | awk '!/^ *#/ && !/^[[:space:]]*$/ { print$1; }')

MEMBER_LIST=
MEMBER_TYPE=
case $COMPONENT_TYPE in

  (-on-locators)
    MEMBER_LIST=$LOCATOR_LIST
    MEMBER_TYPE="locator"
    ;;

  (-on-servers)
    MEMBER_LIST=$SERVER_LIST
    MEMBER_TYPE="server"
    ;;
  (-on-leads)
    MEMBER_LIST=$LEAD_LIST
    MEMBER_TYPE="lead"
    ;;
  (-on-all)
    MEMBER_LIST="all"
    MEMBER_TYPE="all"
    ;;
esac


START_ALL_TIMESTAMP="$(date +"%Y_%m_%d_%H_%M_%S")"

function copyConf() { 
  for entry in "${SPARK_CONF_DIR}"/*; do
      if [ -f "$entry" ];then
	fileName=$(basename $entry)
        if [[ $fileName == "log4j.properties" || $fileName == "snappy-env.sh" || $fileName == "spark-env.sh" ]];then 	       	
	  if ! ssh $node "test -e $entry"; then #"File does not exist."	  
	    scp ${SPARK_CONF_DIR}/$fileName  $node:${SPARK_CONF_DIR}
	  else
	    backupDir="backup"
	    if [[ ! -z $(ssh $node "cat $entry" | diff - "$entry") ]] ; then
	      backupFileName=${fileName}_${START_ALL_TIMESTAMP}
	      (ssh "$node" "mkdir -p \"${SPARK_CONF_DIR}/$backupDir\" ")
	      ssh $node "mv ${SPARK_CONF_DIR}/$fileName ${SPARK_CONF_DIR}/$backupDir/$backupFileName"		    
	      scp ${SPARK_CONF_DIR}/$fileName  $node:${SPARK_CONF_DIR} 
              echo "INFO: Copying $filename from this host to $node. Moved the original $filename on $node to $backupFileName."
	    fi
	  fi
	fi # end of if, check the conf file name      					
      fi # end of if to get each file
  done  #end of for loop
}

function executeCommand() {
  echo
  echo "--------- Executing $@ on $MEMBER_TYPE $node ----------"
  echo 
  if [[ $ISFORCE -eq 0 ]];then
    read -p "Are you sure to run $@ on $MEMBER_TYPE $node (y/n)?" userinput
    echo 
    if [[ $userinput == "y" || $userinput == "yes" || $userinput == "Y" || $userinput == "YES" ]]; then
      if [[ $COPY_CONF = 1 ]]; then
        copyConf "$@"
      else
        ssh $node "$@ | column"
      fi     
    fi
  else
    if [[ $COPY_CONF = 1 ]]; then
      copyConf "$@"
    else
      ssh $node "$@ | column"
    fi  
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

