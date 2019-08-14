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

if [[ ! -d ${SNAPPY_HOME} ]]; then
  echo "${SNAPPY_HOME} does not exist. Exiting ..."
  exit 1
fi

bold=$(tput bold)
normal=$(tput sgr0)
space=
usage() {
  echo "${bold}Usage: ${normal}cluster-util.sh --on-locators|--on-servers|--on-leads|--on-all [-y] --copy-conf | --run '<cmd-to-run-on-selected-nodes>'"
  echo
  echo "${bold}Description${normal}"
  echo
  echo -e ' \t '"This is an utility to execute a given command on selected members of the cluster."
  echo -e ' \t '"The script relies on the entries you specify in locators, servers and leads files in conf directory to identify the members of the cluster."
  echo 
  echo -e ' \t '"--on-locators"   
  echo -e ' \t ''\t'"Indicates the given command would be executed on locators."
  echo
  echo 
  echo -e ' \t '"--on-servers"   
  echo -e ' \t ''\t'"Indicates the given command would be executed on servers."
  echo
  echo 
  echo -e ' \t '"--on-leads"   
  echo -e ' \t ''\t'"Indicates the given command would be executed on leads."
  echo
  echo 
  echo -e ' \t '"--on-all"   
  echo -e ' \t ''\t'"Indicates the given command would be executed on all members of cluster."
  echo
  echo -e ' \t '"-y"
  echo -e ' \t ''\t'"If specified, the script doesn't ask for confirmation for execution of the command on each member node."
  echo
  echo -e ' \t '"--copy-conf"
  echo -e ' \t ''\t'"This is a shortcut command which when specified copies log4j.properties, snappy-env.sh and "
  echo -e ' \t ''\t'"spark-env.sh configuration files from local machine to all the members."
  echo -e ' \t ''\t'"These files are copied only if a) these are absent in the destination member or b) their content is different. In "
  echo -e ' \t ''\t'"latter case, a backup of the file is taken in conf/backup directory on destination member, before copy."
  echo -e ' \t '"--run '<cmd-to-run-on-selected-nodes>'"
  echo -e ' \t ''\t'"Will execute the given command on specified member type. Any argument after --run will be consider as command, its not getting validated."
  echo
  exit 1
}

ISFORCE=0
SPARK_CONF_DIR=
COPY_CONF=0

MEMBER_LIST=
MEMBER_TYPE=

COMMAND=

while [ "$1" != "" ]; do

  case $1 in
    --run)
      RUN="true"
      COMMAND="$2"
      shift
      ;;
    --copy-conf)
      COPY_CONF=1
      SPARK_CONF_DIR=$SNAPPY_HOME/conf/
      shift
      ;; 
    -y)
      ISFORCE=1
      shift
      ;;
    --on-locators)
      if [[ ! -e $SNAPPY_HOME/conf/locators ]]; then
        echo "${SNAPPY_HOME}/conf/locators does not exist. Exiting ..."
        exit 2
      fi
      LOCATOR_LIST=$(sed ':loop /^[^#].*[^\\]\\$/N; s/\\\n//; t loop' $SNAPPY_HOME/conf/locators | awk '!/^ *#/ && !/^[[:space:]]*$/ { print$1; }' | awk '!seen[$0]++')
      MEMBER_LIST=$LOCATOR_LIST
      MEMBER_TYPE="locator"
      shift
      ;;
    --on-servers)
      if [[ ! -e $SNAPPY_HOME/conf/servers ]]; then
        echo "${SNAPPY_HOME}/conf/servers does not exist. Exiting ..."
        exit 3
      fi
      SERVER_LIST=$(sed ':loop /^[^#].*[^\\]\\$/N; s/\\\n//; t loop' $SNAPPY_HOME/conf/servers | awk '!/^ *#/ && !/^[[:space:]]*$/ { print$1; }' | awk '!seen[$0]++')
      MEMBER_LIST=$SERVER_LIST
      MEMBER_TYPE="server"
      shift
      ;;
    --on-leads)
      if [[ ! -e $SNAPPY_HOME/conf/leads ]]; then
        echo "${SNAPPY_HOME}/conf/leads does not exist. Exiting ..."
        exit 4
      fi
      LEAD_LIST=$(sed ':loop /^[^#].*[^\\]\\$/N; s/\\\n//; t loop' $SNAPPY_HOME/conf/leads | awk '!/^ *#/ && !/^[[:space:]]*$/ { print$1; }' | awk '!seen[$0]++')
      MEMBER_LIST=$LEAD_LIST
      MEMBER_TYPE="lead"
      shift
      ;;
    --on-all)
      MEMBER_LIST="all"
      MEMBER_TYPE="all"
      LOCATOR_LIST=$(sed ':loop /^[^#].*[^\\]\\$/N; s/\\\n//; t loop' $SNAPPY_HOME/conf/locators | awk '!/^ *#/ && !/^[[:space:]]*$/ { print$1; }' | awk '!seen[$0]++')
      echo $LOCATOR_LIST >> /tmp/snappy-nodes.txt
      SERVER_LIST=$(sed ':loop /^[^#].*[^\\]\\$/N; s/\\\n//; t loop' $SNAPPY_HOME/conf/servers | awk '!/^ *#/ && !/^[[:space:]]*$/ { print$1; }' | awk '!seen[$0]++')
      echo $SERVER_LIST >> /tmp/snappy-nodes.txt
      LEAD_LIST=$(sed ':loop /^[^#].*[^\\]\\$/N; s/\\\n//; t loop' $SNAPPY_HOME/conf/leads | awk '!/^ *#/ && !/^[[:space:]]*$/ { print$1; }' | awk '!seen[$0]++')
      echo $LEAD_LIST >> /tmp/snappy-nodes.txt
      #cat /tmp/snappy-nodes.txt
      ALL_MEMBER_LIST=$(awk '!seen[$0]++' /tmp/snappy-nodes.txt) 
      rm /tmp/snappy-nodes.txt
      shift
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

if [[ $COPY_CONF == 1 && $RUN == "true" ]]; then
  echo
  echo "Invalid operation: Either execute --copy-conf or --run '<command>'"
  echo 
  usage
  exit
fi

START_ALL_TIMESTAMP="$(date +"%Y_%m_%d_%H_%M_%S")"

function copyConf() { 
  for entry in "${SPARK_CONF_DIR}"/*; do
    if [ -f "$entry" ];then
      fileName=$(basename $entry)
      if [[ $fileName == "log4j.properties" || $fileName == "snappy-env.sh" || $fileName == "spark-env.sh" ]]; then  	
	if ! ssh $node "test -e $entry"; then #"File does not exist."
	  scp ${SPARK_CONF_DIR}/$fileName  $node:${SPARK_CONF_DIR}
	else
	  backupDir="backup_"${START_ALL_TIMESTAMP}
	  if [[ ! -z $(ssh $node "cat $entry" | diff - "$entry") ]] ; then
	    #backupFileName=${fileName}_${START_ALL_TIMESTAMP}
            echo "backup directory name: $backupDir"
	    ssh "$node" "mkdir -p \"${SPARK_CONF_DIR}/$backupDir\" "
	    ssh $node "mv ${SPARK_CONF_DIR}/$fileName ${SPARK_CONF_DIR}/$backupDir/$fileName"
            echo "INFO:Copying $filename from this host to $node. Moved the original $filename on $node to $backupDir/$fileName."    
	    scp ${SPARK_CONF_DIR}/$fileName  $node:${SPARK_CONF_DIR}
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
  if [[ $ISFORCE -eq 0 ]]; then
    read -p "Are you sure to run $@ on $MEMBER_TYPE $node (y/n)? " userinput
    echo 
    if [[ $userinput == "y" || $userinput == "yes" || $userinput == "Y" || $userinput == "YES" ]]; then
      if [[ $COPY_CONF == 1 ]]; then
        copyConf "$@"
      elif [[ $RUN == "true" ]]; then
        ssh $node "$COMMAND | column"
      else
        echo "Invalid operation"
        echo
        usage
        exit
      fi     
    fi
  else
    if [[ $COPY_CONF == 1 ]]; then
      copyConf "$@"
    elif [[ $RUN == "true" ]]; then
      ssh $node "$COMMAND | column"
    else
      echo "Invalid operation"
      echo
      usage
      exit
    fi   
  fi #end if ISFORCE
}


if [[ $MEMBER_LIST != "all" ]]; then
  for node in $MEMBER_LIST; do
    executeCommand "$@"
  done
else
  for node in $ALL_MEMBER_LIST; do
    executeCommand "$@"
  done
fi


