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
  echo "${bold}Usage: ${normal}cluster-util.sh -onlocator|-onserver|-onlead|-onall [-f|--force] --run  -copyconf | <cmd-to-run-on-selected-nodes>"
  echo
  echo "${bold}Discription${normal}"
  echo
  echo -e ' \t '"This is exprimental utility for basically syncup cluster's member configuration files and along with this "
  echo -e ' \t '"execute user command on selected member type"
  echo 
  echo -e ' \t '"-onlocator|-onserver|-onlead|-onall"   
  echo -e ' \t ''\t'"Member type on which command will get execute"
  echo
  echo -e ' \t '"-f|--force"
  echo -e ' \t ''\t'"For skiping the confirmation before executing the input command"
  echo
  echo -e ' \t '"-copyconf"
  echo -e ' \t ''\t'"Copy log4j.properties, snappy-env.sh, spark-env.sh configuration files on the selected member type of cluster."
  echo -e ' \t ''\t'"If these file already present in selected member then will only copy if there are some diff between these files"
  echo -e ' \t ''\t'"but create the backup file before copy"
  echo -e ' \t '"<cmd-to-run-on-selected-nodes>"
  echo -e ' \t ''\t'"Command"
  echo
  exit 1
}

COMPONENT_TYPE=$1
shift

# Whether to apply the operation forcefully.
ISFORCE=0
if [ "$1" = "-f" -o "$1" = "--force" ]; then
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

SERVER_LIST=`awk '!/^ *#/ && !/^[[:space:]]*$/ { print$1; }' $SNAPPY_DIR/conf/servers`

LEAD_LIST=`awk '!/^ *#/ && !/^[[:space:]]*$/ { print$1; }' $SNAPPY_DIR/conf/leads`

LOCATOR_LIST=`awk '!/^ *#/ && !/^[[:space:]]*$/ { print$1; }' $SNAPPY_DIR/conf/locators`

MEMBER_LIST=
MEMBER_TYPE=
case $COMPONENT_TYPE in

  (-onlocator)
    MEMBER_LIST=$LOCATOR_LIST
    MEMBER_TYPE="locator"
    ;;

  (-onserver)
    MEMBER_LIST=$SERVER_LIST
    MEMBER_TYPE="server"
    ;;
  (-onlead)
    MEMBER_LIST=$LEAD_LIST
    MEMBER_TYPE="lead"
    ;;
  (-onall)
    MEMBER_LIST="all"
    MEMBER_TYPE="all"
    ;;
esac

function execute() {
  echo
  echo "--------- Executing $@ on $MEMBER_TYPE $node ----------"
  echo 
  if [[ $COPY_CONF = 1 ]]; then
    executeCopyCommand "$@"
  else
    executeCommand "$@"
  fi
}

START_ALL_TIMESTAMP="$(date +"%Y_%m_%d_%H_%M_%S")"

function copyConf() { 
  for entry in "${SPARK_CONF_DIR}"/*; do
      if [ -f "$entry" ];then
	fileName=$(basename $entry)
 	template=".template"
	#skip file with .template extension
	if [[ ! "$fileName" = @(*.template) ]]; then
	  if [[ $fileName == "log4j.properties" || $fileName == "snappy-env.sh" || $fileName == "spark-env.sh" ]];then 	       	
	    if ! ssh $node "test -e $entry"; then #"File does not exist."	  
	      scp ${SPARK_CONF_DIR}/$fileName  $node:${SPARK_CONF_DIR}
	    else
	      backupDir="backup"
	      if [[ ! -z $(ssh $node "cat $entry" | diff - "$entry") ]] ; then
		backupFileName=${fileName}_${START_ALL_TIMESTAMP}
		echo "INFO: Copied $filename from this host to $node. Moved the original $filename on $node to $backupFileName."
		(ssh "$node" "{ if [ ! -d \"${SPARK_CONF_DIR}/$backupDir\" ]; then  mkdir \"${SPARK_CONF_DIR}/$backupDir\"; fi; } ")
		ssh $node "mv ${SPARK_CONF_DIR}/$fileName ${SPARK_CONF_DIR}/$backupDir/$backupFileName"		    
		scp ${SPARK_CONF_DIR}/$fileName  $node:${SPARK_CONF_DIR} 
	      fi
	    fi
	  fi # end of if, check the conf file name      				
	fi # end of if, check extension
      fi # end of if to get each file
  done  #end of for loop
}

function executeCopyCommand() {
  if [[ $ISFORCE -eq 0 ]];then
    read -p "Are you sure to run $@ on $MEMBER_TYPE $node (y/n)?" userinput
    echo 
    if [[ $userinput == "y" || $userinput == "yes" || $userinput == "Y" || $userinput == "YES" ]]; then
      copyConf "$@"
    fi
  else
    copyConf "$@"
  fi
}

function executeCommand() {
  
  if [[ $ISFORCE -eq 0 ]];then
    read -p "Are you sure to run $@ on $MEMBER_TYPE $node (y/n)?" userinput
    echo 
    if [[ $userinput == "y" || $userinput == "yes" || $userinput == "Y" || $userinput == "YES" ]]; then
      ssh $node "$@ | column"
    fi
  else
    ssh $node "$@ | column"
  fi
}

if [[ $RUN = "true" ]]; then
  if [[ $MEMBER_LIST != "all" ]]; then
    for node in $MEMBER_LIST; do
      execute "$@"
    done
  else
    for node in $SERVER_LIST; do
      MEMBER_TYPE="server"
      execute "$@"
    done
    for node in $LEAD_LIST; do
      MEMBER_TYPE="lead"
      execute "$@"
    done
    for node in $LOCATOR_LIST; do
      MEMBER_TYPE="locator"
      execute "$@"
    done
  fi
fi

