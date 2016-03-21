#!/usr/bin/env bash

#
# Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

#!/usr/bin/env bash

usage="Usage: collect-debug-artifacts [ -c|--conf|--config conffile ] [ -h|--help ] \
       [ -a|--all ] [ -v|--verbose ]"

PWD=`pwd`
while [ "$1" != "" ]; do
key="$1"

case $key in
  -c|--conf|--config)
  CONF_FILE="$2"
  shift # past argument
  ;;
  -h|--help)
  echo $usage
  exit 0
  ;;
  -a|--all)
  GET_EVERYTHING=1
  ;;
  -v|--verbose)
  VERBOSE=1
  ;;
esac
shift # past argument or value
done

# Check configurations and assign defaults
function check_configs {
  if [ -z "${CONF_FILE}" ]; then
    CONF_FILE="../conf/debug.conf.template"
  fi

  if [ ! -f "${CONF_FILE}" ]; then
    echo "Config file ${CONF_FILE} does not exists"
    exit 1
  fi

  source $CONF_FILE

  if [ ! -f "${MEMBERS_INFO_FILE}" ]; then
    echo "MEMBERS_INFO_FILE file ${MEMBERS_INFO_FILE} does not exists"
    exit 1
  fi

  if [ -z "${NO_OF_STACK_DUMPS}" ]; then
    NO_OF_STACK_DUMPS=5
  fi

  if [ -z "${INTERVAL_BETWEEN_DUMPS}" ]; then
    INTERVAL_BETWEEN_DUMPS=5
  fi

  if [ -z "${GET_EVERYTHING}" ]; then
    GET_EVERYTHING=0
  fi

  if [ "${VERBOSE}" = "1" ]; then
    echo CONF=$CONF_FILE
    echo MEMINFO=$MEMBERS_INFO_FILE
    echo NUM_STACK_DUMPS=$NO_OF_STACK_DUMPS
    echo INTERVAL_BETWEEN_DUMPS=$INTERVAL_BETWEEN_DUMPS
    echo GET_EVERYTHING=${GET_EVERYTHING}
  fi
}

function collect_data {
  host=$1
  wd=$2
  srv_num=$3

  if [ "${VERBOSE}" = "1" ]; then
    echo "Collecting data for process running on ${host} with working_dir ${wd}"
  fi

  # make a sub directory for this host and pid
  out_dir="${PWD}/debug_data/${host}-${srv_num}"

  collector_host="pnq-kneeraj3"
  mkdir -p $out_dir 
  if [ "${VERBOSE}" = "1" ]; then
    echo "Args Being passed"
    echo "arg1 wd = ${wd}" 
    echo "arg2 num_stack_dumps = ${NO_OF_STACK_DUMPS}" 
    echo "arg3 interval_dumps = ${INTERVAL_BETWEEN_DUMPS}" 
    echo "arg4 GET_EVERYTHING = ${GET_EVERYTHING}" 
    echo "arg5 collector_host = ${collector_host}" 
    echo "arg6 out dir = ${out_dir}"
  fi

  typeset -f | ssh $host "$(cat);collect_on_remote ${wd} ${NO_OF_STACK_DUMPS} \\
      ${INTERVAL_BETWEEN_DUMPS} ${GET_EVERYTHING} ${collector_host} ${out_dir} ${VERBOSE}"
}

function collect_on_remote {
  data_dir=$1
  num_stack_dumps=$2
  int_stack_dumps=$3
  get_all=$4
  collector_host=$5
  collector_dir=$6
  verbose=$7

  # first get the pid. The latest log file with the header will have the pid
  host=`hostname`
  if [ ! -d $data_dir ]; then
    echo "${data_dir} not found on host: ${host}"
  fi

  cd $data_dir
  files=()
  last_restart_log=""
  for l in $( ls -t *.log )
  do
    copyright_headers=`grep 'Copyright (C)' ${l}`
    if [ ! -z "$copyright_headers" ]; then
      # also check for the pid line and get the pid
      proc_id=`sed -n 's/.*Process ID: \([0-9]\+\)$/\1/p' ${l}`
      if [ "${verbose}" = "1" ]; then
        echo "Addiing file ${l} to the array"
      fi
      files+=($l)
      last_restart_log=$l
      break
    fi
  done

  if [ -z "${proc_id}" ]; then
    echo "No valid process id could be obtained from logs on ${host}"
    exit 1
  fi

  # get the stack dumps first
  dump_num=1
  for i in {1..${num_stack_dumps}}
  do
    dump_num=`expr ${dump_num} + 1`
    kill -URG $proc_id
    sleep $int_stack_dumps
  done

  if [ "${get_all}" = "1" ]; then
    if [ "${verbose}" = "1" ]; then
      rsync -avrz $data_dir "${collector_host}:${collector_dir}/"
    else
      rsync -avrzq $data_dir "${collector_host}:${collector_dir}/"
    fi
  else
    # get the latest log and the latest with the copyright headers
    for l in $( ls -t *.log )
    do
      files+=($l)
      if [ "${l}" = "${last_restart_log}" ]; then
        break
      fi
    done
    # get the latest stats file
    for l in $( ls -t *.gfs 2> /dev/null )
    do
      if [ "${verbose}" = "1" ]; then
        echo "Addiing file ${l} to the array"
      fi
      files+=($l)
    done
  fi
  if [ "${verbose}" = "1" ]; then
    echo "FILES=${files[@]} will be rsynced to ${collector_host}:$collector_dir}"
  fi
  if [ "${verbose}" = "1" ]; then
    rsync -avz ${files[@]} "${collector_host}:${collector_dir}"
  else
    rsync -avzq ${files[@]} "${collector_host}:${collector_dir}"
  fi
}

check_configs
# Assuming each line in the members info file has the following format
# host pid cwd

all_pids=()
# Make output directory
rm -rf debug_data 2> /dev/null
mkdir debug_data

serv_num=1
while  read -r line || [[ -n "$line" ]]; do
  if [ "${VERBOSE}" = "1" ]; then
    echo "Line read from file: $line"
  fi

  read host cwd <<< $line
  if [ "${VERBOSE}" = "1" ]; then
    echo "host: $host pid: $pid and cwd: $cwd"
  fi

  collect_data $host $cwd $serv_num &
  serv_num=`expr $serv_num + 1`
  all_pids+=($!)
done < $MEMBERS_INFO_FILE

for p in "${all_pids[@]}"
do
  if [ "${VERBOSE}" = "1" ]; then
    echo "Waiting for pid ${p}"
  fi
  wait $p 2> /dev/null
done

# make zipped tar ball
tar -zcf debug_data.tar.gz debug_data
