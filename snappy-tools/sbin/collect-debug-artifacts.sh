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

usage="Usage: collect-debug-artifacts \
        [ -c confifile|--conf=conffile|--config=conffile ] [ -h|--help ] \
        [ -a|--all ] [ -v|--verbose ]"

SCRIPT_DIR="`dirname "$0"`"
SCRIPT_DIR="`cd "$SCRIPT_DIR" && pwd`"

while [ "$1" != "" ]; do
  option="$1"

  case "$option" in
    -c)
      CONF_FILE="$2"
      shift ;;
    --conf=*|--config=*)
      CONF_FILE="`echo "$2" | sed 's/^[^=]*=//'`" ;;
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
    -d|--dump)
    DUMP_STACK=1
    ;;
  esac
  shift # past argument or value
done

# Check configurations and assign defaults
function check_configs {
  if [ -z "${CONF_FILE}" ]; then
    CONF_FILE="${SCRIPT_DIR}/../conf/debug.conf.template"
  fi

  if [ ! -f "${CONF_FILE}" ]; then
    echo "Config file ${CONF_FILE} does not exist"
    exit 1
  fi

  source $CONF_FILE

  if [ ! -f "${MEMBERS_FILE}" ]; then
    echo "members file ${MEMBERS_FILE} does not exist"
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
    echo MEMINFO=$MEMBERS_FILE
    echo NUM_STACK_DUMPS=$NO_OF_STACK_DUMPS
    echo INTERVAL_BETWEEN_DUMPS=$INTERVAL_BETWEEN_DUMPS
    echo GET_EVERYTHING=${GET_EVERYTHING}
  fi
}

collector_host=`hostname`

function collect_data {
  host="$1"
  wd="$2"
  srv_num="$3"
  top_level_out_dir="$4"

  if [ "${VERBOSE}" = "1" ]; then
    echo "Collecting data for process running on ${host} with working_dir ${wd}"
  fi

  # make a sub directory for this host and pid
  out_dir="${top_level_out_dir}/${host}-${srv_num}"

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

  if [ "${DUMP_STACK}" != "1" ]; then
    NO_OF_STACK_DUMPS=0
    INTERVAL_BETWEEN_DUMPS=0
  fi

  typeset -f | ssh $host "$(cat);collect_on_remote \"${wd}\" \"${NO_OF_STACK_DUMPS}\" \\
      \"${INTERVAL_BETWEEN_DUMPS}\" \"${GET_EVERYTHING}\" \"${collector_host}\" \"${out_dir}\" \"${VERBOSE}\""
}

function collect_on_remote {
  data_dir="$1"
  num_stack_dumps="$2"
  int_stack_dumps="$3"
  get_all="$4"
  collector_host="$5"
  collector_dir="$6"
  verbose="$7"

  tmp_dir="$(mktemp -d --tmpdir="`pwd`" data.XXXX)"
  retval=$?
  if [ ! -d ${tmp_dir} ]; then
    echo "FAILED TO CREATE tmp dir on ${host} at ${data_dir} with errno ${retval}"
  fi

  # first get the pid. The latest log file with the header will have the pid
  host=`hostname`
  if [ ! -d "$data_dir" ]; then
    echo "${data_dir} not found on host: ${host}"
  fi

  cd "$data_dir"

  if [ "${get_all}" = "1" ]; then
    for l in $( ls *.log 2> /dev/null )
    do
      files+=($l)
    done
    for l in $( ls *.gfs 2> /dev/null )
    do
      files+=($l)
    done
  else
    logs_sorted_reverse=`ls *.log | sed 's/\([0-9]\)/;\1/' | sort -r -n -t\; -k2,2 | tr -d ';'`

    arr=($logs_sorted_reverse)
    latest_log=${arr[-1]}
    unset arr[${#arr[@]}-1]

    all_logs=()
    all_logs+=($latest_log)
    all_logs+=("${arr[@]}")

    files=()
    last_restart_log=""
    for l in "${all_logs[@]}"
    do
      # If last log is got, get the one before that as well
      if [ ! -z "$last_restart_log" ]; then
        if [ "${verbose}" = "1" ]; then
          echo "Adding the last file ${l} to the array"
        fi
        files+=($l)
        break
      fi
      copyright_headers=`grep 'Copyright [ ]*([ ]*.[ ]*)' ${l}`
      if [ ! -z "$copyright_headers" ]; then
        # also check for the pid line and get the pid
        proc_id=`sed -n 's/.*Process ID: \([0-9]\+\)$/\1/p' ${l}`
        if [ "${verbose}" = "1" ]; then
          echo "Adding file ${l} to the array"
        fi
        files+=($l)
        last_restart_log="$l"
      fi
    done

    # get all the gfs files as well
    for l in $( ls *.gfs 2> /dev/null )
    do
      files+=($l)
    done
  fi

  # get the stack dumps first
  if [ "${num_stack_dumps}" -gt 0 ]; then
    dump_num=1
    for i in `seq 1 ${num_stack_dumps}`
    do
      dump_num=`expr ${dump_num} + 1`
      if [ "${verbose}" = "1" ]; then
        echo "Taking the dump for on ${host} -- count ${i}"
      fi
      kill -URG $proc_id
      sleep $int_stack_dumps
    done
  fi

  for f in "${files[@]}"
  do
    if [ "${verbose}" = "1" ]; then
      echo "copying file ${f} in dir ${tmp_dir}"
    fi
    cp $f "${tmp_dir}/"
  done

  if [ "${verbose}" = "1" ]; then
    echo "FILES=${files[@]} will be rsynced to ${collector_host}:$collector_dir}"
  fi

  cd "${tmp_dir}/.."
  tar cvzf "${tmp_dir}.tar.gz" $(basename $tmp_dir) && \
    rsync -av "${tmp_dir}.tar.gz" "${collector_host}:${collector_dir}/" && \
      rm -f "${tmp_dir}.tar.gz"
}

check_configs
# Assuming each line in the members info file has the following format
# host pid cwd

all_pids=()
# Make output directory
TS=`date +%m.%d.%H.%M.%S`
OUT_DIR="${SCRIPT_DIR}/debug_data_${TS}"

mkdir "$OUT_DIR"

serv_num=1
while  read -r line || [[ -n "$line" ]]; do
  if [ "${VERBOSE}" = "1" ]; then
    echo "Line read from file: $line"
  fi

  read host cwd <<< $line
  if [ "${VERBOSE}" = "1" ]; then
    echo "host: $host pid: $pid and cwd: $cwd"
  fi

  collect_data $host $cwd $serv_num $OUT_DIR &
  serv_num=`expr $serv_num + 1`
  all_pids+=($!)
done < $MEMBERS_FILE

for p in "${all_pids[@]}"
do
  if [ "${VERBOSE}" = "1" ]; then
    echo "Waiting for pid ${p}"
  fi
  wait $p 2> /dev/null
done

# make tar ball
cd "${OUT_DIR}/.."
tar -cf "${OUT_DIR}.tar" $(basename $OUT_DIR)
rm -rf ${OUT_DIR}
