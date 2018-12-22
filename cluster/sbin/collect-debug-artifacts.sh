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

#!/usr/bin/env bash

timestamp_format="YYYY-MM-DD HH:MM[:SS]"

function usage {
  echo
  echo "Usage: collect-debug-artifacts"
  echo "       [ -c conffile|--conf=conffile|--config=conffile ]"
  echo "       [ -o resultdir|--out=resultdir|--outdir=resultdir ]"
  echo "       [ -h|--help ]"
  echo "       [ -a|--all ]"
  echo "       [ -d|--dump ]"
  echo "       [ -v|--verbose ]"
  echo "       [ -s starttimestamp|--start=starttimestamp ]"
  echo "       [ -e endtimestamp|--end=endtimestamp ]"
  echo "       [ -x debugtarfile|--extract=debugtarfile ]"
  echo
  echo "       Timestamp format: ${timestamp_format}"
  echo
}

function absPath() {
  perl -MCwd -le 'print Cwd::abs_path(shift)' "$1"
}

if [ -z "${SNAPPY_HOME}" ]; then
  export SNAPPY_HOME="$(absPath "$(dirname "$(absPath "$0")")/..")"
fi

while [ "$1" != "" ]; do
  option="$1"

  case "$option" in
    -c)
      CONF_FILE="$2"
      shift ;;
    --conf=*|--config=*)
      CONF_FILE="`echo "$1" | sed 's/^[^=]*=//'`" ;;
    -x)
      TAR_FILE="$2"
      shift ;;
    --extract=*|--xtract=*)
      TAR_FILE="`echo "$1" | sed 's/^[^=]*=//'`" ;;
    -o)
      OUTPUT_DIR="$2"
      shift ;;
    --out=*|--outdir=*)
      OUTPUT_DIR="`echo "$1" | sed 's/^[^=]*=//'`" ;;
    -s)
      START_TIME="$2"
      shift ;;
    --start=*)
      START_TIME="`echo "$1" | sed 's/^[^=]*=//'`" ;;
    -e)
      END_TIME="$2"
      shift ;;
    --end=*)
      END_TIME="`echo "$1" | sed 's/^[^=]*=//'`" ;;
    -h|--help)
    usage
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
    -m|--hprofdump)
    HPROF_DUMP=1
    ;;
    *)
    usage
    exit 1 
  esac
  shift # past argument or value
done

num_regex='^[0-9]+$'

# Check configurations and assign defaults
function check_configs {

  if [ -n "${TAR_FILE}" ]; then
    if [ ! -f "${TAR_FILE}" ]; then
      echo "Debug Tar file ${TAR_FILE} does not exist"
      exit 1
    fi
    # no need of further configuration checks for extraction
    return
  fi

  if [ -z "${CONF_FILE}" ]; then
    CONF_FILE="${SNAPPY_HOME}/conf/debug.conf.template"
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
    echo START TIME = "${START_TIME}"
    echo END   TIME = "${END_TIME}"
    echo SNAPPY_HOME = "${SNAPPY_HOME}"
    echo OUTPUT_DIR = "${OUTPUT_DIR}"
    echo TAR_FILE = "${TAR_FILE}"
  fi

  if [ -z "${START_TIME}" ]; then
    START_EPOCH=0
  else
    START_EPOCH=$(date +%s --date "${START_TIME}" 2>/dev/null)
    if ! [[ "$START_EPOCH" =~ $num_regex ]] ; then
      echo "Error: Not expected date format '${START_TIME}'"
      echo 
      echo "Expected Timestamp format: ${timestamp_format}"
      exit 1
    fi
  fi

  if [ -z "${END_TIME}" ]; then
    END_EPOCH=0
  else
    END_EPOCH=`date +%s --date "${END_TIME}" 2>/dev/null`
    if ! [[ $END_EPOCH =~ $num_regex ]] ; then
      echo "Error: Not expected date format '${END_TIME}'"
      echo 
      echo "Expected Timestamp format: ${timestamp_format}"
      exit 1
    fi
  fi

  if [ "${START_EPOCH}" = "0" -a "${END_EPOCH}" != "0" ] \
    || [ "${START_EPOCH}" != "0" -a  "${END_EPOCH}" = "0" ]; then
    echo
    echo "Please verify start and end time both"
    echo "Timestamp format: ${timestamp_format}"
    usage
    exit 1
  fi
}

collector_host=`hostname`

function extract {
    debugtarzip="$1"
    xtractdir=`dirname ${debugtarzip}`
    tarname=`basename ${debugtarzip}`
    cd $xtractdir
    tar -xf $tarname
    for zf in `find . -name '*.gz'`; do
      ( cd "`dirname "$zf"`" && gunzip "`basename "$zf"`" )
    done
    echo "extracted in ${xtractdir}"
}

function collect_data {
  host="$1"
  wd="$2"

  if [ "${VERBOSE}" = "1" ]; then
    echo "Collecting data for process running on ${host} with working_dir ${wd}"
  fi

  if [ "${VERBOSE}" = "1" ]; then
    echo "Args Being passed for host ${host}"
    echo "arg1 working directory = ${wd}" 
    echo "arg2 num_stack_dumps = ${NO_OF_STACK_DUMPS}" 
    echo "arg3 interval_dumps = ${INTERVAL_BETWEEN_DUMPS}" 
    echo "arg4 get_everything = ${GET_EVERYTHING}" 
    echo "arg5 collector_host = ${collector_host}" 
    echo "arg6 verbose = ${VERBOSE}"
    echo "arg7 start epoch = ${START_EPOCH}"
    echo "arg8 end epoch = ${END_EPOCH}"
  fi

  if [ "${DUMP_STACK}" != "1" ]; then
    NO_OF_STACK_DUMPS=0
    INTERVAL_BETWEEN_DUMPS=0
  fi

  if [ "${HPROF_DUMP}" != "1" ]; then
    HPROF_DUMP=0
  fi

  # Create the outdir with the same name on each remote and collect everything there.
  typeset -f | ssh $host "$(cat);collect_on_remote \"${wd}\" \"${NO_OF_STACK_DUMPS}\" \\
      \"${INTERVAL_BETWEEN_DUMPS}\" \"${GET_EVERYTHING}\" \"${collector_host}\" \\
      \"${VERBOSE}\" \"${START_EPOCH}\" \"${END_EPOCH}\" \"${HPROF_DUMP}\""
}

function collect_on_remote {
  data_dir="$1"
  num_stack_dumps="$2"
  int_stack_dumps="$3"
  get_all="$4"
  collector_host="$5"
  verbose="$6"
  start_epoch="$7"
  end_epoch="$8"
  get_hprof="$9"

  # Create a .tmpcda dir if not exists else empty it
  tmp_dir="$data_dir/.tmpcda"
  if [ -d ${tmp_dir} ]; then
    rm -rf ${tmp_dir}/*
  else
    mkdir -p $tmp_dir
    retval=$?
    if [ ! -d ${tmp_dir} ]; then
      echo "FAILED TO CREATE tmp dir on ${host} at ${data_dir} with errno ${retval}"
      exit 1
    fi
    if [ "${verbose}" = "1" ]; then
      echo "created dir ${tmp_dir} on remote host"
    fi
  fi  

  # first get the pid. The latest log file with the header will have the pid
  host=`hostname`
  if [ ! -d "$data_dir" ]; then
    echo "${data_dir} not found on host: ${host}"
    exit 1
  fi

  proc_id=""

  cd $data_dir

  if [ "${get_all}" = "1" ]; then
    if [ "${verbose}" = "1" ]; then
      echo "collecting everything in the working dir"
    fi
    for l in $( ls *.log* 2> /dev/null )
    do
      files+=($l)
    done
    for l in $( ls *.gfs* 2> /dev/null )
    do
      files+=($l)
    done
    for l in $( ls *.conf* 2> /dev/null )
    do
      files+=($l)
    done
    for l in $( ls *.out* 2> /dev/null )
    do
      files+=($l)
    done
    for l in $( ls *.bin* 2> /dev/null )
    do
      files+=($l)
    done
  elif [ "${start_epoch}" = "0" ]; then
    if [ "${verbose}" = "1" ]; then
      echo "collecting latest log files and all stats file"
    fi
    logs_latest_first=`ls -t *.log* | grep -Ev '(^start_.+\.log|^locator.+views\.log|derby.log)'`
    all_logs=($logs_latest_first)
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
          echo "Adding latest copyright header file ${l} to the array"
        fi
        files+=($l)
        last_restart_log="$l"
      else
        if [ "${verbose}" = "1" ]; then
          echo "Adding file ${l} to the array"
        fi
        files+=($l)
      fi
    done

    # get all the gfs files as well
    for l in $( ls *.gfs* 2> /dev/null )
    do
      files+=($l)
    done
  else 
    if [ "${verbose}" = "1" ]; then
      echo "collecting files based on modified time"
    fi
    files=()
    prev_file_mod_epoch=0
    for l in $( ls -tr *.log* | grep -Ev '(^start_.+\.log|^locator.+views\.log|derby.log)' 2>/dev/null )
    do
      file_mod_epoch=`stat -c %Y $l`
      if [ "${file_mod_epoch}" -ge "${start_epoch}" -a "${prev_file_mod_epoch}" -le "${end_epoch}" ]; then
        if [ "${verbose}" = "1" ]; then
          echo "${l} MOD TIME = ${file_mod_epoch}"
          echo "Adding file ${l} to the array"
        fi
        files+=($l)
        file_added=1
      fi
      prev_file_mod_epoch=$file_mod_epoch
    done

    prev_file_mod_epoch=0
    for l in $( ls -tr *.gfs* 2>/dev/null )
    do
      file_mod_epoch=`stat -c %Y $l`
      if [ "${file_mod_epoch}" -ge "${start_epoch}" -a "${prev_file_mod_epoch}" -le "${end_epoch}" ]; then
        if [ "${verbose}" = "1" ]; then
          echo "${l} MOD TIME = ${file_mod_epoch}"
          echo "Adding file ${l} to the array"
        fi
        files+=($l)
      fi
      prev_file_mod_epoch=$file_mod_epoch
    done
  fi

  # get the stack dumps if required
  if [ "$num_stack_dumps" -gt "0" ]; then
    # add the latest log file and keep it. Later after taking the dump take all the log files
    # which got created after this one as rollover would have taken place.

    logs_latest_first=`ls -t *.log* | grep -Ev '(^start_.+\.log|^locator.+views\.log|derby.log)' 2>/dev/null`
    all_logs=($logs_latest_first)
    latest_log=${all_logs[0]}
    all_logs=($logs_latest_first)
    for l in "${all_logs[@]}"
    do
      copyright_headers=`grep 'Copyright [ ]*([ ]*.[ ]*)' ${l}`
      if [ ! -z "$copyright_headers" ]; then
        # also check for the pid line and get the pid
        proc_id=`sed -n 's/.*Process ID: \([0-9]\+\)$/\1/p' ${l}`
        break
      fi
    done

    dump_num=1
    for i in `seq 1 ${num_stack_dumps}`
    do
      dump_num=`expr ${dump_num} + 1`
      if [ "${verbose}" = "1" ]; then
        echo "Taking the dump of process ${proc_id} on ${host} -- count ${i}"
      fi
      kill -URG $proc_id
      kill -QUIT $proc_id
      # record the last modified time of this log
      if [ "$i" = "1" ]; then
        first_dump_file_mod_epoch=`stat -c %Y $latest_log`
      fi

      if [ "$i" -lt "${num_stack_dumps}" ]; then
        echo "Sleeping for ${int_stack_dumps} seconds before taking next stack dump"
      fi
      sleep $int_stack_dumps
    done
  fi

  logs_latest_first=`ls -t *.log* | grep -Ev '(^start_.+\.log|^locator.+views\.log|derby.log)' 2>/dev/null`
  all_logs=($logs_latest_first)
  # add all the remaining whose modified time is greater than the last recorded
  if [ ! -z "${first_dump_file_mod_epoch}" ]; then
    for l in "${all_logs[@]}"
    do
      mod_epoch=`stat -c %Y $l`
      if [ "${mod_epoch}" -gt "${first_dump_file_mod_epoch}" ]; then
        files+=($l)
      else
        break
      fi
    done
  fi

  # Add other files
  for l in $( ls *.jfr* 2> /dev/null )
  do
    files+=($l)
  done
  for l in $( ls jvmkill*.log 2> /dev/null )
  do
    files+=($l)
  done
  for l in $( ls *.jmap 2> /dev/null )
  do
    files+=($l)
  done

  # Add hprof files too if asked for
  if [ "${get_hprof}" = "1" ]; then
    if [ "${verbose}" = "1" ]; then
      echo "collecting hprof files too"
    fi
    for l in $( ls *.hprof 2> /dev/null )
    do
      files+=($l)
    done
  fi

  for f in "${files[@]}"
  do
    if [ "${verbose}" = "1" ]; then
      echo "copying file ${f} in dir ${tmp_dir}/"
    fi
    cp $f "${tmp_dir}/"
    returnval=$?
    if [ "${verbose}" = "1" ]; then
      echo "copied and returnval=${returnval}"
    fi
  done

  # gzip all the files so that rsync is fast
  cd "${tmp_dir}"
  for i in `ls`
  do
    if [ "${verbose}" = "1" ]; then
      echo "zipping ${i}"
    fi
    gzip $i
    if [ "${verbose}" = "1" ]; then
      echo "zipp of ${i} done"
    fi
  done

  if [ "${verbose}" = "1" ]; then
    echo "FILES=${files[@]} zipped and copied to ${collector_host}:$tmp_dir}"
  fi
}

check_configs
# Assuming each line in the members info file has the following format
# host pid cwd

if [ -n "${TAR_FILE}" ]; then
  ( extract "${TAR_FILE}" )
  exit 0
fi

# Make output directory
TS=`date +%m.%d.%H.%M.%S`
if [ -z "${OUTPUT_DIR}" ]; then
  out_dir="${SNAPPY_HOME}/work/debug_data_${TS}"
else
  out_dir="${OUTPUT_DIR}/debug_data_${TS}"
fi

if [ "${VERBOSE}" = "1" ]; then
  echo "Top Level output dir = ${out_dir}"
fi

mkdir -p $out_dir

# get the uniq lines from the members file
tmp_members_file="$(mktemp /tmp/debug_mem.XXXX)"

sort $MEMBERS_FILE | uniq >  $tmp_members_file

all_pids=()
while  read -r line || [[ -n "$line" ]]; do
  if [ "${VERBOSE}" = "1" ]; then
    echo "Line read from file: $line"
  fi

  read host cwd <<< $line
  if [ "${VERBOSE}" = "1" ]; then
    echo "host: $host pid: $pid and cwd: $cwd"
  fi

  collect_data $host $cwd &
  all_pids+=($!) 
done < $tmp_members_file

# wait for all the collection to end on respective hosts
# Then rsync 1 by 1
for p in "${all_pids[@]}"
do
  if [ "${VERBOSE}" = "1" ]; then
    echo "Waiting for pid ${p}"
  fi
  wait $p 2> /dev/null
done

while  read -r line || [[ -n "$line" ]]; do
  read host cwd <<< $line
  basenamedir=`basename $cwd`
  hostout_dir="${out_dir}/${host}-${basenamedir}"
  mkdir "${hostout_dir}"
  if [ "${VERBOSE}" = "1" ]; then
    rsync -av --remove-source-files "${host}:${cwd}/.tmpcda/*"  "${hostout_dir}"/
  else
    rsync -a --remove-source-files "${host}:${cwd}/.tmpcda/*"  "${hostout_dir}"/
  fi
done < $tmp_members_file

rm -rf $tmp_members_file

# make tar ball
echo
echo "Collected artifacts in tar file: ${out_dir}.tar"
echo
cd "${out_dir}/.."
tar -cf "${out_dir}.tar" $(basename $out_dir)
rm -rf ${out_dir}
