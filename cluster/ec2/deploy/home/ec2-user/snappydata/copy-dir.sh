#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This script is taken from 
# https://github.com/amplab/spark-ec2/blob/branch-1.6/copy-dir.sh
# with modifications.


DELETE_FLAG=""

usage() {
  echo "Usage: copy-dir [--delete] <dir> <host-list>"
  exit 1
}

while :
do
  case $1 in
    --delete)
      DELETE_FLAG="--delete"
      shift
      ;;
    -*)
      echo "ERROR: Unknown option: $1" >&2
      usage
      ;;
    *) # End of options
      break
      ;;
  esac
done

if [[ "$#" != "2" ]] ; then
  usage
fi

if [[ ! -e "$1" ]] ; then
  echo "File or directory $1 doesn't exist!"
  exit 1
fi

if [[ ! -e "$2" ]] ; then
  echo "File $2, containing the host-list, doesn't exist!"
  exit 1
fi

DIR=`readlink -f "$1"`
DIR=`echo "$DIR"|sed 's@/$@@'`
DEST=`dirname "$DIR"`

HOSTS=`cat "$2"`

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=5"

for node in $HOSTS; do
    echo "$2:  RSYNC'ing $1  to  $node:$DEST"
    rsync -e "ssh $SSH_OPTS" -az $DELETE_FLAG --exclude=ec2-* "$DIR" "$node:$DEST" & sleep 0.5
done
wait
