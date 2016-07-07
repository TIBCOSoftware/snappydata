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

#set -vx 

usage(){
  echo "Usage: longRunning.sh <snappydata-base-directory-path> " 1>&2
  echo " snappydata-base-directory-path    checkout path of snappy-data " 1>&2
  echo " (e.g. sh longRunning.sh /home/swati/snappy-commons" 1>&2
  exit 1
}

if [ $# -ne 1 ]; then
  usage
fi

SNAPPYDATA_SOURCE_DIR=$1

$SNAPPYDATA_SOURCE_DIR/store/tests/core/src/main/java/bin/sample-runbt.sh /home/swati/snappyHydraLogs $SNAPPYDATA_SOURCE_DIR  -r 1  -d false io/snappydata/hydra/cluster/startSnappyCluster.bt
sleep 30;

$SNAPPYDATA_SOURCE_DIR/store/tests/core/src/main/java/bin/sample-runbt.sh /home/swati/snappyHydraLogs $SNAPPYDATA_SOURCE_DIR  -r 30  -d false io/snappydata/hydra/cluster/longRunningTest.bt
sleep 30;

$SNAPPYDATA_SOURCE_DIR/store/tests/core/src/main/java/bin/sample-runbt.sh /home/swati/snappyHydraLogs $SNAPPYDATA_SOURCE_DIR  -r 1  -d false io/snappydata/hydra/cluster/stopSnappyCluster.bt
