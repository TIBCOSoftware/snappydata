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

#set -vx

usage(){
  echo "Usage: stability.sh <snappydata-base-directory-path>  <result-directory-path>" 1>&2
  echo " result-directory-path         Location to put the test results " 1>&2
  echo " snappydata-base-directory-path    checkout path of snappy-data " 1>&2
  echo " (e.g. sh stability.sh /home/swati/snappydata /data/snappyHydraLogs)" 1>&2
  exit 1
}

if [ $# -ne 2 ]; then
  usage
else
  SNAPPYDATA_SOURCE_DIR=$1
  shift
fi
resultDir=$1
mkdir -p $resultDir
shift

$SNAPPYDATA_SOURCE_DIR/store/tests/core/src/main/java/bin/sample-runbt.sh $resultDir $SNAPPYDATA_SOURCE_DIR  -r 1 -l  $SNAPPYDATA_SOURCE_DIR/store/tests/sql/build-artifacts/linux/classes/main/io/snappydata/hydra/cluster/local.startEmbeddedModeClusterWithUserConfs.conf -d false io/snappydata/hydra/cluster/startEmbeddedModeClusterWithUserConfs.bt
sleep 30;

$SNAPPYDATA_SOURCE_DIR/store/tests/core/src/main/java/bin/sample-runbt.sh $resultDir $SNAPPYDATA_SOURCE_DIR  -r 1  -d false io/snappydata/hydra/concurrency/stability.bt
sleep 30;

$SNAPPYDATA_SOURCE_DIR/store/tests/core/src/main/java/bin/sample-runbt.sh $resultDir $SNAPPYDATA_SOURCE_DIR  -r 1  -d false io/snappydata/hydra/cluster/stopEmbeddedModeClusterWithUserConfs.bt

