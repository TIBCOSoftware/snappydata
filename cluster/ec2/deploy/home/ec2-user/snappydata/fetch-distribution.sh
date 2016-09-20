#!/usr/bin/env bash
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

extract() {
  TAR_NAME=`echo ${URL} | cut -d'/' -f 9`
  SNAPPY_HOME_DIR=`echo ${TAR_NAME%.tar.gz}`
  SNAPPY_HOME_DIR_NO_BIN=`echo ${SNAPPY_HOME_DIR%-bin}`

  if [[ ! -d ${SNAPPY_HOME_DIR} ]] && [[ ! -d ${SNAPPY_HOME_DIR_NO_BIN} ]]; then
    # Download and extract the distribution tar
    echo "Downloading ${URL}..."
    wget -q "${URL}"
    tar -xf "${TAR_NAME}"

    rm -f README.md "${TAR_NAME}" releases
  fi
  if [[ -d ${SNAPPY_HOME_DIR_NO_BIN} ]]; then
    SNAPPY_HOME_DIR=${SNAPPY_HOME_DIR_NO_BIN}
  fi
}

getLatestUrl() {
  rm -f README.md
  # Extract the url from the README.md to download the latest distribution tar from.
  wget -q https://github.com/SnappyDataInc/snappydata/blob/master/README.md
  URL=`grep -o "https://github.com/SnappyDataInc/snappydata/releases/download[a-zA-Z0-9.\/\-]**tar.gz" README.md`
}

SNAPPY_HOME_DIR="snappydata-${SNAPPYDATA_VERSION}-bin"
SNAPPY_HOME_DIR_NO_BIN="snappydata-${SNAPPYDATA_VERSION}"

if [[ "${SNAPPYDATA_VERSION}" = "LATEST" ]]; then
  getLatestUrl
  extract
elif [[ ! -d ${SNAPPY_HOME_DIR} ]] && [[ ! -d ${SNAPPY_HOME_DIR_NO_BIN} ]]; then
  wget -q https://github.com/SnappyDataInc/snappydata/releases
  URL_PART=`grep -o "/SnappyDataInc/snappydata/releases/download/[a-zA-Z0-9.\/\-]**${SNAPPYDATA_VERSION}-bin.tar.gz" releases`
  GREP_RESULT=`echo $?`
  if [[ ${GREP_RESULT} != 0 ]]; then
    # Try without '-bin'
    URL_PART=`grep -o "/SnappyDataInc/snappydata/releases/download/[a-zA-Z0-9.\/\-]**${SNAPPYDATA_VERSION}.tar.gz" releases`
    GREP_RESULT=`echo $?`
  fi
  if [[ ${GREP_RESULT} != 0 ]]; then
    echo "Did not find binaries for ${SNAPPYDATA_VERSION}, instead will use the latest version."
    getLatestUrl
  else
    URL="https://github.com${URL_PART}"
  fi
  extract
else
  if [[ -d ${SNAPPY_HOME_DIR_NO_BIN} ]]; then
    SNAPPY_HOME_DIR=${SNAPPY_HOME_DIR_NO_BIN}
  fi
fi

echo -e "export SNAPPY_HOME_DIR=${SNAPPY_HOME_DIR}" >> ec2-variables.sh
