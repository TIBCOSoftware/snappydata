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

  if [[ ! -d ${SNAPPY_HOME_DIR} ]]; then
    # Download and extract the distribution tar
    echo "Downloading ${URL}..."
    wget "${URL}"
    tar -xf "${TAR_NAME}"

    rm README.md "${TAR_NAME}"
  fi
}

if [[ "${SNAPPYDATA_VERSION}" = "LATEST" ]]; then
  rm -f README.md
  # Extract the url from the README.md to download the latest distribution tar from.
  wget -q https://github.com/SnappyDataInc/snappydata/blob/master/README.md
  URL=`grep -o "https://github.com/SnappyDataInc/snappydata/releases/download[a-zA-Z0-9.\/\-]**tar.gz" README.md`
else
  # TODO If tag name is case sensitive, this may result into incorrect url.
  URL="https://github.com/SnappyDataInc/snappydata/releases/download/v${SNAPPYDATA_VERSION}/snappydata-${SNAPPYDATA_VERSION}-bin.tar.gz"
fi

extract

echo -e "export SNAPPY_HOME_DIR=${SNAPPY_HOME_DIR}" >> ec2-variables.sh
