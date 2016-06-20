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

pushd /root/snappydata > /dev/null

# Extract the url to download the latest distribution tar from
wget -q https://github.com/SnappyDataInc/snappydata/blob/master/README.md

URL=`grep -o "https://github.com/SnappyDataInc/snappydata/releases/download[a-zA-Z0-9.\/\-]**tar.gz" README.md`

echo "Downloading ${URL}..."

TARNAME=`echo ${URL} | cut -d'/' -f 9`

SNAPPY_HOME_DIR=`echo ${TARNAME%.tar.gz}`

# Download and extract the distribution tar
wget -q "${URL}"

tar -xf "${TARNAME}"

rm README.md "${TARNAME}"

source ec2-variables.sh

# Place the list of locators, leads and servers under conf directory
echo "$LOCATORS" > "${SNAPPY_HOME_DIR}/conf/locators"

echo "$LEADS" > "${SNAPPY_HOME_DIR}/conf/leads"

echo "$SERVERS" > "${SNAPPY_HOME_DIR}/conf/servers"

if [[ -e snappy-env.sh ]]; then
  mv snappy-env.sh "${SNAPPY_HOME_DIR}/conf/"
fi

OTHER_LEADS=`cat "${SNAPPY_HOME_DIR}/conf/leads" | sed '1d'`

echo "$OTHER_LEADS" > other-leads

# Copy this extracted directory to all the other instances
sh copy-dir.sh "${SNAPPY_HOME_DIR}"  "${SNAPPY_HOME_DIR}/conf/locators"

sh copy-dir.sh "${SNAPPY_HOME_DIR}"  other-leads

sh copy-dir.sh "${SNAPPY_HOME_DIR}"  "${SNAPPY_HOME_DIR}/conf/servers"

# Launch the SnappyData cluster
sh "${SNAPPY_HOME_DIR}/sbin/snappy-start-all.sh"

popd > /dev/null
