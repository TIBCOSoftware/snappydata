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

pushd /home/ec2-user/snappydata > /dev/null

# Extract the url from the README.md to download the latest distribution tar from.
wget -q https://github.com/SnappyDataInc/snappydata/blob/master/README.md

URL=`grep -o "https://github.com/SnappyDataInc/snappydata/releases/download[a-zA-Z0-9.\/\-]**tar.gz" README.md`
TAR_NAME=`echo ${URL} | cut -d'/' -f 9`
SNAPPY_HOME_DIR=`echo ${TAR_NAME%.tar.gz}`

# Download and extract the distribution tar
echo "Downloading ${URL}..."
wget "${URL}"
tar -xf "${TAR_NAME}"

rm README.md "${TAR_NAME}"

source ec2-variables.sh

echo "$LOCATORS" > locator_list
echo "$LEADS" > lead_list
echo "$SERVERS" > server_list

if [[ -e snappy-env.sh ]]; then
  mv snappy-env.sh "${SNAPPY_HOME_DIR}/conf/"
fi

# Place the list of locators, leads and servers under conf directory
if [[ -e locators ]]; then
  mv locators "${SNAPPY_HOME_DIR}/conf/"
else
  cp locator_list "${SNAPPY_HOME_DIR}/conf/locators"
fi

if [[ -e leads ]]; then
  mv leads "${SNAPPY_HOME_DIR}/conf/"
else
  cp lead_list "${SNAPPY_HOME_DIR}/conf/leads"
fi

if [[ -e servers ]]; then
  mv servers "${SNAPPY_HOME_DIR}/conf/"
else
  cp server_list "${SNAPPY_HOME_DIR}/conf/servers"
fi

OTHER_LOCATORS=`cat locator_list | sed '1d'`
echo "$OTHER_LOCATORS" > other-locators

# Copy this extracted directory to all the other instances
sh copy-dir.sh "${SNAPPY_HOME_DIR}"  other-locators
sh copy-dir.sh "${SNAPPY_HOME_DIR}"  lead_list
sh copy-dir.sh "${SNAPPY_HOME_DIR}"  server_list

# Launch the SnappyData cluster
sh "${SNAPPY_HOME_DIR}/sbin/snappy-start-all.sh"

popd > /dev/null
