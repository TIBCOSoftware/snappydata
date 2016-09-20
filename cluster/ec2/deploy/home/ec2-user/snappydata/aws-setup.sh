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

source ec2-variables.sh

sh resolve-hostname.sh

# Download and extract the appropriate distribution.
sh fetch-distribution.sh

# Do it again to read new variables.
source ec2-variables.sh

# Stop an already running cluster, if so.
sh "${SNAPPY_HOME_DIR}/sbin/snappy-stop-all.sh"

echo "$LOCATORS" > locator_list
echo "$LEADS" > lead_list
echo "$SERVERS" > server_list
echo "$ZEPPELIN_HOST" > zeppelin_server

if [[ -e snappy-env.sh ]]; then
  mv snappy-env.sh "${SNAPPY_HOME_DIR}/conf/"
fi

# Place the list of locators, leads and servers under conf directory
if [[ -e locators ]]; then
  mv locators "${SNAPPY_HOME_DIR}/conf/"
else
  cp locator_list "${SNAPPY_HOME_DIR}/conf/locators"
fi

# Enable jmx-manager for pulse to start
sed -i '/^#/ ! {/\\$/ ! { /^[[:space:]]*$/ ! s/$/ -jmx-manager=true -jmx-manager-start=true/}}' "${SNAPPY_HOME_DIR}/conf/locators"


if [[ -e leads ]]; then
  mv leads "${SNAPPY_HOME_DIR}/conf/"
else
  cp lead_list "${SNAPPY_HOME_DIR}/conf/leads"
fi

if [[ "${ZEPPELIN_HOST}" != "zeppelin_server" ]]; then
  # Enable interpreter on lead
  sed -i '/^#/ ! {/\\$/ ! { /^[[:space:]]*$/ ! s/$/ -zeppelin.interpreter.enable=true/}}' "${SNAPPY_HOME_DIR}/conf/leads"

  # Add interpreter jar to snappydata's jars directory
  # TODO Download this from official-github-release. See fetch-distribution.sh:getLatestUrl() on how we can get the latest url.
  INTERPRETER_JAR="snappydata-zeppelin-0.6.jar"
  INTERPRETER_URL="https://github.com/SnappyDataInc/zeppelin-interpreter/releases/download/v0.6/${INTERPRETER_JAR}"
  wget -q "${INTERPRETER_URL}"
  mv ${INTERPRETER_JAR} ${SNAPPY_HOME_DIR}/jars/
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
sh copy-dir.sh resolve-hostname.sh  other-locators

sh copy-dir.sh "${SNAPPY_HOME_DIR}"  lead_list
sh copy-dir.sh resolve-hostname.sh  lead_list

sh copy-dir.sh "${SNAPPY_HOME_DIR}"  server_list
sh copy-dir.sh resolve-hostname.sh  server_list

DIR=`readlink -f resolve-hostname.sh`
DIR=`echo "$DIR"|sed 's@/$@@'`
DIR=`dirname "$DIR"`

for node in ${OTHER_LOCATORS}; do
    ssh "$node" "sh ${DIR}/resolve-hostname.sh"
done
for node in ${LEADS}; do
    ssh "$node" "sh ${DIR}/resolve-hostname.sh"
done
for node in ${SERVERS}; do
    ssh "$node" "sh ${DIR}/resolve-hostname.sh"
done

# Launch the SnappyData cluster
sh "${SNAPPY_HOME_DIR}/sbin/snappy-start-all.sh"

# Setup and launch zeppelin, if configured.
if [[ "${ZEPPELIN_HOST}" != "zeppelin_server" ]]; then
  if [[ "${ZEPPELIN_MODE}" = "NON-EMBEDDED" ]]; then
    sh copy-dir.sh "${SNAPPY_HOME_DIR}" zeppelin_server
  fi
  for server in "$ZEPPELIN_HOST"; do
    ssh "$server" -o StrictHostKeyChecking=no "mkdir -p ~/snappydata"
    scp -q -o StrictHostKeyChecking=no ec2-variables.sh "${server}:~/snappydata"
    scp -q -o StrictHostKeyChecking=no zeppelin-setup.sh "${server}:~/snappydata"
    scp -q -o StrictHostKeyChecking=no fetch-distribution.sh "${server}:~/snappydata"
  done
  ssh "$ZEPPELIN_HOST" -t -t -o StrictHostKeyChecking=no "sh ${DIR}/zeppelin-setup.sh"
fi

popd > /dev/null
