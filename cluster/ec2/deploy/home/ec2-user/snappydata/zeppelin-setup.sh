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

# TODO Remove the hardcoding wherever possible.

pushd /home/ec2-user/snappydata > /dev/null

source /home/ec2-user/snappydata/ec2-variables.sh

which jar

JAR_STATUS=`echo $?`

if [[ ${JAR_STATUS} -ne 0 ]]; then
  # if ! yum -q list installed  java-1.7.0-openjdk-devel &>/dev/null; then
  echo "Installing openjdk 1.7 ..."
  sudo yum -y install java-1.7.0-openjdk-devel
fi

ZEP_DIR="zeppelin-0.6.1-bin-netinst"

# Download and extract Zeppelin distribution
if [[ ! -d ${ZEP_DIR} ]]; then
  if [[ ! -e "${ZEP_DIR}.tgz" ]]; then
    wget "http://mirror.fibergrid.in/apache/zeppelin/zeppelin-0.6.1/${ZEP_DIR}.tgz"
  fi
  tar -xf zeppelin-0.6.1-bin-netinst.tgz
fi

# At this point, SnappyData dist should be available (placed here by lead)
if [[ ! -d ${SNAPPY_HOME_DIR} ]]; then
  sh fetch-distribution.sh
fi

# Download, extract and place SnappyData interpreter under interpreter/ directory
# TODO Later we need to download this from maven/official-github.
INTERPRETER_URL="https://github.com/SnappyDataInc/snappy-poc/releases/download/v0.5.1/snappydata-zeppelin-interpreter-0.5.2-SNAPSHOT.jar"
INTERPRETER_DIR="${ZEP_DIR}/interpreter/snappydata"

if [[ ! -d ${INTERPRETER_DIR} ]]; then
  wget -q "${INTERPRETER_URL}"
  # tar -xf snappydata-interpreter.tar.gz
  mkdir "${INTERPRETER_DIR}"
  mv snappydata-zeppelin-interpreter-0.5.2-SNAPSHOT.jar "${INTERPRETER_DIR}"
  jar -xf "${INTERPRETER_DIR}/snappydata-zeppelin-interpreter-0.5.2-SNAPSHOT.jar" interpreter-setting.json
  mv interpreter-setting.json "${INTERPRETER_DIR}"

  # Place interpreter dependencies into the directory
  cp "${SNAPPY_HOME_DIR}/lib/datanucleus-api-jdo-3.2.6.jar" "${INTERPRETER_DIR}"
  cp "${SNAPPY_HOME_DIR}/lib/datanucleus-core-3.2.10.jar" "${INTERPRETER_DIR}"
  cp "${SNAPPY_HOME_DIR}/lib/datanucleus-rdbms-3.2.9.jar" "${INTERPRETER_DIR}"
  cp "${SNAPPY_HOME_DIR}/lib/snappydata-assembly_2.10-0.5.2-SNAPSHOT-hadoop2.4.1.jar" "${INTERPRETER_DIR}"
  cp "${SNAPPY_HOME_DIR}/lib/snappydata-store-client-1.5.1-SNAPSHOT.jar" "${INTERPRETER_DIR}"
fi


# Modify conf/zeppelin-site.xml
if [[ ! -e "${ZEP_DIR}/conf/zeppelin-site.xml" ]]; then
  cp "${ZEP_DIR}/conf/zeppelin-site.xml.template" "${ZEP_DIR}/conf/zeppelin-site.xml"
  SEARCH_STRING="<name>zeppelin.interpreters<\/name>"
  INSERT_STRING="org.apache.zeppelin.interpreter.SnappyDataZeppelinInterpreter,org.apache.zeppelin.interpreter.SnappyDataSqlZeppelinInterpreter,"
  # sed -i "/${SEARCH_STRING}/{n;s/<\/value>/${INSERT_STRING}<\/value>/}" "${ZEP_DIR}/conf/zeppelin-site.xml"
  sed -i "/${SEARCH_STRING}/{n;s/<value>/<value>${INSERT_STRING}/}" "${ZEP_DIR}/conf/zeppelin-site.xml"
fi

# Modify interpreter/snappydata/interpreter-setting.json
# TODO Pass the port, if modified by user.
echo "${LOCATORS}" > locator_list
FIRST_LOCATOR=`cat locator_list | sed -n '1p'`
SEARCH_STRING="jdbc:snappydata:\/\/localhost:1527\/"
INSERT_STRING="jdbc:snappydata:\/\/${FIRST_LOCATOR}:1527\/"
sed -i "s/${SEARCH_STRING}/${INSERT_STRING}/" "${INTERPRETER_DIR}/interpreter-setting.json"


# Ensure conf/interpreter.json exists (generated after zeppelin is started for the first time)
if [[ -e "${ZEP_DIR}/conf/interpreter.json.orig" ]]; then
  cp "${ZEP_DIR}/conf/interpreter.json.orig" "${ZEP_DIR}/conf/interpreter.json"
else # if [[ ! -e "${ZEP_DIR}/conf/interpreter.json" ]]; then
  # Start and stop the Zeppelin daemon
  sh "${ZEP_DIR}/bin/zeppelin-daemon.sh" start
  sleep 2
  sh "${ZEP_DIR}/bin/zeppelin-daemon.sh" stop
  if [[ ! -e "${ZEP_DIR}/conf/interpreter.json" ]]; then
    echo "The file interpreter.json was not generated."
  else
    cp "${ZEP_DIR}/conf/interpreter.json" "${ZEP_DIR}/conf/interpreter.json.orig"
  fi
fi

# Modify conf/interpreter.json
if [[ -e "${ZEP_DIR}/conf/interpreter.json" ]]; then
  LEAD_HOST="localhost"
  LEAD_PORT="3768"
  if [[ ${ZEPPELIN_MODE} != "EMBEDDED" ]]; then
    echo "${LEADS}" > lead_list
    LEAD_HOST=`cat lead_list | sed -n '1p'`
  fi
  sed -i "/group\": \"snappydata\"/,/isExistingProcess\": false/{s/isExistingProcess\": false/isExistingProcess\": snappydatainc_marker,/}" "${ZEP_DIR}/conf/interpreter.json"
  sed -i "/snappydatainc_marker/a \"host\": \"${LEAD_HOST}\",\
         \"port\": \"${LEAD_PORT}\"" "${ZEP_DIR}/conf/interpreter.json"
  sed -i "s/snappydatainc_marker/true/" "${ZEP_DIR}/conf/interpreter.json"
fi

sh "${ZEP_DIR}/bin/zeppelin-daemon.sh" start
sleep 3

popd > /dev/null
