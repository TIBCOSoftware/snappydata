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

which jar > /dev/null

JAR_STATUS=`echo $?`

if [[ ${JAR_STATUS} -ne 0 ]]; then
  echo "Installing openjdk 1.7 ..."
  sudo yum -y install java-1.7.0-openjdk-devel
fi

echo "${LOCATORS}" > locator_list
FIRST_LOCATOR=`cat locator_list | sed -n '1p'`
echo "${LEADS}" > lead_list
FIRST_LEAD=`cat lead_list | sed -n '1p'`

ZEP_DIR="zeppelin-0.6.1-bin-netinst"
# ZEP_URL="http://mirror.fibergrid.in/apache/zeppelin/zeppelin-0.6.1/${ZEP_DIR}.tgz"
ZEP_URL_MIRROR="http://redrockdigimark.com/apachemirror/zeppelin/zeppelin-0.6.1/${ZEP_DIR}.tgz"
ZEP_NOTEBOOKS_URL="https://github.com/SnappyDataInc/zeppelin-interpreter/raw/notes/examples/notebook"
ZEP_NOTEBOOKS_DIR="notebook"

# Download and extract Zeppelin distribution
if [[ ! -d ${ZEP_DIR} ]]; then
  if [[ ! -e "${ZEP_DIR}.tgz" ]]; then
    echo "Downloading Apache Zeppelin distribution..."
    wget -q "${ZEP_URL_MIRROR}"
    # Try the same mirror again. Zeppelin website has removed fibergrid mirror.
    # TODO while loop
    if [ $? -ne 0 ]; then
      rm "${ZEP_DIR}.tgz"
      wget -q "${ZEP_URL_MIRROR}"
    fi
    
  fi
  tar -xf "${ZEP_DIR}.tgz"
fi

# Download examples notebook from the github
if [[ ! -e "${ZEP_NOTEBOOKS_DIR}.tar.gz" ]]; then
  echo "Downloading sample notebooks..."
  wget -q "${ZEP_NOTEBOOKS_URL}/${ZEP_NOTEBOOKS_DIR}.tar.gz"
fi
tar -xzf "${ZEP_NOTEBOOKS_DIR}.tar.gz"

find ${ZEP_NOTEBOOKS_DIR} -type f -print0 | xargs -0 sed -i "s/localhost:4040/${FIRST_LEAD}:4040/g"
find ${ZEP_NOTEBOOKS_DIR} -type f -print0 | xargs -0 sed -i "s/localhost:7070/${FIRST_LOCATOR}:7070/g"

echo "Copying sample notebooks..."
cp -ar "${ZEP_NOTEBOOKS_DIR}/." "${ZEP_DIR}/${ZEP_NOTEBOOKS_DIR}/"

# At this point, SnappyData dist should be available (placed here by lead)
if [[ ! -d ${SNAPPY_HOME_DIR} ]]; then
  sh fetch-distribution.sh
fi

# Download, extract and place SnappyData interpreter under interpreter/ directory
# TODO See fetch-distribution.sh:getLatestUrl() on how we can get the latest url.
INTERPRETER_JAR="snappydata-zeppelin-0.6.jar"
INTERPRETER_URL="https://github.com/SnappyDataInc/zeppelin-interpreter/releases/download/v0.6/${INTERPRETER_JAR}"
INTERPRETER_DIR="${ZEP_DIR}/interpreter/snappydata"

if [[ ! -d ${INTERPRETER_DIR} ]] || [[ ! -e interpreter-setting.json.orig ]]; then
  mkdir -p "${INTERPRETER_DIR}"
  if [[ ! -e ${INTERPRETER_JAR} ]]; then
    echo "Downloading SnappyData interpreter jar..."
    wget -q "${INTERPRETER_URL}"
    mv "${INTERPRETER_JAR}" "${INTERPRETER_DIR}"
  fi
  jar -xf "${INTERPRETER_DIR}/${INTERPRETER_JAR}" interpreter-setting.json
  mv interpreter-setting.json interpreter-setting.json.orig

  # Place interpreter dependencies into the directory
  cp -a "${SNAPPY_HOME_DIR}/jars/." "${INTERPRETER_DIR}"
fi

cp interpreter-setting.json.orig "${INTERPRETER_DIR}"/interpreter-setting.json


# Modify conf/zeppelin-site.xml to include classnames of snappydata interpreters.
if [[ ! -e "${ZEP_DIR}/conf/zeppelin-site.xml" ]]; then
  cp "${ZEP_DIR}/conf/zeppelin-site.xml.template" "${ZEP_DIR}/conf/zeppelin-site.xml"
  SEARCH_STRING="<name>zeppelin.interpreters<\/name>"
  INSERT_STRING="org.apache.zeppelin.interpreter.SnappyDataZeppelinInterpreter,org.apache.zeppelin.interpreter.SnappyDataSqlZeppelinInterpreter,"
  sed -i "/${SEARCH_STRING}/{n;s/<value>/<value>${INSERT_STRING}/}" "${ZEP_DIR}/conf/zeppelin-site.xml"
fi

# Modify interpreter/snappydata/interpreter-setting.json to include locator host and port.
SEARCH_STRING="jdbc:snappydata:\/\/localhost:1527\/"
INSERT_STRING="jdbc:snappydata:\/\/${FIRST_LOCATOR}:${LOCATOR_CLIENT_PORT}\/"
sed -i "s/${SEARCH_STRING}/${INSERT_STRING}/" "${INTERPRETER_DIR}/interpreter-setting.json"
# Add S3 access credentials
if [[ -n "${AWS_ACCESS_KEY_ID}" ]] && [[ -n "${AWS_SECRET_ACCESS_KEY}" ]]; then
  sed -i "/SnappyDataZeppelinInterpreter/,/properties/ s/properties/properties_marker/"  "${INTERPRETER_DIR}/interpreter-setting.json"
  sed -i "/properties_marker/a \
        \"fs.s3a.access.key\": {\n\
            \"envName\": null, \n\
            \"propertyName\": \"fs.s3a.access.key\", \n\
            \"defaultValue\": \"${AWS_ACCESS_KEY_ID}\", \n\
            \"description\": \"S3 ACCESS KEY ID.\" \n\
        }, \n\
        \"fs.s3a.secret.key\": {\n\
            \"envName\": null, \n\
            \"propertyName\": \"fs.s3a.secret.key\", \n\
            \"defaultValue\": \"${AWS_SECRET_ACCESS_KEY}\", \n\
            \"description\": \"S3 SECRET ACCESS KEY.\" \n\
        },"  "${INTERPRETER_DIR}/interpreter-setting.json"
  sed -i "s/properties_marker/properties/"  "${INTERPRETER_DIR}/interpreter-setting.json"
  # fs.s3.awsAccessKeyId
  # fs.s3.awsSecretAccessKey
fi


# Ensure conf/interpreter.json exists (generated after zeppelin is started for the first time)
if [[ -e "${ZEP_DIR}/conf/interpreter.json.orig" ]]; then
  cp "${ZEP_DIR}/conf/interpreter.json.orig" "${ZEP_DIR}/conf/interpreter.json"
  sh "${ZEP_DIR}/bin/zeppelin-daemon.sh" stop
else
  # TODO If user has made any changes to the interpreter config, those may be lost.
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

# Modify conf/interpreter.json to include lead host and port and set isExistingProcess to true.
if [[ -e "${ZEP_DIR}/conf/interpreter.json" ]]; then
  LEAD_HOST="localhost"
  LEAD_PORT="3768"
  if [[ ${ZEPPELIN_MODE} != "EMBEDDED" ]]; then
    echo "${LEADS}" > lead_list
    LEAD_HOST=`cat lead_list | sed -n '1p'`
  fi
  sed -i "/group\": \"snappydata\"/,/isExistingProcess\": false/{s/isExistingProcess\": false/isExistingProcess\": snappydatainc_marker,/}" "${ZEP_DIR}/conf/interpreter.json"
  sed -i "/snappydatainc_marker/a \"host\": \"${LEAD_HOST}\",\n\
         \"port\": \"${LEAD_PORT}\"" "${ZEP_DIR}/conf/interpreter.json"
  sed -i "s/snappydatainc_marker/true/" "${ZEP_DIR}/conf/interpreter.json"
fi

sh "${ZEP_DIR}/bin/zeppelin-daemon.sh" start
sleep 3

popd > /dev/null
