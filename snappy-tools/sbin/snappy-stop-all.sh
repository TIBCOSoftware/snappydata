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

# Stops all snappy daemons - locator, lead and server on the nodes specified in the
# conf/locators, conf/leads and conf/servers files repsectively

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

# Load the Spark configuration

. "$sbin/spark-config.sh"
. "$sbin/snappy-config.sh"


# Stop Leads
"$sbin"/snappy-leads.sh stop

# Stop Servers
"$sbin"/snappy-servers.sh stop

# Stop locators
"$sbin"/snappy-locators.sh stop



