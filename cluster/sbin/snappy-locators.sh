#!/usr/bin/env bash

#
# Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

# Starts a locator instance on each machine specified in the conf/locators file.

function absPath() {
  perl -MCwd -le 'print Cwd::abs_path(shift)' "$1"
}
sbin="$(dirname "$(absPath "$0")")"

. "$sbin/snappy-config.sh"
. "$sbin/spark-config.sh"


. "$SNAPPY_HOME/bin/load-spark-env.sh"
. "$SNAPPY_HOME/bin/load-snappy-env.sh"

CONF_DIR_OPT=
# Check if --config is passed as an argument. It is an optional parameter.
if [ "$1" == "--config" ]
then
  CONF_DIR=$2
  CONF_DIR_OPT="--config $CONF_DIR"
  shift 2
fi

# Launch the slaves
if echo $@ | grep -qw start; then
  "$sbin/snappy-nodes.sh" locator $CONF_DIR_OPT cd "$SNAPPY_HOME" \; "$sbin/snappy-locator.sh" "$@" $LOCATOR_STARTUP_OPTIONS $ENCRYPT_PASSWORD_OPTIONS
else
  "$sbin/snappy-nodes.sh" locator $CONF_DIR_OPT cd "$SNAPPY_HOME" \; "$sbin/snappy-locator.sh" "$@"
fi
