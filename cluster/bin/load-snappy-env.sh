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

# This script loads spark-env.sh if it exists, and ensures it is only loaded once.
# spark-env.sh is loaded from SPARK_CONF_DIR if set, or within the current directory's
# conf/ subdirectory.

function absPath() {
  perl -MCwd -le 'print Cwd::abs_path(shift)' "$1"
}
FWDIR="$(dirname "$(absPath "$0")")"

if [ -z "$SNAPPY_ENV_LOADED" ]; then
  export SNAPPY_ENV_LOADED=1

  # Returns the parent of the directory this script lives in.
  parent_dir="`absPath "$FWDIR/.."`"

  if [ -z "$MALLOC_ARENA_MAX" ]; then
    export MALLOC_ARENA_MAX=4
  fi

  if [ -z "$MALLOC_MMAP_THRESHOLD_" ]; then
    export MALLOC_MMAP_THRESHOLD_=131072
  fi

  if [ -z "$MALLOC_MMAP_MAX_" ]; then
    export MALLOC_MMAP_MAX_=2147483647
  fi

  user_conf_dir="${SPARK_CONF_DIR:-"$parent_dir"/conf}"

  if [ -f "${user_conf_dir}/snappy-env.sh" ]; then
    # Promote all variable declarations to environment (exported) variables
    set -a
    . "${user_conf_dir}/snappy-env.sh"
    set +a
  fi

fi

# Setting SPARK_SCALA_VERSION if not already set.

if [ -z "$SPARK_SCALA_VERSION" ]; then

    ASSEMBLY_DIR2="$FWDIR/assembly/target/scala-2.11"
    ASSEMBLY_DIR1="$FWDIR/assembly/target/scala-2.10"

    if [[ -d "$ASSEMBLY_DIR2" && -d "$ASSEMBLY_DIR1" ]]; then
        echo -e "Presence of build for both scala versions(SCALA 2.10 and SCALA 2.11) detected." 1>&2
        echo -e 'Either clean one of them or, export SPARK_SCALA_VERSION=2.11 in spark-env.sh.' 1>&2
        exit 1
    fi

    if [ -d "$ASSEMBLY_DIR2" ]; then
        export SPARK_SCALA_VERSION="2.11"
    else
        export SPARK_SCALA_VERSION="2.10"
    fi
fi
