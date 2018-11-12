#
# Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

# included in all the snappy scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*

function absPath() {
  perl -MCwd -le 'print Cwd::abs_path(shift)' "$1"
}

if [ -z "$SNAPPY_HOME" ]; then
  if [ -z "$SPARK_HOME" ]; then
    export SPARK_HOME="$(absPath "$(dirname "$(absPath "$0")")/..")"
  fi
  export SNAPPY_HOME="${SPARK_HOME}"
elif [ -z "$SPARK_HOME" ]; then
  export SPARK_HOME="${SNAPPY_HOME}"
fi
