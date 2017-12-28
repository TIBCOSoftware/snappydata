#
# Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

# check for AWS and set SPARK_PUBLIC_DNS (only supported for Linux)
if [ -z "$SPARK_PUBLIC_DNS" ]; then
  CHECK_AWS=1
  if [ -r /sys/hypervisor/uuid ]; then
    if ! grep -q '^ec2' /sys/hypervisor/uuid; then
      CHECK_AWS=
    fi
  elif [ -r /sys/devices/virtual/dmi/id/product_name ]; then
    if ! grep -iq 'hvm' /sys/devices/virtual/dmi/id/product_name; then
      CHECK_AWS=
    fi
  else
    # not running on AWS if neither of those two files are present
    CHECK_AWS=
  fi
  if [ -n "$CHECK_AWS" ]; then
    SPARK_PUBLIC_DNS="$(curl -s --connect-timeout 3 http://169.254.169.254/latest/meta-data/public-hostname)"
    if [ -n "$SPARK_PUBLIC_DNS" ]; then
      SPARK_IS_AWS_INSTANCE=1
      export SPARK_PUBLIC_DNS SPARK_IS_AWS_INSTANCE
    fi
  fi
fi
