#!/bin/sh
#
# Create the snappy-spark symlink in top-level snappy-commons directory.
#

# Get the parent base directory of this script
scriptdir="`dirname "$0"`"
basedir="`cd "${scriptdir}/.." && pwd`"

if [ ! -h "${basedir}/snappy-spark" ]; then
  [ -e "${basedir}/snappy-spark" ] && echo "${basedir}/snappy-spark exists but is not a symlink" && exit 1

  sspdir="${basedir}/../snappy-spark"
  # Search for snappy-spark first in SPARK_HOME
  if [ -n "${SPARK_HOME}" ]; then
    ln -s "${SPARK_HOME}" "${basedir}"
  # Then one level up
  elif [ -d "${sspdir}" ]; then
    ln -s ../snappy-spark "${basedir}"
  else
    echo "Failed to find ${basedir}/../snappy-spark. Either set SPARK_HOME to its location or place it in the same directory as snappy-commons."
    exit 1
  fi
fi

exit 0
