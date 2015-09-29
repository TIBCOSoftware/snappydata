#!/bin/sh
#
# Create the snappy-spark symlink in top-level snappy-commons directory.
#

# Get the parent base directory of this script
scriptdir="`dirname "$0"`"

realpath() {
  ( cd "$1" && pwd )
}

basedir="`realpath "${scriptdir}/.."`"
destdir="${basedir}/snappy-spark"

if [ ! -d "${destdir}" ]; then
  [ -e "${destdir}" ] && echo "${destdir} exists but is not a directory" && exit 1

  sspdir="${basedir}/../snappy-spark"
  # Search for snappy-spark first in SPARK_HOME
  if [ -n "${SPARK_HOME}" -a -d "${SPARK_HOME}" ]; then
    if [ "`realpath "${SPARK_HOME}"`" != "${destdir}" ]; then
      cp -a "${SPARK_HOME}" "${basedir}"
    fi
  # Then one level up
  elif [ -d "${sspdir}" ]; then
    sspdir="`realpath "${sspdir}"`"
    cp -a "${sspdir}" "${destdir}"
  else
    echo "Failed to find ${sspdir}. Either set SPARK_HOME to its location or place it in the same directory as snappy-commons."
    exit 1
  fi
fi

exit 0
