#!/bin/sh
#
# Create the snappy-spark copy in top-level snappy-commons directory
# and local-repo links in directories.
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

for localRepo in "${basedir}/snappy-core" "${basedir}/snappy-tools"; do
  rm -rf "${localRepo}/local-repo"
  ln -s ../local-repo "${localRepo}/local-repo"
done

exit 0
