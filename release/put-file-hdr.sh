#!/usr/bin/env bash

usage="Usage: put-file-hdr.sh < -d|--dir srcfolder > < -h|--header hdrfile > [ -e|--extension file_extension ]"

file_extensions=(.scala .java)

while [[ $# > 1 ]]
do
key="$1"

case $key in
  -e|--extension)
  EXTENSION="$2"
  if [ $EXTENSION != ".scala" -a $EXTENSION != ".java" ]; then
    file_extensions+=($EXTENSION)
  fi
  shift # past argument
  ;;
  -d|--dir)
  SOURCEPATH="$2"
  shift # past argument
  ;;
  -h|--header)
  HEADER="$2"
  shift # past argument
  ;;
  -S|--snappypattern)
  SNAPPY_PATTERN="$2"
  shift # past argument
  ;;
  *)
  # unknown option
  echo $usage
  exit 1
  ;;
esac
shift # past argument or value
done

if [ ! -f ${HEADER} ]; then
  echo Header File ${HEADER} does not exists
  exit 1
fi

if [ ! -d ${SOURCEPATH} ]; then
  echo Directory ${SOURCEPATH} does not exists
  exit 1
fi

echo FILE EXTENSION  = "${file_extensions[@]}"
echo SOURCE PATH     = "${SOURCEPATH}"
echo HEADER FILE     = "${HEADER}"
echo SNAPPY PATTERN  = "${SNAPPY_PATTERN}"

if [ ! -d ${SOURCEPATH} ]; then
  echo ${SOURCEPATH} " does not exists "
  exit 1
fi

red='\e[0;31m'
bluebg='\e[0;44m'
NC='\e[0m' # No Color

for ext in "${file_extensions[@]}"
do
  echo
  echo EXTENSION = ${ext}
  for f in `find ${SOURCEPATH} -name *${ext}`
  do
    SNAPPY_HDR_EXISTS=`grep -n "${SNAPPY_PATTERN}" ${f}`
    #echo SNAPPY_PATTERN = ${SNAPPY_PATTERN}
    #echo SNAPPY_HDR_EXISTS= ${SNAPPY_HDR_EXISTS}
    if [ ! -z "${SNAPPY_HDR_EXISTS}" ]; then
      echo -e "${bluebg}File ${f} already has snappy header${NC}"
      continue
    fi
    echo -e "${red}Putting header in file ${f}"
    sed -i -e "1r${HEADER}" -e "1{h;d}" -e "2{x;G}" ${f}
  done
done
echo
echo BYE BYE !!!
