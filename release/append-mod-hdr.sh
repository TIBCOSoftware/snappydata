#!/usr/bin/env bash

usage="Usage: appendSnappyDataHdr.sh < -d|--dir srcfolder > < -r|--referenceFolder refDir > \
        < -h|--header snappy_hdrfile > < -f|--from file > < -l|--lines first_n_lines >"

while [[ $# > 1 ]]
do
key="$1"

case $key in
  -f|--from)
  FROM_FILE="$2"
  shift # past argument
  ;;
  -d|--dir)
  SOURCEPATH="$2"
  shift # past argument
  ;;
  -r|--refdir)
  REFDIRPATH="$2"
  shift # past argument
  ;;
  -h|--header)
  HEADER="$2"
  shift # past argument
  ;;
  -l|--lines)
  FIRST_N_LINES="$2"
  shift # past argument
  ;;
  -p|--pattern)
  PATTERN="$2"
  echo FOUND PATTERN = ${PATTERN}
  shift # past argument
  ;;
  -D|--diff)
  DIFFFILE="$2"
  shift # past argument
  ;;
  -S|--snappypattern)
  SNAPPY_PATTERN="$2"
  shift # past argument
  ;;
  *)
  # unknown option
  echo UNKNOWN OPTION $1
  echo $usage
  exit 1
  ;;
esac
shift # past argument or value
done

if [ ! -f "${HEADER}" ]; then
  echo Header File ${HEADER} does not exists
  echo ${usage}
  exit 1
fi

if [ ! -d "${SOURCEPATH}" ]; then
  echo Directory ${SOURCEPATH} does not exists
  echo ${usage}
  exit 1
fi

if [ ! -d "${REFDIRPATH}" ]; then
  echo Reference Directory ${REFDIRPATH} does not exists
  echo ${usage}
  exit 1
fi

if [ ! -f "${FROM_FILE}" ]; then
  FROM_FILE=`find ${REFDIRPATH} -name ${FROM_FILE}`
  if [ ! -n $FROM_FILE ]; then
    echo Could not find from file
  fi 
  if [ ! -f ${FROM_FILE} ]; then
    echo Did not find any file named ${FROM_FILE} in ${REFDIRPATH}
    echo ${usage}
    exit 1
  fi
fi

if [ ! -n "${FIRST_N_LINES}" ]; then
  echo First n lines parameter not specified
  echo ${usage}
  exit 1
fi

if [ ${FIRST_N_LINES} -le 0 ]; then
  echo Invalid first n lines parameter
  echo ${usage}
  exit 1
fi

echo SOURCE PATH     = "${SOURCEPATH}"
echo REF DIR PATH    = "${REFDIRPATH}"
echo HEADER FILE     = "${HEADER}"
echo FIRST N LINES   = "${FIRST_N_LINES}"
echo FROM_FILE       = "${FROM_FILE}"

TMP_ORIG_HDR_FILE=/tmp/.tmpOrigSparkHdr
TMP_EXTRACTED_HDR_FILE=/tmp/.tmpExtractedSparkHdr
TMP_NO_HDR_LINES_SPARK_FILES=/tmp/.tmpNoHdrSparkFiles
TMP_LESS_LINES_SPARK_FILES=/tmp/.tmpLessLinesThanSparkHdr

rm ${TMP_ORIG_HDR_FILE} 2> /dev/null
rm ${TMP_EXTRACTED_HDR_FILE} 2> /dev/null
rm ${TMP_NO_HDR_LINES_SPARK_FILES} 2> /dev/null
rm ${TMP_LESS_LINES_SPARK_FILES} 2> /dev/null
if [ ! -z ${DIFFFILE} ]; then
  rm ${DIFFFILE} 2> /dev/null
  touch ${DIFFFILE}
fi

touch ${TMP_ORIG_HDR_FILE}
touch ${TMP_EXTRACTED_HDR_FILE}
touch ${TMP_NO_HDR_LINES_SPARK_FILES}
touch ${TMP_LESS_LINES_SPARK_FILES}

if [ ! -f ${TMP_ORIG_HDR_FILE} ]; then
  echo Could not create tmp file
  exit 1
fi

head -n ${FIRST_N_LINES} ${FROM_FILE} > ${TMP_ORIG_HDR_FILE}
#echo Original Spark Header
#echo ==========================================
#cat ${TMP_ORIG_HDR_FILE}
#echo ==========================================

red='\e[0;31m'
green='\e[0;32m'
yellow='\e[0;33m'
blue='\e[0;34m'
bluebg='\e[0;44m'
NC='\e[0m' # No Color

TO_INSERT_AT=`expr ${FIRST_N_LINES} + 1`
echo AT = ${TO_INSERT_AT}

for f in `find ${REFDIRPATH} -name *.scala` # For Testing
#for f in `find ${REFDIRPATH}`
do
#echo ${f} size = ${#f} and refdir prefix length = ${#REFDIRPATH}
  rp=${f:${#REFDIRPATH}:${#f}}
  snappyfile=${SOURCEPATH}/${rp}
  if [ -s ${f} ]; then
    cmp --silent ${f} ${snappyfile}
    cmp_exit_status=$?
    if [ ${cmp_exit_status} -ne 0 ]; then
      SNAPPY_HDR_EXISTS=`grep -n "${SNAPPY_PATTERN}" ${snappyfile}`
      #echo SNAPPY_PATTERN = ${SNAPPY_PATTERN}
      #echo SNAPPY_HDR_EXISTS= ${SNAPPY_HDR_EXISTS}
      if [ ! -z "${SNAPPY_HDR_EXISTS}" ]; then
        echo -e "${bluebg}File ${rp} already has snappy header${NC}"
        continue
      fi
      numLinesInFile=`wc -l ${f} | cut -d' ' -f1`
      if [ ${numLinesInFile} -gt ${FIRST_N_LINES} ]; then
        # extract the header
        head -n ${FIRST_N_LINES} ${f} > ${TMP_EXTRACTED_HDR_FILE}
        cmp --silent ${TMP_ORIG_HDR_FILE} ${TMP_EXTRACTED_HDR_FILE}
        cmp_exit_status=$?
        if [ ${cmp_exit_status} -eq 0 ]; then
          echo -e "${red}File ${rp} has been modified and found expected header${NC}"
          if [ ! -z ${DIFFFILE} ]; then
              git --no-pager diff  ${f} ${snappyfile} >> ${DIFFFILE}
          fi
          sed -i "${TO_INSERT_AT}r ${HEADER}" ${snappyfile}
        else
          # May be the header is there but is a little differently written
          # Saw that in some source file of Apache Spark.
          echo pat = ${PATTERN}
          PATTERN_EXISTS=`grep -n "${PATTERN}" ${TMP_EXTRACTED_HDR_FILE}`
          #cat ${TMP_EXTRACTED_HDR_FILE}
          echo PATTERN EXISTS = ${PATTERN_EXISTS}
          if [ -n "${PATTERN_EXISTS}" ]; then
            echo -e "${yellow}File ${rp} has been modified but has a different header${NC}"
            sed -i "${TO_INSERT_AT}r ${HEADER}" ${snappyfile}
            if [ ! -z ${DIFFFILE} ]; then
              git --no-pager diff  ${f} ${snappyfile} >> ${DIFFFILE}
            fi
            sed -i "${TO_INSERT_AT}r ${HEADER}" ${snappyfile}
          else
            echo -e "${blue}File {rp} is modified but has no license header${NC}"
          fi
        fi
      fi
    else 
      echo -e "${green}File ${rp} is not modified${NC}"
    fi 
  fi 
done

echo
echo Done ... Exiting ... BYE BYE !!!
