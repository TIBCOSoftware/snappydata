#!/usr/bin/env bash

usage="Usage: replace-txt.sh < -d|--dir srcfolder > < -t|--text text > < -r|--replace replacement text > [ -e|--extension file_extension ]"

file_extensions=(.scala .java .sh .gradle .h .cpp .py .xml .thrift .tmpl .properties)

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
  -t|--text)
  TEXT="$2"
  shift # past argument
  ;;
  -r|--replace)
  REPLACE_TEXT="$2"
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

if [ ! -d ${SOURCEPATH} ]; then
  echo Directory ${SOURCEPATH} does not exists
  exit 1
fi

echo FILE EXTENSION  = "${file_extensions[@]}"
echo SOURCE PATH     = "${SOURCEPATH}"

red='\e[0;31m'
bluebg='\e[0;44m'
NC='\e[0m' # No Color

for ext in "${file_extensions[@]}"
do
  echo
  echo EXTENSION = ${ext}
  echo "---------------------------------------"
  sleep 5
  for f in `find ${SOURCEPATH} -name "*${ext}"`
  do
    TEXT_EXISTS=`grep -n "${TEXT}" ${f}`
    #echo SNAPPY_PATTERN = ${SNAPPY_PATTERN}
    #echo SNAPPY_HDR_EXISTS= ${SNAPPY_HDR_EXISTS}
    if [ ! -z "${TEXT_EXISTS}" ]; then
      echo -e "${bluebg}Replacing text in file ${f} ${NC}"
      sed -i -e "s/${TEXT}/${REPLACE_TEXT}/" ${f}
    else 
      echo -e "${red}Text to be replaced does not exists in file ${f}"
    fi
  done
done
echo
echo BYE BYE !!!
