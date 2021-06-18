#!/usr/bin/env bash

file_extensions=".scala .java .sh .gradle .h .cpp .py .xml .thrift .tmpl .properties"

usage() {
  echo "Usage: replace-txt.sh < -d|--dir srcfolder > < -t|--text perl regex (multiline matching) > < -r|--replace replacement text|-f|--replace-file replacement text from file > [ -e|--extension file_extensions ]"
  echo
  echo "For example to update license headers: replace-text.sh -d <dir> -t '\/\*.*?\* Copyright [^ ]* 20..-20.. TIBCO Software Inc. All rights reserved..*?\*\/' -f filehdr.txt"
  echo
}

while [[ $# > 1 ]]
do
key="$1"

case $key in
  -e|--extension)
  file_extensions="$2"
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
  -f|--replace-file)
  REPLACE_TEXT="`cat "$2" | sed 's,/,\\\/,g'`"
  shift # past argument
  ;;
  *)
  # unknown option
  usage
  exit 1
  ;;
esac
shift # past argument or value
done

if [ -z "${TEXT}" ]; then
  echo Text to be replaced is empty
  echo
  usage
  exit 1
fi

if [ ! -d "${SOURCEPATH}" ]; then
  echo Directory ${SOURCEPATH} does not exists
  echo
  usage
  exit 1
fi

echo FILE EXTENSIONS  = "$file_extensions"
echo SOURCE PATH     = "${SOURCEPATH}"

red='\e[0;31m'
bluebg='\e[0;44m'
NC='\e[0m' # No Color

for ext in $file_extensions
do
  echo
  echo EXTENSION = ${ext}
  echo "---------------------------------------"
  for f in `find ${SOURCEPATH} -name "*${ext}"`
  do
    #TEXT_EXISTS=`grep -n "${TEXT}" ${f}`
    #echo SNAPPY_PATTERN = ${SNAPPY_PATTERN}
    #echo SNAPPY_HDR_EXISTS= ${SNAPPY_HDR_EXISTS}
    #if [ ! -z "${TEXT_EXISTS}" ]; then
      echo -e "${bluebg}Replacing text in file ${f} ${NC}"
      perl -pi -e "BEGIN{undef $/;} s/${TEXT}/${REPLACE_TEXT}/smg" "$f"
    #else
    #  echo -e "${red}Text to be replaced does not exists in file ${f}"
    #fi
  done
done
echo
echo BYE BYE !!!
