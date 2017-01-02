#!/usr/bin/env sh
#set -vx

# This script helps to download and
# untar the uploaded airline parquet data.

if [ $# -eq 0 ]; then
    echo "ERROR: incorrect argument specified: " "$@"
    echo "Usage:./download_full_airlinedata.sh <destination_folder>"
    exit 1
fi

#FILEID=0Bz26LQmzyZJHZ2FXUVVub3h3SWc
#source=https://googledrive.com/host/$FILEID

# Download the parquet data from source to destination
fileName=airlineParquetData_2007-15.tar.gz
destFile="$1/${fileName}"
mkdir -p "$1"
#curl -L -o "${destFile}" $source
wget -O "$destFile" https://s3-us-west-2.amazonaws.com/zeppelindemo/data/$fileName

#untar the downloaded file.
cd "$1"
tar -zxf "${fileName}"
