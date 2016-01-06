#!/usr/bin/env sh
#set -vx

# This script helps to download and
# untar the uploaded airline parquet data.


if [ $# -eq 0 ]; then
    echo "ERROR: incorrect argument specified: " "$@"
    echo "Usage:./download_full_airlinedata.sh <destination_folder>"
    exit 1
fi

FILEID=0Bz26LQmzyZJHLTItWkp5THJZbHc
source=https://googledrive.com/host/$FILEID

# Download the parquet data from source to destination
 wget -P $1 $source

 #untar the downloaded file.
 destFile=$1/airlineParquetData_2007-15.tar.gz
 mv $1/0Bz26LQmzyZJHLTItWkp5THJZbHc $destFile
 cd $1
 tar -zxf $destFile

