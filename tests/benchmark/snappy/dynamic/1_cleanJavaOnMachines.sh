#!/usr/bin/env bash
source PerfRun.conf

echo "*****************Stop locator, server, lead***********************"
sh $SnappyData/sbin/snappy-stop-all.sh

rm -rf $SnappyData/work/*
rm -rf $SnappyData/conf/leads
rm -rf $SnappyData/conf/locators
rm -rf $SnappyData/conf/servers

#ssh $leads killall -9 vmstat
#for element in "${servers[@]}";
#  do
#   ssh $element killall -9 vmstat
#done
echo "removing directory lead locator and servers"
ssh $leads rm -rf $leadDir
ssh $locator rm -rf $locatorDir
COUNTER=1
for element in "${servers[@]}";
  do
   ssh $element rm -rf $serverDir$COUNTER
   COUNTER=$[$COUNTER+1]
done
