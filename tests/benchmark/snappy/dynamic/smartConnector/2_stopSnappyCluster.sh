#!/usr/bin/env bash
source PerfRun.conf

#top master and slaves from master machines
#ssh $master sh $SNAPPY_HOME/sbin/snappy-stop-all.sh
$SNAPPY_HOME/sbin/snappy-stop-all.sh

rm -rf $SNAPPY_HOME/conf/leads
rm -rf $SNAPPY_HOME/conf/locators
rm -rf $SNAPPY_HOME/conf/servers

echo "removing directory lead locator and servers"
ssh $leads rm -rf $leadDir
ssh $locator rm -rf $locatorDir
COUNTER=1
for element in "${servers[@]}";
  do
   ssh $element rm -rf $serverDir$COUNTER
   COUNTER=$[$COUNTER+1]
done
