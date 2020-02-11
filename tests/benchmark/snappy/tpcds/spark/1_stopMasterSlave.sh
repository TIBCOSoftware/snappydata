#!/usr/bin/env bash
source PerfRun.conf

#top master and slaves from master machines
ssh $master bash $sparkHome/sbin/stop-all.sh

rm -rf $sparkHome/conf/slaves

#echo "*****************kill java on lead**********************"
#ssh $client killall -9 java
#echo "*****************kill java on locator**********************"
#ssh $master killall -9 java
#echo "*****************kill java on server***********************"

#for element in "${slaves[@]}";
#  do
#	ssh $element killall -9 java
#  done
