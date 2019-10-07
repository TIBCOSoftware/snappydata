#!/usr/bin/env bash
source PerfRun.conf

rm -rf $SnappyData/work/*

rm -rf $SnappyData/conf/leads
rm -rf $SnappyData/conf/locators
rm -rf $SnappyData/conf/servers

cat > $SnappyData/conf/leads << EOF
$leads -locators=$locator:10334 $sparkProperties
EOF
echo "******************Created conf/leads*********************"

cat > $SnappyData/conf/locators << EOF
$locator -client-bind-address=$locator
EOF
echo "******************Created conf/locators******************"

for element in "${servers[@]}"; 
  do 
	echo $element -locators=$locator:10334 $serverMemory >> $SnappyData/conf/servers
  done
echo "******************Created conf/servers******************"



echo "******************start locators******************"
sh $SnappyData/sbin/snappy-locators.sh start

echo "******************start servers******************"
sh $SnappyData/sbin/snappy-servers.sh start

echo "******************start leads******************"
sh $SnappyData/sbin/snappy-leads.sh start



