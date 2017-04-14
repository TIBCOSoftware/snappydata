#!/usr/bin/env bash
source PerfRun.conf


ssh $leads mkdir $leadDir
echo "*****************Created dir for lead**********************"
ssh $locator mkdir $locatorDir
echo "*****************Created dir for locator**********************"
COUNTER=1
for element in "${servers[@]}";
  do
        ssh $element mkdir $serverDir$COUNTER
        COUNTER=$[$COUNTER +1]
 done
echo "*****************Created dir for server**********************"

cat > $SnappyData/conf/leads << EOF
$leads -locators=$locator:10334 $sparkProperties -dir=$leadDir
EOF
echo "******************Created conf/leads*********************"

cat > $SnappyData/conf/locators << EOF
$locator -client-bind-address=$locator -dir=$locatorDir
EOF
echo "******************Created conf/locators******************"

COUNTER=1
for element in "${servers[@]}";
  do
        echo $element -locators=$locator:10334 $serverMemory -dir=$serverDir$COUNTER >> $SnappyData/conf/servers
        COUNTER=$[$COUNTER +1]
  done
echo "******************Created conf/servers******************"


#sh $SnappyData/sbin/snappy-start-all.sh start -classpath=$TPCHJar

echo "******************start locators******************"
#sh $SnappyData/sbin/snappy-locators.sh start -classpath=$TPCHJar
sh $SnappyData/sbin/snappy-locators.sh start

echo "******************start servers******************"
sh $SnappyData/sbin/snappy-servers.sh start
#sh $SnappyData/sbin/snappy-servers.sh start -classpath=$TPCHJar

echo "******************start leads******************"
sh $SnappyData/sbin/snappy-leads.sh start
#sh $SnappyData/sbin/snappy-leads.sh start -classpath=$TPCHJar
