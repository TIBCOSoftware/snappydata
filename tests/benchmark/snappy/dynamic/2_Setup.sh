#!/usr/bin/env bash
source PerfRun.conf

ssh $leads mkdir -p $leadDir
echo "*****************Created dir for lead**********************"
ssh $locator mkdir -p $locatorDir
echo "*****************Created dir for locator**********************"
COUNTER=1
for element in "${servers[@]}";
  do
        ssh $element mkdir -p $serverDir$COUNTER
        COUNTER=$[$COUNTER +1]
 done
echo "*****************Created dir for server**********************"

cp PerfRun.conf $leadDir

cat > $SnappyData/conf/leads << EOF
$leads -locators=$locator:10334 -jobserver.waitForInitialization=true $sparkProperties -dir=$leadDir
EOF
echo "******************Created conf/leads*********************"

cat > $SnappyData/conf/locators << EOF
$locator -client-bind-address=$locator $locatorProperties -dir=$locatorDir
EOF
echo "******************Created conf/locators******************"

rm -f $SnappyData/conf/servers
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
sh $SnappyData/sbin/snappy-servers.sh -bg start
#sh $SnappyData/sbin/snappy-servers.sh start -classpath=$TPCHJar

echo "******************start leads******************"
sh $SnappyData/sbin/snappy-leads.sh start
#sh $SnappyData/sbin/snappy-leads.sh start -classpath=$TPCHJar
