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

cat > $SNAPPY_HOME/conf/leads << EOF
$leads -locators=$locator:10334 $leadProperties -dir=$leadDir
EOF
echo "******************Created conf/leads*********************"

cat > $SNAPPY_HOME/conf/locators << EOF
$locator -client-bind-address=$locator $locatorProperties -dir=$locatorDir
EOF
echo "******************Created conf/locators******************"

COUNTER=1
for element in "${servers[@]}";
  do
        echo $element -locators=$locator:10334 $serverProperties -dir=$serverDir$COUNTER >> $SNAPPY_HOME/conf/servers
        COUNTER=$[$COUNTER +1]
  done
echo "******************Created conf/servers******************"


bash $SNAPPY_HOME/sbin/snappy-start-all.sh 
