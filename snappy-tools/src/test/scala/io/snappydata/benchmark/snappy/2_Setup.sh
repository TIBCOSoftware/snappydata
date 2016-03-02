source PerfRun.conf

rm -rf $SnappyData/build-artifacts/scala-2.10/snappy/work/*

rm -rf $SnappyData/build-artifacts/scala-2.10/snappy/conf/leads 
rm -rf $SnappyData/build-artifacts/scala-2.10/snappy/conf/locators 
rm -rf $SnappyData/build-artifacts/scala-2.10/snappy/conf/servers

cat > $SnappyData/build-artifacts/scala-2.10/snappy/conf/leads << EOF
$leads -locators=$locator:10334 $sparkProperties
EOF
echo "******************Created conf/leads*********************"

cat > $SnappyData/build-artifacts/scala-2.10/snappy/conf/locators << EOF
$locator -client-bind-address=$locator
EOF
echo "******************Created conf/locators******************"

for element in "${servers[@]}"; 
  do 
	echo $element -locators=$locator:10334 $serverMemory >> $SnappyData/build-artifacts/scala-2.10/snappy/conf/servers 
  done
echo "******************Created conf/servers******************"



echo "******************start locators******************"
sh $SnappyData/build-artifacts/scala-2.10/snappy/sbin/snappy-locators.sh start -classpath=$TPCHJar

echo "******************start servers******************"
sh $SnappyData/build-artifacts/scala-2.10/snappy/sbin/snappy-servers.sh start -classpath=$TPCHJar

echo "******************start leads******************"
sh $SnappyData/build-artifacts/scala-2.10/snappy/sbin/snappy-leads.sh start -classpath=$TPCHJar



