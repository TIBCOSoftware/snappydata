#!/usr/bin/env bash
source PerfRun.conf

ssh $leads killall -9 vmstat
for element in "${servers[@]}";
  do
   ssh $element killall -9 vmstat
done

ssh $leads "nohup vmstat -n 1 3000 >> $leadDir/Snappy_Vmstat_Query_Lead.out &"
for element in "${servers[@]}";
  do
   ssh $element "nohup vmstat -n 1 3000 >> $serverDir/Snappy_Vmstat_Query_$element.out &"
done



echo "******************start Executing Query******************"
. $SnappyData/bin/snappy-job.sh submit --lead $leads:8090 --app-name QueryExecution --class io.snappydata.benchmark.snappy.TPCH_Snappy_Query --app-jar $TPCHJar --conf queries=$queries --conf sparkSqlProps=$sparkSqlProperties --conf useIndex=$UseIndex --conf resultCollection=$ResultCollection --conf warmUpIterations=$WarmupRuns --conf actualRuns=$AverageRuns

#. $SnappyData/bin/snappy-job.sh submit --lead localhost:8090 --app-name myapp --class io.snappydata.cluster.Cluster_TPCH_Snappy_Query --app-jar $TPCHJar

