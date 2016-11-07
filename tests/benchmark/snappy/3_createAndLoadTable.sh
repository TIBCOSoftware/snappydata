#!/usr/bin/env bash
source PerfRun.conf

ssh $leads "nohup vmstat -n 1 1000 >> $leadDir/Snappy_Vmstat_Load_Lead.out &"
for element in "${servers[@]}";
  do
   ssh $element "nohup vmstat -n 1 1000 >> $serverDir/Snappy_Vmstat_Load_$element.out &"
done

echo "******************start Creating Table******************"
. $SnappyData/bin/snappy-job.sh submit --lead $leads:8090 --app-name TableCreation --class io.snappydata.benchmark.snappy.TPCH_Snappy_Tables --app-jar $TPCHJar --conf dataLocation=$dataDir --conf Buckets_Order_Lineitem=$buckets_Order_Lineitem --conf Buckets_Cust_Part_PartSupp=$buckets_Cust_Part_PartSupp --conf useIndex=$UseIndex --conf Buckets_Nation_Region_Supp=$buckets_Nation_Region_Supp --conf Nation_Region_Supp_col=$Nation_Region_Supp_col

#. $SnappyData/bin/snappy-job.sh submit --lead localhost:8090 --app-name myapp --class io.snappydata.cluster.Cluster_TPCH_Snappy_Tables --app-jar $TPCHJar
