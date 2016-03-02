source PerfRun.conf

export APP_PROPS="dataLocation=$dataDir"

echo "******************start Creating Table******************"
. $SnappyData/build-artifacts/scala-2.10/snappy/bin/snappy-job.sh submit --lead localhost:8090 --app-name myapp --class io.snappydata.snappy.TPCH_Snappy_Tables --app-jar $TPCHJar

#. $SnappyData/build-artifacts/scala-2.10/snappy/bin/snappy-job.sh submit --lead localhost:8090 --app-name myapp --class io.snappydata.cluster.Cluster_TPCH_Snappy_Tables --app-jar $TPCHJar
