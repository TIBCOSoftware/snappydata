# Working with Hadoop YARN Cluster Manager 

We assume that Apache Hadoop and YARN are already installed, and you want to bring in SnappyData cluster to work with YARN.

You need to set, following environment variables -

```
HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop

```
## Launching spark-shell with Yarn - 

Start a SnappyData default cluster using the `./sbin/snappy-start-all.sh` command

If you want to run SnappyData quickstart example using YARN, do the following. 

```
./bin/spark-shell --master yarn  --conf spark.snappydata.connection=localhost:1527 --conf spark.ui.port=4041 -i $SNAPPY_HOME/quickstart/scripts/Quickstart.scala
```

!!!Note:
	YARN is mentioned as a master url.
    
## Submitting spark-jobs using YARN
1. Create the required tables in SnappyData cluster

    ```
    ./bin/snappy-job.sh submit --lead localhost:8090 --app-name CreateAndLoadAirlineDataJob --class io.snappydata.examples.CreateAndLoadAirlineDataJob --app-jar $SNAPPY_HOME/examples/jars/quickstart.jar
    ```
    
2. Run queries on the tables created from CreateAndLoadAirlineDataJob.

    ```
    ./bin/spark-submit --class io.snappydata.examples.AirlineDataSparkApp --master yarn --conf spark.snappydata.connection=localhost:1527 --conf spark.ui.port=4041 $SNAPPY_HOME/examples/jars/quickstart.jar
    ```