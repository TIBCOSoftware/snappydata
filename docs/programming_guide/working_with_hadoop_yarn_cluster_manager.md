# Working with Hadoop YARN Cluster Manager

The TIBCO ComputeDB embedded cluster uses its own cluster manager and as such cannot be managed using the YARN cluster manager. However, you can start the Spark cluster with the YARN cluster manager, which can interact with the TIBCO ComputeDB cluster in the [Smart Connector Mode](../affinity_modes/connector_mode.md).

!!! Note
	We assume that Apache Hadoop and YARN are already installed, and you want to bring in TIBCO ComputeDB cluster to work with YARN.

You need to set the following environment variables:

```pre
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop

```
## Launching spark-shell with YARN 

Start a TIBCO ComputeDB default cluster using the `./sbin/snappy-start-all.sh` command

To run TIBCO ComputeDB quickstart example using YARN, do the following:

```pre
./bin/spark-shell --master yarn  --conf spark.snappydata.connection=localhost:1527 --conf spark.ui.port=4041 -i $SNAPPY_HOME/quickstart/scripts/Quickstart.scala
```

!!! Note
	YARN is mentioned as a master url.
    
## Submitting spark-jobs using YARN

1. Create the required tables in the TIBCO ComputeDB cluster

		./bin/snappy-job.sh submit --lead localhost:8090 --app-name CreateAndLoadAirlineDataJob --class io.snappydata.examples.CreateAndLoadAirlineDataJob --app-jar $SNAPPY_HOME/examples/jars/quickstart.jar

2. Run queries on the tables created from CreateAndLoadAirlineDataJob.

		./bin/spark-submit --class io.snappydata.examples.AirlineDataSparkApp --master yarn --conf spark.snappydata.connection=localhost:1527 --conf spark.ui.port=4041 $SNAPPY_HOME/examples/jars/quickstart.jar

