# Using spark-shell and spark-submit

TIBCO ComputeDB, out-of-the-box, colocates Spark executors and the TIBCO ComputeDB store for efficient data intensive computations. 
You, however, may need to isolate the computational cluster for other reasons. For instance, a  computationally intensive Map-reduce machine learning algorithm that needs to iterate over a cached data set repeatedly.
Refer to [TIBCO ComputeDB Smart Connector Mode](../affinity_modes/connector_mode.md#example) for examples.


To support such cases it is also possible to run native Spark jobs that access a TIBCO ComputeDB cluster as a storage layer in a parallel fashion. To connect to the Snappy store the `spark.snappydata.connection` property should be provided while starting the Spark-shell. 

To run all TIBCO ComputeDB functionalities, you need to create a [SnappySession](http://snappydatainc.github.io/snappydata/apidocs/#org.apache.spark.sql.SnappySession).

```pre
// from the TIBCO ComputeDB base directory  
// Start the Spark shell in local mode. Pass TIBCO ComputeDB's locators host:clientPort as a conf parameter.
$ ./bin/spark-shell  --master local[*] --conf spark.snappydata.connection=locatorhost:clientPort --conf spark.ui.port=4041
scala>
 // Try few commands on the spark-shell. Following command shows the tables created using the snappy-sql
scala> val snappy = new org.apache.spark.sql.SnappySession(spark.sparkContext)
scala> val airlineDF = snappy.table("airline").show
scala> val resultset = snappy.sql("select * from airline")
```

Any Spark application can also use the TIBCO ComputeDB as store and Spark as a computational engine by providing the `spark.snappydata.connection` property as mentioned below:

```pre
// Start the Spark standalone cluster from TIBCO ComputeDB base directory
$ ./sbin/start-all.sh 
// Submit AirlineDataSparkApp to Spark Cluster with TIBCO ComputeDB's locator host port.
$ ./bin/spark-submit --class io.snappydata.examples.AirlineDataSparkApp --master spark://masterhost:7077 --conf spark.snappydata.connection=locatorhost:clientPort $SNAPPY_HOME/examples/jars/quickstart.jar

// The results can be seen on the command line.
```

