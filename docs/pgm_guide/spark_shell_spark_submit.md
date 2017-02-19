## Using the Spark Shell and spark-submit

SnappyData, out-of-the-box, collocates Spark executors and the SnappyData store for efficient data intensive computations. 
You however may need to isolate the computational cluster for other reasons. For instance, a  computationally intensive Map-reduce machine learning algorithm that needs to iterate over a cached data set repeatedly.

To support such cases it is also possible to run native Spark jobs that accesses a SnappyData cluster as a storage layer in a parallel fashion. To connect to the SnappyData store the `spark.snappydata.store.locators` property should be provided while starting the Spark-shell. 

To run all SnappyData functionalities you need to create a [SnappySession](http://snappydatainc.github.io/snappydata/apidocs/#org.apache.spark.sql.SnappySession).

```bash
// from the SnappyData base directory  
# Start the Spark shell in local mode. Pass SnappyData's locators host:port as a conf parameter.
# Change the UI port because the default port 4040 is being used by Snappy’s lead. 
$ bin/spark-shell  --master local[*] --conf spark.snappydata.store.locators=locatorhost:port --conf spark.ui.port=4041
scala>
#Try few commands on the spark-shell. Following command shows the tables created using the snappy-shell
scala> val snappy = new org.apache.spark.sql.SnappySession(spark.sparkContext)
scala> val airlineDF = snappy.table("airline").show
scala> val resultset = snappy.sql("select * from airline")
```

Any Spark application can also use the SnappyData as store and Spark as a computational engine by providing an extra `spark.snappydata.store.locators` property in the conf.

```bash
# Start the Spark standalone cluster from SnappyData base directory 
$ sbin/start-all.sh 
# Submit AirlineDataSparkApp to Spark Cluster with snappydata's locator host port.
$ bin/spark-submit --class io.snappydata.examples.AirlineDataSparkApp --master spark://masterhost:7077 --conf spark.snappydata.store.locators=locatorhost:port --conf spark.ui.port=4041 $SNAPPY_HOME/examples/jars/quickstart.jar

# The results can be seen on the command line.
```
