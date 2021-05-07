## Using the SnappyData SQL Shell

The SnappyData SQL Shell (_snappy-sql_) provides a simple command line interface to the SnappyData cluster. It allows you to run interactive queries on row and column stores, run administrative operations and run status commands on the cluster. Internally it uses JDBC to interact with the cluster. You can also use tools like SquirrelSQL or DBVisualizer( JDBC to connect to the cluster) to interact with SnappyData.

<!--using javascript as the code language here... should this be sql?-->
```javascript

// from the SnappyData base directory  
$ cd quickstart/scripts  
$ ../../bin/snappy-sql
Version 2.0-BETA
snappy> 

//Connect to the cluster as a client  
snappy> connect client 'localhost:1527';

//Show active connections  
snappy> show connections;

//Display cluster members by querying a system table  
snappy> select id, kind, status, host, port from sys.members;
//or
snappy> show members;

//Run a sql script. This particular script creates and loads a column table in the default schema  
snappy> run 'create_and_load_column_table.sql';


//Run a sql script. This particular script creates and loads a row table in the default schema  
snappy> run 'create_and_load_row_table.sql';
```

The complete list of commands available through _snappy_shell_ can be found [here](http://gemfirexd.docs.pivotal.io/docs-gemfirexd/reference/gfxd_commands/gfxd-launcher.html)

## Using the Spark Shell and spark-submit

SnappyData, out-of-the-box, colocates Spark executors and the SnappyData store for efficient data intensive computations. 
But it may be desirable to isolate the computational cluster for other reasons, for instance, a  computationally 
intensive Map-reduce machine learning algorithm that needs to iterate over a cached data set repeatedly. 
To support such scenarios it is also possible to run native Spark jobs that access a SnappyData cluster as a storage layer 
in a parallel fashion. To connect to the SnappyData store spark.snappydata.connection property needs to be 
provided while starting the spark-shell. To run all SnappyData functionalities you need to create 
a [SnappySession](http://tibcosoftware.github.io/snappydata/apidocs/#org.apache.spark.sql.SnappySession).

```bash
// from the SnappyData base directory  
# Start the spark shell in local mode. Pass SnappyData's locators host:port as a conf parameter.
# Change the UI port because the default port 4040 is being used by Snappy’s lead. 
$ bin/spark-shell  --master local[*] --conf spark.snappydata.connection=locatorhost:port --conf spark.ui.port=4041
scala>
#Try few commands on the spark-shell. Following command shows the tables created using the snappy-sql
scala> val snappy = new org.apache.spark.sql.SnappySession(spark.sparkContext)
scala> val airlineDF = snappy.table("airline").show
scala> val resultset = snappy.sql("select * from airline")
```

Any spark application can also use the SnappyData as store and spark as computational engine by providing an extra spark.snappydata.connection property in the conf.

```bash
# Start the Spark standalone cluster from SnappyData base directory 
$ sbin/start-all.sh 
# Submit AirlineDataSparkApp to Spark Cluster with snappydata's locator host port.
$ bin/spark-submit --class io.snappydata.examples.AirlineDataSparkApp --master spark://masterhost:7077 --conf spark.snappydata.connection=locatorhost:port --conf spark.ui.port=4041 $SNAPPY_HOME/examples/jars/quickstart.jar

# The results can be seen on the command line.
```


## Using JDBC with SnappyData
SnappyData ships with a few JDBC drivers. 
The connection URL typically points to one of the Locators. Underneath the covers the driver acquires the endpoints for all the servers in the cluster along with load information and automatically connects clients to one of the data servers directly. The driver provides HA by automatically swizzling underlying physical connections in case servers were to fail. 

```java

// 1527 is the default port a Locator or Server uses to listen for thin client connections
Connection c = DriverManager.getConnection ("jdbc:snappydata://locatorHostName:1527/");
// While, clients typically just point to a locator, you could also directly point the 
//   connection at a server endpoint
```

## Accessing SnappyData Tables from Spark code
Spark applications access the SnappyStore using the new [Spark Data Source API](http://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases). 

By default, SnappyData servers runs the Spark Executors colocated with the data store. And, the default store provider is SnappyData. 
When the spark program connects to the cluster using a [SnappyContext](http://tibcosoftware.github.io/snappydata/apidocs/#org.apache.spark.sql.SnappyContext) (extends SQLContext), there is no need to configure the database URL and other options.  

```scala
// Here is an Scala example 
  val sc = new org.apache.spark.SparkContext(conf)
  val snappy = new org.apache.spark.sql.SnappySession(sc)

  val props = Map[String, String]()
  // Save some application dataframe into a SnappyData row table
  myAppDataFrame.write.format("row").options(props).saveAsTable("MutableTable")

```

When running a native spark program, you can access SnappyData purely as a DataSource ...
```scala
// Access SnappyData as a storage cluster .. 
  val spark: SparkSession = SparkSession
          .builder
          .appName("SparkApp")
          .master("local[4]")
          .getOrCreate

  val props = Map(
          "url" -> "jdbc:snappydata://locatorHostName:1527/",
          "poolImpl" -> "tomcat", 
          "user" -> "app",
          "password" -> "app"
    )

  // Save some application dataframe into a JDBC DataSource
  myAppDataFrame.write.format("jdbc").options(props).saveAsTable("MutableTable")
```
