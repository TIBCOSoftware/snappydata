## Getting Started with SnappyData SQL Shell

The SnappyData SQL Shell (_snappy-shell_) provides a simple command line interface to the SnappyData cluster. It allows you to run interactive queries on row and column stores, run administrative operations and run status commands on the cluster. Internally it uses JDBC to interact with the cluster. You can also use tools like SquirrelSQL or DBVisualizer( JDBC to connect to the cluster) to interact with SnappyData.

<!--using javascript as the code language here... should this be sql?-->
```javascript

// from the SnappyData base directory  
$ cd quickstart/scripts  
$ ../../bin/snappy-shell  
Version 2.0-SNAPSHOT.1  
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

//Start pulse to monitor the SnappyData cluster  
snappy> start pulse;
```

The complete list of commands available through _snappy_shell_ can be found [here](http://gemfirexd.docs.pivotal.io/docs-gemfirexd/reference/gfxd_commands/gfxd-launcher.html)


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

By default, SnappyData servers runs the Spark Executors collocated with the data store. And, the default store provider is SnappyData. 
When the spark program connects to the cluster using a SnappyContext (extends SQLContext), there is no need to configure the database URL and other options.  

```scala
// Here is an Scala example 
  val sc = new org.apache.spark.SparkContext(conf)
  val snContext = org.apache.spark.sql.SnappyContext(sc)

  val props = Map[String, String]()
  // Save some application dataframe into a SnappyData row table
  myAppDataFrame.write.format("row").options(props).saveAsTable("MutableTable")

```

When running a native spark program, you can access SnappyData purely as a DataSource ...
```scala
// Access SnappyData as a storage cluster .. 
  val sc = new org.apache.spark.SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val props = Map(
    "url" -> "jdbc:snappydata://locatorHostName:1527/",
    "poolImpl" -> "tomcat", 
    "user" -> "app",
    "password" -> "app"
    )

  // Save some application dataframe into a SnappyData row table
  myAppDataFrame.write.format("row").options(props).saveAsTable("MutableTable")
```







