<a id="howto-jdbc"></a>
# How to Connect using JDBC Driver

You can connect to and execute queries against TIBCO ComputeDB cluster using JDBC driver. 
The connection URL typically points to one of the locators. The locator passes the information of all available servers, based on which the driver automatically connects to one of the servers.


To connect to the TIBCO ComputeDB cluster using JDBC, use URL of the form `jdbc:snappydata://<locatorHostName>:<locatorClientPort>/`

Where the `<locatorHostName>` is the hostname of the node on which the locator is started and `<locatorClientPort>` is the port on which the locator accepts client connections (default 1527).

You can use Maven or SBT dependencies to get the latest TIBCO ComputeDB JBDC driver which is used for establishing the JDBC connection with TIBCO ComputeDB. Other than this you can also directly download the JDBC driver from the TIBCO ComputeDB release page. 

## Using Maven/SBT Dependencies 

You can use the Maven or the SBT dependencies to get the latest released version of TIBCO ComputeDB JDBC driver.

**Example: Maven dependency**
```pre
<!-- https://mvnrepository.com/artifact/io.snappydata/snappydata-store-client -->
<dependency>
    <groupId>io.snappydata</groupId>
    <artifactId>snappydata-jdbc_2.11</artifactId>
    <version>1.1.0</version>
</dependency>
```

**Example: SBT dependency**
```pre
// https://mvnrepository.com/artifact/io.snappydata/snappydata-store-client
libraryDependencies += "io.snappydata" % "snappydata-jdbc_2.11" % "1.1.0"
```

!!! Note

	If your project fails when resolving the above dependency (that is, it fails to download javax.ws.rs#javax.ws.rs-api;2.1), it may be due to an issue with its pom file. </br>As a workaround, add the below code to the **build.sbt**:

```
val workaround = {
  sys.props += "packaging.type" -> "jar"
  ()
}
```

For more details, refer [https://github.com/sbt/sbt/issues/3618](https://github.com/sbt/sbt/issues/3618).

## Dowloading TIBCO ComputeDB JDBC Driver Jar

You can directly [download the TIBCO ComputeDB JDBC driver](https://github.com/SnappyDataInc/snappydata/releases/latest) from the latest release page. Scroll down to download the TIBCO ComputeDB JDBC driver jar which is listed in the **Description of download Artifacts** > **Assets** section.


## Code Example

**Connect to a TIBCO ComputeDB cluster using JDBC on default client port**

The code snippet shows how to connect to a TIBCO ComputeDB cluster using JDBC on default client port 1527. The complete source code of the example is located at [JDBCExample.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/JDBCExample.scala)

```pre
val url: String = s"jdbc:snappydata://localhost:1527/"
val conn1 = DriverManager.getConnection(url)

val stmt1 = conn1.createStatement()
// Creating a table (PARTSUPP) using JDBC connection
stmt1.execute("DROP TABLE IF EXISTS APP.PARTSUPP")
stmt1.execute("CREATE TABLE APP.PARTSUPP ( " +
     "PS_PARTKEY     INTEGER NOT NULL PRIMARY KEY," +
     "PS_SUPPKEY     INTEGER NOT NULL," +
     "PS_AVAILQTY    INTEGER NOT NULL," +
     "PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL)" +
    "USING ROW OPTIONS (PARTITION_BY 'PS_PARTKEY')")

// Inserting records in PARTSUPP table via batch inserts
val preparedStmt1 = conn1.prepareStatement("INSERT INTO APP.PARTSUPP VALUES(?, ?, ?, ?)")

var x = 0
for (x <- 1 to 10) {
  preparedStmt1.setInt(1, x*100)
  preparedStmt1.setInt(2, x)
  preparedStmt1.setInt(3, x*1000)
  preparedStmt1.setBigDecimal(4, java.math.BigDecimal.valueOf(100.2))
  preparedStmt1.addBatch()
}
preparedStmt1.executeBatch()
preparedStmt1.close()
```

!!! Note 
	If the tool does not automatically select a driver class, you may have the option of selecting a class from within the JAR file. In this case, select the **io.snappydata.jdbc.ClientDriver** class.


<a id="jdbcpooldriverconnect"></a>
## Connecting with JDBC Client Pool Driver

JDBC client pool driver provides built-in connection pooling and relies on the non-pooled [JDBC driver](/howto/connect_using_jdbc_driver.md). The driver initializes the pool when the first connection is created using this driver. Thereafter, for every request, the connection is returned from the pool instead of establishing a new connection with the server. 
We recommend using the pooled driver for low latency operations such as point lookups and when using the Spark JDBC data source API (see example below). When you access TIBCO ComputeDB from Java frameworks such as Spring, we recommend using pooling provided in the framework and switch to using the non-pooled driver. 

!!! Important
	The underlying pool is uniquely associated with the set of properties that are passed while creating the connection. If any of the properties change, a new pool is created.

**To connect to TIBCO ComputeDB Cluster using JDBC client pool driver**, use the url of the form: </br> `jdbc:snappydata:pool://<host>:<port>`</br>
Where `<host>` is the hostname of the node on which the locator is started and `<port>` is the port on which the locator accepts client connections (default 1527).

The client pool driver class name is **io.snappydata.jdbc.ClientPoolDriver**.

The following pool related properties can be used to tune the JDBC client pool driver:

| Property | Description |
|--------|--------|
|    pool.user    |   The username to be passed to the JDBC client pool driver to establish a connection.   |
|pool.password|The password to be passed to the JDBC  client pool driver to establish a connection.|
|pool.initialSize|The initial number of connections that are created when the pool is started. Default value is `max(256, availableProcessors * 8)`.|
|pool.maxActive| The maximum number of active connections that can be allocated from this pool at a time. The default value is `max(256, availableProcessors * 8)`. |
|pool.minIdle| The minimum number of established connections that should be maintained in the client pool. Default value is **1**.|
|pool.maxIdle| The maximum number of connections that should be maintained in the client pool. Default value is **maxActive:**`max(256, availableProcessors * 8)`. Idle connections are checked periodically, if enabled, and the connections that are idle for more than the time set in **minEvictableIdleTimeMillis** are released.|
|pool.maxWait|(int) The maximum waiting period, in milliseconds, for establishing a connection after which an exception is thrown. Default value is 30000 (30 seconds).|
|pool.removeAbandoned| Flag to remove the abandoned connections, in case they exceed the settings for **removeAbandonedTimeout**. If set to true a connection is considered abandoned and eligible for removal, if its no longer in use than the settings for **removeAbandonedTimeout**. Setting this to **true** can recover db connections from applications that fail to close a connection. The default value is **false**.|
|pool.removeAbandonedTimeout| Timeout in seconds before an abandoned connection, that was in use, can be removed. The default value is 60 seconds. The value should be set to the time required for the longest running query in your applications.|
|pool.timeBetweenEvictionRunsMillis| Time period required to sleep between runs of the idle connection validation/cleaner thread. You should always set this value above one second. This time period determines how often we check for idle and abandoned connections and how often to validate the idle connections. The default value is 5000 (5 seconds).|
|pool.minEvictableIdleTimeMillis|The minimum time period, in milliseconds, for which an object can be idle in the pool before it qualifies for eviction. The default value is 60000 (60 seconds).|
|driver|`io.snappydata.jdbc.ClientPoolDriver`</br>This should be passed through Spark JDBC API for loading and using the driver.|
|pool.testOnBorrow|Indicates if the objects are validated before being borrowed from the pool. If the object fails to validate, it will be dropped from the pool, and will attempt to borrow another. In order to have a more efficient validation, see `pool.validationInterval`. Default value is **true**.|
|pool.validationInterval|Avoid excess validation, only run validation at most at this frequency - time in milliseconds. If a connection is due for validation, but has been validated previously within this interval, it will not be validated again. The default value is 10000 (10 seconds).|

**Example Code Snippet:**

```pre
val properties = new Properties()
properties.setProperty("pool.user", "user")
properties.setProperty("pool.password", "pass")
properties.setProperty("driver", ““io.snappydata.jdbc.ClientPoolDriver””)

val builder = SparkSession
.builder.
appName("app")
.master("local[*]")

val spark: SparkSession = builder.getOrCreate

val df = spark.read.jdbc(“jdbc:snappydata:pool://localhost:1527”, "Table_X", properties)

```

## Limitations

If you set any of the following properties for a pooled connection, it gets auto-reset to the default values whenever you obtain a new pooled connection.

*	**setAutoCommit**
*	**setTransactionIsolation**
*	**setReadOnly**

However, if you have set any of the other properties (e.g. spark or snappy AQP related properties), it does not get auto-reset when you obtain a new pooled connection.

