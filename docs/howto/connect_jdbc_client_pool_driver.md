# How to Connect with JDBC Client Pool Driver

###Note: I think this section should be merged with the other 'How to connect using JDBC' section (jags)

JDBC client pool driver provides built-in connection pooling and relies on the non-pooled [JDBC driver](/howto/connect_using_jdbc_driver.md). The driver will initialize the pool when the first connection is created using this driver. Thereafter, for every request, the connection is returned from the pool instead of establishing a new connection with the server. We recommend using the pooled driver for low latency operations (e.g. a point lookup query) and when using the Spark JDBC data source API (see example below). When accessing SnappyData from Java frameworks such as Spring we recommend you use pooling provided in the framework and switch to using the non-pooled driver. 

#### (Jags) I don't understand this below section. It doesn't make sense with no context on what property you are talkign about. 
I don't know the exact semantics but perhaps this is the wording .....
The underlying pool is uniquely associated with the set of properties passed when creating the connection. If any of the properties change a new pool will be created. 
#### (Jags) it is super important for us to document the caveats - if you set a connection property like autocommit(false) other connection requests on this pool will also have this property set. Not sure if this issue exists. If so, critical to document and provide suggestions. Lizy, please verify. 


!!!Important
	Internally, the JDBC client pool driver maintains a map, where the key is the property and the value is the connection pool. Therefore in a second request, if you pass a different property object, then internally it adds another record in the map.

**To connect to SnappyData Cluster using JDBC client pool driver**, use the url of the form: </br> `jdbc:snappydata:pool://<host>:<port>`

The client pool driver class name is **io.snappydata.jdbc.ClientPoolDriver**.

Where the `<locatorHostName>` is the hostname of the node on which the locator is started and `<locatorClientPort>` is the port on which the locator accepts client connections (default 1527).


/*Dependency section needs approval/discussion */
**Dependencies**: Use the Maven/SBT dependencies for the latest released version of SnappyData. 

**Example: Maven dependency**
```pre
<!-- https://mvnrepository.com/artifact/io.snappydata/snappydata-store-client -->
<dependency>
    <groupId>io.snappydata</groupId>
    <artifactId>snappydata-jdbc_2.11</artifactId>
    <version>1.0.2.1</version>
</dependency>
```

**Example: SBT dependency**
```pre
// https://mvnrepository.com/artifact/io.snappydata/snappydata-store-client
libraryDependencies += "io.snappydata" % "snappydata-jdbc_2.11" % "1.1.0"
```

!!! Note

	If your project fails when resolving the above dependency (that is, it fails to download javax.ws.rs#javax.ws.rs-api;2.1), it may be due an issue with its pom file. </br>As a workaround, add the below code to the **build.sbt**:

```
val workaround = {
  sys.props += "packaging.type" -> "jar"
  ()
}
```

For more details, refer [https://github.com/sbt/sbt/issues/3618](https://github.com/sbt/sbt/issues/3618).


The following additional properties can be configured for JDBC client pool driver connection:

| Property | Description |
|--------|--------|
|    pool.user    |   The username to be passed to the JDBC client pool driver to establish a connection.   |
|pool.password|The password to be passed to the JDBC  client pool driver to establish a connection.|
|pool.initialSize|The initial number of connections that are created when the pool is started. Default value is `max(256, availableProcessors * 8)`.|
|pool.maxActive| The maximum number of active connections that can be allocated from this pool at a time. The default value is `max(256, availableProcessors * 8)`. |
|pool.minIdle| The minimum number of established connections that should be maintained in the client pool. Default value is derived from **initialSize:`max(256, availableProcessors * 8)`**.|
|pool.maxIdle| The maximum number of connections that should be maintained in the client pool. Default value is **maxActive:`max(256, availableProcessors * 8)`**. Idle connections are checked periodically, if enabled, and the connections that are idle for more than the time set in **minEvictableIdleTimeMillis** are released.|
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
