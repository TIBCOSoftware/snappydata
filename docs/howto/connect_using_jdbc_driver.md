<a id="howto-jdbc"></a>
# How to Connect using JDBC Driver

You can connect to and execute queries against SnappyData cluster using JDBC driver. 
The connection URL typically points to one of the locators. The locator passes the information of all available servers, based on which the driver automatically connects to one of the servers.


To connect to the SnappyData cluster using JDBC, use URL of the form `jdbc:snappydata://<locatorHostName>:<locatorClientPort>/`

Where the `<locatorHostName>` is the hostname of the node on which the locator is started and `<locatorClientPort>` is the port on which the locator accepts client connections (default 1527).

You can use Maven or SBT dependencies to get the latest SnappyData JBDC driver which is used for establishing the JDBC connection with SnappyData. Other than this you can also directly download the JDBC driver from the SnappyData release page. 

## Using Maven/SBT Dependencies 

You can use the Maven or the SBT dependencies to get the latest released version of SnappyData JDBC driver.

**Example: Maven dependency**
```pre
<!-- https://mvnrepository.com/artifact/io.snappydata/snappydata-store-client -->
<dependency>
    <groupId>io.snappydata</groupId>
    <artifactId>snappydata-jdbc_2.11</artifactId>
    <version>1.2.0</version>
</dependency>
```

**Example: SBT dependency**
```pre
// https://mvnrepository.com/artifact/io.snappydata/snappydata-store-client
libraryDependencies += "io.snappydata" % "snappydata-jdbc_2.11" % "1.2.0"
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

## Dowloading SnappyData JDBC Driver Jar

You can directly [download the SnappyData JDBC driver](https://github.com/SnappyDataInc/snappydata/releases/latest) from the latest SnappyData release page. Scroll down to download the SnappyData JDBC driver jar which is listed in the **Description of download Artifacts** > **Assets** section.


## Code Example

**Connect to a SnappyData cluster using JDBC on default client port**

The code snippet shows how to connect to a SnappyData cluster using JDBC on default client port 1527. The complete source code of the example is located at [JDBCExample.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/JDBCExample.scala)

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
