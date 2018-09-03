# How to Connect using JDBC Driver
<a id="howto-jdbc"></a>

You can connect to and execute queries against SnappyData cluster using JDBC driver. The connection URL typically points to one of the locators. The locator passes the information of all available servers based on which, the driver automatically connects to one of the servers.

**To connect to the SnappyData cluster**: Using JDBC, use URL of the form `jdbc:snappydata://<locatorHostName>:<locatorClientPort>/`

Where the `<locatorHostName>` is the hostname of the node on which the locator is started and `<locatorClientPort>` is the port on which the locator accepts client connections (default 1527).

**Dependencies**: Use the Maven/SBT dependencies for the latest released version of SnappyData.

**Example: Maven dependency:**
```pre
<!-- https://mvnrepository.com/artifact/io.snappydata/snappydata-store-client -->
<dependency>
    <groupId>io.snappydata</groupId>
    <artifactId>snappydata-store-client</artifactId>
    <version>1.6.2</version>
</dependency>
```

**Example: SBT dependency:**
```pre
// https://mvnrepository.com/artifact/io.snappydata/snappydata-store-client
libraryDependencies += "io.snappydata" % "snappydata-store-client" % "1.6.2"
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


**Code Example:**

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
