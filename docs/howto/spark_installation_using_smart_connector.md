<a id="howto-splitmode"></a>
# How to Access SnappyData Store from an existing Spark Installation using Smart Connector

SnappyData comes with a Smart Connector that enables Spark applications to work with the SnappyData cluster, from any compatible Spark cluster. You can use any distribution that is compatible with Apache Spark 2.1.1. To connect from a Spark 2.4 based cluster, you may use JDBC connector as described [here](/programming_guide/spark_jdbc_connector.md#howto-sparkjdbc). The Spark cluster executes in its own independent JVM processes and connects to SnappyData as a Spark data source. This is similar to how Spark applications today work with stores like Cassandra, Redis, etc.

For more information on the various modes, refer to the [SnappyData Smart Connector](../affinity_modes/connector_mode.md) section of the documentation.

## Code Example
The code example for this mode is in [SmartConnectorExample.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/SmartConnectorExample.scala)

**Configure a SnappySession**:</br>
The code below shows how to initialize a SparkSession. Here the property `snappydata.connection` instructs the connector to acquire cluster connectivity, catalog metadata and register it locally in the Spark cluster. The values consists of *locator host* and *JDBC client port* on which the locator listens for connections (default 1527).

```pre
val spark: SparkSession = SparkSession
    .builder
    .appName("SmartConnectorExample")
    // It can be any master URL
    .master("local[4]")
    // snappydata.connection property enables the application to interact with SnappyData store
    .config("snappydata.connection", "localhost:1527")
    .getOrCreate

val snSession = new SnappySession(spark.sparkContext)
```

**Create Table and Run Queries**: 
You can now create tables and run queries in SnappyData store using your Apache Spark program.

```pre
// reading an already created SnappyStore table SNAPPY_COL_TABLE
val colTable = snSession.table("SNAPPY_COL_TABLE")
colTable.show(10)

snSession.dropTable("TestColumnTable", ifExists = true)

// Creating a table from a DataFrame
val dataFrame = snSession.range(1000).selectExpr("id", "floor(rand() * 10000) as k")

snSession.sql("create table TestColumnTable (id bigint not null, k bigint not null) using column")

// insert data in TestColumnTable
dataFrame.write.insertInto("TestColumnTable")
```

## Running a Smart Connector Application

Start a SnappyData cluster and create a table.

```pre
$ ./sbin/snappy-start-all.sh

$ ./bin/snappy
SnappyData version 1.1.1
snappy>  connect client 'localhost:1527';
Using CONNECTION0
snappy> CREATE TABLE SNAPPY_COL_TABLE(r1 Integer, r2 Integer) USING COLUMN;
snappy> insert into SNAPPY_COL_TABLE VALUES(1,1);
1 row inserted/updated/deleted
snappy> insert into SNAPPY_COL_TABLE VALUES(2,2);
1 row inserted/updated/deleted
exit;
```

The Smart Connector Application can now connect to this SnappyData cluster. </br>

The following command executes an example that queries SNAPPY_COL_TABLE and creates a new table inside the SnappyData cluster. </br>SnappyData package has to be specified along with the application jar to run the Smart Connector application.

```pre
$ ./bin/spark-submit --master local[*] --conf snappydata.connection=localhost:1527  --class org.apache.spark.examples.snappydata.SmartConnectorExample --packages SnappyDataInc:snappydata:1.1.1-s_2.11       $SNAPPY_HOME/examples/jars/quickstart.jar
```

## Execute a Smart Connector Application
Start a SnappyData cluster and create a table inside it.

```pre
$ ./sbin/snappy-start-all.sh

$ ./bin/snappy
SnappyData version 1.1.1
snappy>  connect client 'localhost:1527';
Using CONNECTION0
snappy> CREATE TABLE SNAPPY_COL_TABLE(r1 Integer, r2 Integer) USING COLUMN;
snappy> insert into SNAPPY_COL_TABLE VALUES(1,1);
1 row inserted/updated/deleted
snappy> insert into SNAPPY_COL_TABLE VALUES(2,2);
1 row inserted/updated/deleted
exit;
```

A Smart Connector Application can now connect to this SnappyData cluster. The following command executes an example that queries SNAPPY_COL_TABLE and creates a new table inside SnappyData cluster. SnappyData package has to be specified along with the application jar to run the Smart Connector application. 

```pre
$ ./bin/spark-submit --master local[*] --conf spark.snappydata.connection=localhost:1527  --class org.apache.spark.examples.snappydata.SmartConnectorExample   --packages SnappyDataInc:snappydata:1.1.1-s_2.11 $SNAPPY_HOME/examples/jars/quickstart.jar
```
